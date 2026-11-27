package kinesis

import (
	"encoding/base64"
	"sort"
	"unicode/utf8"
)

const (
	defaultDescribeLimit  = 100
	maxListLimit          = 10000
	maxTagsPerPage        = 50
	maxEncryptionKeyIDLen = 2048
)

type modeDetails struct {
	StreamMode string
}

type monitoring struct {
	ShardLevelMetrics []string
}

type createStreamInput struct {
	StreamName        string
	ShardCount        *int
	StreamModeDetails *modeDetails
}

func (s *Service) createStreamOp(in createStreamInput) (any, error) {
	mode := modeProvisioned
	if in.StreamModeDetails != nil && in.StreamModeDetails.StreamMode != "" {
		mode = in.StreamModeDetails.StreamMode
	}
	shards := onDemandShards
	switch {
	case mode != modeProvisioned && mode != modeOnDemand:
		return nil, errConstraint("streamModeDetails.streamMode", "Member must satisfy enum value set: [PROVISIONED, ON_DEMAND]")
	case in.ShardCount != nil:
		shards = *in.ShardCount
	case mode == modeProvisioned:
		return nil, errValidation("ShardCount must be specified for a PROVISIONED stream.")
	}
	return empty{}, s.createStream(in.StreamName, shards, mode)
}

type deleteStreamInput struct {
	streamRef
	EnforceConsumerDeletion bool
}

func (s *Service) deleteStreamOp(in deleteStreamInput) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	return empty{}, s.DeleteStream(st.name)
}

type describeStreamInput struct {
	streamRef
	Limit                 *int
	ExclusiveStartShardId string
}

func (s *Service) describeStream(in describeStreamInput) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	limit := defaultDescribeLimit
	if in.Limit != nil {
		limit = *in.Limit
	}
	if limit < 1 || limit > maxListLimit {
		return nil, errConstraint("limit", "Member must have value between 1 and 10000")
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	page, more, err := pageShards(st, in.ExclusiveStartShardId, limit)
	if err != nil {
		return nil, err
	}
	description := map[string]any{
		"StreamName":              st.name,
		"StreamARN":               st.arn,
		"StreamStatus":            "ACTIVE",
		"StreamModeDetails":       modeDetails{StreamMode: st.mode},
		"Shards":                  page,
		"HasMoreShards":           more,
		"RetentionPeriodHours":    st.retentionHours,
		"StreamCreationTimestamp": epoch(st.created),
		"EnhancedMonitoring":      []monitoring{{ShardLevelMetrics: []string{}}},
	}
	applyEncryptionFields(description, st)
	return map[string]any{"StreamDescription": description}, nil
}

func (s *Service) describeStreamSummary(in streamRef) (any, error) {
	st, err := s.resolve(in)
	if err != nil {
		return nil, err
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	summary := map[string]any{
		"StreamName":              st.name,
		"StreamARN":               st.arn,
		"StreamStatus":            "ACTIVE",
		"StreamModeDetails":       modeDetails{StreamMode: st.mode},
		"RetentionPeriodHours":    st.retentionHours,
		"StreamCreationTimestamp": epoch(st.created),
		"EnhancedMonitoring":      []monitoring{{ShardLevelMetrics: []string{}}},
		"OpenShardCount":          len(st.shards),
		"ConsumerCount":           0,
	}
	applyEncryptionFields(summary, st)
	return map[string]any{"StreamDescriptionSummary": summary}, nil
}

func applyEncryptionFields(dst map[string]any, st *stream) {
	encType := st.encryptionType
	if encType == "" {
		encType = "NONE"
	}
	dst["EncryptionType"] = encType
	if encType == "KMS" && st.keyID != "" {
		dst["KeyId"] = st.keyID
	}
}

type streamEncryptionInput struct {
	streamRef
	EncryptionType string
	KeyId          string
}

func validateEncryptionInput(in streamEncryptionInput) error {
	switch in.EncryptionType {
	case "":
		return errConstraint("encryptionType", "Member must not be null")
	case "NONE":
		return errInvalidArgument("EncryptionType must be KMS.")
	case "KMS":
	default:
		return errConstraint("encryptionType", "Member must satisfy enum value set: [NONE, KMS]")
	}
	if in.KeyId == "" {
		return errConstraint("keyId", "Member must not be null")
	}
	if utf8.RuneCountInString(in.KeyId) > maxEncryptionKeyIDLen {
		return errConstraint("keyId", "Member must have length less than or equal to 2048")
	}
	return nil
}

func (s *Service) updateStreamEncryption(in streamEncryptionInput, encryptionType, keyID string) (any, error) {
	if err := validateEncryptionInput(in); err != nil {
		return nil, err
	}
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	st.encryptionType = encryptionType
	st.keyID = keyID
	return empty{}, nil
}

func (s *Service) startStreamEncryption(in streamEncryptionInput) (any, error) {
	return s.updateStreamEncryption(in, "KMS", in.KeyId)
}

func (s *Service) stopStreamEncryption(in streamEncryptionInput) (any, error) {
	return s.updateStreamEncryption(in, "", "")
}

type listStreamsInput struct {
	Limit                    *int
	ExclusiveStartStreamName string
	NextToken                string
}

type streamSummary struct {
	StreamName              string
	StreamARN               string
	StreamStatus            string
	StreamModeDetails       modeDetails
	StreamCreationTimestamp float64
}

// listStreams pages the streams by name. The next token is the last name
// returned, encoded, so a stream deleted between pages does not break paging.
func (s *Service) listStreams(in listStreamsInput) (any, error) {
	limit := defaultDescribeLimit
	if in.Limit != nil {
		limit = *in.Limit
	}
	if limit < 1 || limit > maxListLimit {
		return nil, errConstraint("limit", "Member must have value between 1 and 10000")
	}
	after := in.ExclusiveStartStreamName
	if in.NextToken != "" {
		raw, err := base64.RawURLEncoding.DecodeString(in.NextToken)
		if err != nil {
			return nil, errInvalidArgument("Invalid NextToken.")
		}
		after = string(raw)
	}
	all := s.snapshot()
	start := sort.Search(len(all), func(i int) bool { return all[i].name > after })
	all = all[start:]
	more := len(all) > limit
	if more {
		all = all[:limit]
	}
	names := make([]string, 0, len(all))
	summaries := make([]streamSummary, 0, len(all))
	for _, st := range all {
		st.mu.RLock()
		summaries = append(summaries, streamSummary{
			StreamName: st.name, StreamARN: st.arn, StreamStatus: "ACTIVE",
			StreamModeDetails: modeDetails{StreamMode: st.mode}, StreamCreationTimestamp: epoch(st.created),
		})
		st.mu.RUnlock()
		names = append(names, st.name)
	}
	out := map[string]any{"StreamNames": names, "StreamSummaries": summaries, "HasMoreStreams": more}
	if more {
		out["NextToken"] = base64.RawURLEncoding.EncodeToString([]byte(names[len(names)-1]))
	}
	return out, nil
}

type retentionInput struct {
	streamRef
	RetentionPeriodHours int
}

func (s *Service) increaseRetention(in retentionInput) (any, error) {
	return s.setRetention(in, true)
}

func (s *Service) decreaseRetention(in retentionInput) (any, error) {
	return s.setRetention(in, false)
}

// setRetention stores a new retention period: longer for an increase (up to
// 8760 hours), shorter for a decrease (down to 24). The emulator does not trim
// by itself; TrimExpired applies the period.
func (s *Service) setRetention(in retentionInput, increase bool) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	hours := in.RetentionPeriodHours
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	switch {
	case increase && (hours > maxRetentionHours || hours <= st.retentionHours):
		return nil, errInvalidArgument("Requested retention period must be greater than the current one and at most 8760 hours.")
	case !increase && (hours < defaultRetentionHours || hours >= st.retentionHours):
		return nil, errInvalidArgument("Requested retention period must be less than the current one and at least 24 hours.")
	}
	st.retentionHours = hours
	return empty{}, nil
}

type addTagsInput struct {
	streamRef
	Tags map[string]string
}

func (s *Service) addTags(in addTagsInput) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	merged := len(st.tags)
	for k := range in.Tags {
		if _, ok := st.tags[k]; !ok {
			merged++
		}
	}
	if merged > maxTags {
		return nil, errLimitExceeded("A stream can have at most 50 tags.")
	}
	if st.tags == nil {
		st.tags = map[string]string{}
	}
	for k, v := range in.Tags {
		st.tags[k] = v
	}
	return empty{}, nil
}

type removeTagsInput struct {
	streamRef
	TagKeys []string
}

func (s *Service) removeTags(in removeTagsInput) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	for _, k := range in.TagKeys {
		delete(st.tags, k)
	}
	return empty{}, nil
}

type listTagsInput struct {
	streamRef
	ExclusiveStartTagKey string
	Limit                *int
}

type tag struct {
	Key   string
	Value string
}

func (s *Service) listTags(in listTagsInput) (any, error) {
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	limit := maxTagsPerPage
	if in.Limit != nil {
		limit = *in.Limit
	}
	if limit < 1 || limit > maxTagsPerPage {
		return nil, errConstraint("limit", "Member must have value between 1 and 50")
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	keys := make([]string, 0, len(st.tags))
	for k := range st.tags {
		if k > in.ExclusiveStartTagKey {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	more := len(keys) > limit
	if more {
		keys = keys[:limit]
	}
	tags := make([]tag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, tag{Key: k, Value: st.tags[k]})
	}
	return map[string]any{"Tags": tags, "HasMoreTags": more}, nil
}
