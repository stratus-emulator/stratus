// Package kinesis emulates Kinesis Data Streams: streams of shards, each an
// append-only log in memory, written with PutRecord(s) and read through shard
// iterators. A stream is ACTIVE the moment it is created.
package kinesis

import (
	"math/big"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
)

const (
	defaultRetentionHours = 24
	maxRetentionHours     = 8760
	maxShardsPerStream    = 10000
	// maxTotalShards bounds the memory a runaway client can reserve.
	maxTotalShards = 100000
	maxTags        = 50

	modeProvisioned = "PROVISIONED"
	modeOnDemand    = "ON_DEMAND"
	// onDemandShards is how many shards an ON_DEMAND stream starts with.
	onDemandShards = 4
)

var streamNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

// Service is the Kinesis emulator. It is safe for concurrent use: the stream
// table has one lock and every stream its own, so traffic on one stream never
// waits for another.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock

	mu      sync.RWMutex
	streams map[string]*stream
	nextGen uint64
}

// stream is one data stream. mu guards everything below it.
type stream struct {
	mu sync.RWMutex

	name    string
	arn     string
	gen     uint64 // tells a re-created stream from its predecessor
	created time.Time
	mode    string

	retentionHours int
	encryptionType string
	keyID          string
	tags           map[string]string
	shards         []*shard
	// firstSeq is the counter at creation, the time the stream was created in
	// nanoseconds: every stream of a name, in this emulator or one before a
	// restart, issues sequence numbers above those of the ones before it, so a
	// checkpoint of an earlier one is told apart and refused.
	firstSeq uint64
	lastSeq  uint64 // the stream-wide sequence counter, the last number issued
	deleted  bool   // set when the stream leaves the table; see Service.live
}

// New returns an empty service of the given account and region.
func New(identity awsapi.Identity, c clock.Clock) *Service {
	return &Service{identity: identity, clock: c, streams: map[string]*stream{}}
}

func (s *Service) Name() string         { return "kinesis" }
func (s *Service) TargetPrefix() string { return "Kinesis_20131202." }

// Reset drops all streams. Iterators and in-flight requests of the dropped
// streams then see ResourceNotFoundException.
func (s *Service) Reset() {
	s.mu.Lock()
	dropped := s.streams
	s.streams = map[string]*stream{}
	s.mu.Unlock()
	for _, st := range dropped {
		st.mu.Lock()
		st.deleted = true
		st.mu.Unlock()
	}
}

// CreateStream creates an ACTIVE stream of the given number of shards (1 to
// 10000) that splits the hash key space evenly. It fails with
// ResourceInUseException when the name is taken.
func (s *Service) CreateStream(name string, shards int) error {
	return s.createStream(name, shards, modeProvisioned)
}

func (s *Service) createStream(name string, shards int, mode string) error {
	if !streamNamePattern.MatchString(name) {
		return errConstraint("streamName", "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and have length between 1 and 128")
	}
	if shards < 1 || shards > maxShardsPerStream {
		return errConstraint("shardCount", "Member must have value between 1 and 10000")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.streams[name]; exists {
		return errInUse("Stream " + name + " under account " + s.identity.Account + " already exists.")
	}
	total := shards
	for _, st := range s.streams {
		total += len(st.shards)
	}
	if total > maxTotalShards {
		return errLimitExceeded("The emulator holds at most 100000 shards in total.")
	}
	s.nextGen++
	now := s.clock.Now()
	first := uint64(max(now.UnixNano(), 0))
	s.streams[name] = &stream{
		name:           name,
		arn:            s.identity.ARN("kinesis", "stream/"+name),
		gen:            s.nextGen,
		created:        now.Truncate(time.Millisecond),
		mode:           mode,
		retentionHours: defaultRetentionHours,
		shards:         newShards(shards),
		firstSeq:       first,
		lastSeq:        first,
	}
	return nil
}

// DeleteStream removes a stream and its records.
func (s *Service) DeleteStream(name string) error {
	s.mu.Lock()
	st, ok := s.streams[name]
	delete(s.streams, name)
	s.mu.Unlock()
	if !ok {
		return errStreamNotFound(name, s.identity.Account)
	}
	st.mu.Lock()
	st.deleted = true
	st.mu.Unlock()
	return nil
}

// TrimExpired drops every record older than its stream's retention period, by
// the injected clock, and returns how many it dropped. Nothing calls it by
// itself: retention is enforced only when the host asks.
func (s *Service) TrimExpired() int {
	now := s.clock.Now()
	trimmed := 0
	for _, st := range s.snapshot() {
		cutoff := now.Add(-time.Duration(st.retention()) * time.Hour)
		st.mu.Lock()
		for _, sh := range st.shards {
			trimmed += sh.trimBefore(cutoff)
		}
		st.mu.Unlock()
	}
	return trimmed
}

// snapshot lists the streams without holding the table lock afterwards.
func (s *Service) snapshot() []*stream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*stream, 0, len(s.streams))
	for _, st := range s.streams {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func (st *stream) retention() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.retentionHours
}

// lookup finds a stream by name or, when name is empty, by ARN. The result may
// be deleted a moment later; callers check st.deleted after taking its lock.
func (s *Service) lookup(name, arn string) (*stream, error) {
	if name == "" && arn == "" {
		return nil, errValidation("Either StreamName or StreamARN must be provided.")
	}
	if name == "" {
		parsed, err := s.nameFromARN(arn)
		if err != nil {
			return nil, err
		}
		name = parsed
	}
	s.mu.RLock()
	st, ok := s.streams[name]
	s.mu.RUnlock()
	if !ok {
		return nil, errStreamNotFound(name, s.identity.Account)
	}
	return st, nil
}

// nameFromARN extracts the stream name from a stream ARN of this emulator's
// account and region.
func (s *Service) nameFromARN(arn string) (string, error) {
	prefix := s.identity.ARN("kinesis", "stream/")
	if rest, ok := strings.CutPrefix(arn, prefix); ok && !strings.Contains(rest, "/") && rest != "" {
		return rest, nil
	}
	if parts := strings.SplitN(arn, ":", 6); len(parts) == 6 && parts[0] == "arn" && parts[2] == "kinesis" &&
		strings.HasPrefix(parts[5], "stream/") {
		return "", errNotFound("Stream " + arn + " under account " + s.identity.Account + " not found.")
	}
	return "", errInvalidArgument("Invalid StreamARN: " + arn)
}

// gone is the error of a stream that was deleted after it was looked up.
func (s *Service) gone(st *stream) error { return errStreamNotFound(st.name, s.identity.Account) }

// entry is a record on its way into a stream.
type entry struct {
	data []byte
	pk   string
	hash *big.Int // nil: the hash of the partition key
}

// placement is where an entry landed.
type placement struct {
	shard string
	seq   uint64
}

// put appends the entries to st, in order, under one write lock.
func (s *Service) put(st *stream, entries []entry) ([]placement, error) {
	now := s.clock.Now().Truncate(time.Millisecond)
	out := make([]placement, len(entries))
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	for i, e := range entries {
		h := e.hash
		if h == nil {
			h = partitionHash(e.pk)
		}
		sh := st.route(h)
		st.lastSeq++
		sh.append(record{seq: st.lastSeq, arrival: now, data: e.data, pk: e.pk})
		out[i] = placement{shard: sh.id, seq: st.lastSeq}
	}
	return out, nil
}

// route is the shard whose hash range holds h: the last one that starts at or
// below it.
func (st *stream) route(h *big.Int) *shard {
	i := sort.Search(len(st.shards), func(i int) bool { return st.shards[i].start.Cmp(h) > 0 })
	return st.shards[i-1]
}
