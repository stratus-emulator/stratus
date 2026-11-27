package kinesis

import (
	"slices"
	"strings"
	"time"
)

// StreamSummary is what an overview of the emulator shows of a stream.
type StreamSummary struct {
	Name           string
	Shards         int
	Records        int // records still held, across all shards
	RetentionHours int
}

// Streams summarises every stream, by name.
func (s *Service) Streams() []StreamSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StreamSummary, 0, len(s.streams))
	for _, st := range s.streams {
		st.mu.RLock()
		sum := StreamSummary{Name: st.name, Shards: len(st.shards), RetentionHours: st.retentionHours}
		for _, sh := range st.shards {
			sum.Records += len(sh.records)
		}
		st.mu.RUnlock()
		out = append(out, sum)
	}
	slices.SortFunc(out, func(a, b StreamSummary) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// StreamInfo is a stream's configuration and shards.
type StreamInfo struct {
	ARN            string
	Mode           string
	Created        time.Time
	RetentionHours int
	EncryptionType string
	KeyID          string
	Shards         []ShardInfo
}

// ShardInfo is one shard: its hash key range and the records it holds.
type ShardInfo struct {
	ID                             string
	StartingHashKey, EndingHashKey string
	Records                        int
}

// Stream describes one stream.
func (s *Service) Stream(name string) (StreamInfo, error) {
	s.mu.RLock()
	st, ok := s.streams[name]
	s.mu.RUnlock()
	if !ok {
		return StreamInfo{}, errStreamNotFound(name, s.identity.Account)
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	info := StreamInfo{
		ARN:            st.arn,
		Mode:           st.mode,
		Created:        st.created,
		RetentionHours: st.retentionHours,
		EncryptionType: st.encryptionType,
		KeyID:          st.keyID,
	}
	for _, sh := range st.shards {
		info.Shards = append(info.Shards, ShardInfo{
			ID: sh.id, StartingHashKey: sh.start.String(), EndingHashKey: sh.end.String(), Records: len(sh.records),
		})
	}
	return info, nil
}

// RecordView is a record as a look at the stream shows it.
type RecordView struct {
	Shard        string
	Sequence     uint64
	PartitionKey string
	Arrival      time.Time
	Data         []byte
}

// RecentRecords returns the stream's newest records, up to limit, newest
// first, across all shards. It reads without moving any iterator.
func (s *Service) RecentRecords(name string, limit int) ([]RecordView, error) {
	s.mu.RLock()
	st, ok := s.streams[name]
	s.mu.RUnlock()
	if !ok {
		return nil, errStreamNotFound(name, s.identity.Account)
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	var out []RecordView
	for _, sh := range st.shards {
		from := max(0, len(sh.records)-limit)
		for _, r := range sh.records[from:] {
			out = append(out, RecordView{Shard: sh.id, Sequence: r.seq, PartitionKey: r.pk, Arrival: r.arrival, Data: r.data})
		}
	}
	slices.SortFunc(out, func(a, b RecordView) int {
		switch {
		case a.Sequence > b.Sequence:
			return -1
		case a.Sequence < b.Sequence:
			return 1
		}
		return 0
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
