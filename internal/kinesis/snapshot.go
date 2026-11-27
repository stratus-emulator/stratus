package kinesis

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"sort"
	"time"
)

// snapshotFormat is the version of what Snapshot writes. Restore reads only
// this one: a state written by another version is refused, never half read.
const snapshotFormat = 1

type snapshot struct {
	Format  int              `json:"format"`
	Streams []streamSnapshot `json:"streams"`
}

type streamSnapshot struct {
	Name           string            `json:"name"`
	Created        time.Time         `json:"created"`
	Mode           string            `json:"mode"`
	RetentionHours int               `json:"retentionHours"`
	EncryptionType string            `json:"encryptionType,omitempty"`
	KeyID          string            `json:"keyId,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	// FirstSeq and LastSeq keep the stream's sequence numbers going where they
	// stopped, so a consumer's checkpoint stays valid across a restart.
	FirstSeq uint64          `json:"firstSeq"`
	LastSeq  uint64          `json:"lastSeq"`
	Shards   []shardSnapshot `json:"shards"`
}

type shardSnapshot struct {
	ID      string           `json:"id"`
	Start   string           `json:"start"` // decimal: a 128-bit hash key
	End     string           `json:"end"`
	Records []recordSnapshot `json:"records"`
}

type recordSnapshot struct {
	Seq     uint64    `json:"seq"`
	Arrival time.Time `json:"arrival"`
	Data    []byte    `json:"data"`
	PK      string    `json:"pk"`
}

// Snapshot returns every stream with its records, for Restore to bring back
// in another emulator.
// keep says which resources, by name, to include; nil includes every one.
func (s *Service) Snapshot(keep func(name string) bool) ([]byte, error) {
	s.mu.RLock()
	streams := make([]*stream, 0, len(s.streams))
	for name, st := range s.streams {
		if keep != nil && !keep(name) {
			continue
		}
		streams = append(streams, st)
	}
	s.mu.RUnlock()
	snap := snapshot{Format: snapshotFormat, Streams: make([]streamSnapshot, 0, len(streams))}
	for _, st := range streams {
		snap.Streams = append(snap.Streams, snapshotStream(st))
	}
	sort.Slice(snap.Streams, func(i, j int) bool { return snap.Streams[i].Name < snap.Streams[j].Name })
	return json.Marshal(snap)
}

func snapshotStream(st *stream) streamSnapshot {
	st.mu.RLock()
	defer st.mu.RUnlock()
	ss := streamSnapshot{
		Name: st.name, Created: st.created, Mode: st.mode, RetentionHours: st.retentionHours,
		EncryptionType: st.encryptionType, KeyID: st.keyID,
		Tags: maps.Clone(st.tags), FirstSeq: st.firstSeq, LastSeq: st.lastSeq,
	}
	for _, sh := range st.shards {
		shs := shardSnapshot{ID: sh.id, Start: sh.start.String(), End: sh.end.String(),
			Records: make([]recordSnapshot, len(sh.records))}
		for i, r := range sh.records {
			shs.Records[i] = recordSnapshot{Seq: r.seq, Arrival: r.arrival, Data: r.data, PK: r.pk}
		}
		ss.Shards = append(ss.Shards, shs)
	}
	return ss
}

// Restore replaces every stream with those of a snapshot.
// keep says which resources of the snapshot, by name, to bring back; nil brings every one.
func (s *Service) Restore(data []byte, keep func(name string) bool) error {
	kept := func(name string) bool { return keep == nil || keep(name) }
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("reading the Kinesis state: %w", err)
	}
	if snap.Format != snapshotFormat {
		return fmt.Errorf("the Kinesis state is in format %d; this stratus reads format %d", snap.Format, snapshotFormat)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	streams := make(map[string]*stream, len(snap.Streams))
	for _, ss := range snap.Streams {
		if !kept(ss.Name) {
			continue
		}
		st, err := s.restoreStream(ss)
		if err != nil {
			return fmt.Errorf("kinesis stream %q: %w", ss.Name, err)
		}
		streams[ss.Name] = st
	}
	for _, st := range s.streams {
		st.mu.Lock()
		st.deleted = true
		st.mu.Unlock()
	}
	s.streams = streams
	return nil
}

// restoreStream builds a stream from its snapshot; the caller holds s.mu.
func (s *Service) restoreStream(ss streamSnapshot) (*stream, error) {
	s.nextGen++
	st := &stream{
		name: ss.Name, arn: s.identity.ARN("kinesis", "stream/"+ss.Name), gen: s.nextGen,
		created: ss.Created, mode: ss.Mode, retentionHours: ss.RetentionHours,
		encryptionType: ss.EncryptionType, keyID: ss.KeyID,
		tags: maps.Clone(ss.Tags), firstSeq: ss.FirstSeq, lastSeq: ss.LastSeq,
	}
	for _, shs := range ss.Shards {
		start, okStart := new(big.Int).SetString(shs.Start, 10)
		end, okEnd := new(big.Int).SetString(shs.End, 10)
		if !okStart || !okEnd {
			return nil, fmt.Errorf("shard %s has a malformed hash key range", shs.ID)
		}
		sh := &shard{id: shs.ID, start: start, end: end, records: make([]record, len(shs.Records))}
		for i, r := range shs.Records {
			sh.records[i] = record{seq: r.Seq, arrival: r.Arrival, data: r.Data, pk: r.PK}
		}
		st.shards = append(st.shards, sh)
	}
	return st, nil
}
