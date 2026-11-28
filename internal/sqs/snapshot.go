package sqs

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"
)

// snapshotFormat is the version of what Snapshot writes. Restore reads only
// this one: a state written by another version is refused, never half read.
const snapshotFormat = 1

type snapshot struct {
	Format int             `json:"format"`
	Queues []queueSnapshot `json:"queues"`
}

type queueSnapshot struct {
	Name         string             `json:"name"`
	CreatedAt    time.Time          `json:"createdAt"`
	ModifiedAt   time.Time          `json:"modifiedAt"`
	Config       configSnapshot     `json:"config"`
	Messages     []messageSnapshot  `json:"messages"`
	Tags         map[string]string  `json:"tags,omitempty"`
	FIFOSequence uint64             `json:"fifoSequence,omitempty"`
	Dedup        []dedupSnapshot    `json:"dedup,omitempty"`
	MoveTasks    []moveTaskSnapshot `json:"moveTasks,omitempty"`
}

type moveTaskSnapshot struct {
	TaskHandle     string    `json:"taskHandle"`
	Status         string    `json:"status"`
	SourceARN      string    `json:"sourceArn"`
	DestinationARN string    `json:"destinationArn,omitempty"`
	MaxPerSecond   *int      `json:"maxPerSecond,omitempty"`
	Moved          int64     `json:"moved"`
	StartedAt      time.Time `json:"startedAt"`
}

type configSnapshot struct {
	Visibility      time.Duration     `json:"visibility"`
	Delay           time.Duration     `json:"delay"`
	ReceiveWait     time.Duration     `json:"receiveWait"`
	Retention       time.Duration     `json:"retention"`
	MaxSize         int               `json:"maxSize"`
	RedriveTarget   string            `json:"redriveTarget,omitempty"`
	MaxReceiveCount int               `json:"maxReceiveCount,omitempty"`
	FIFO            bool              `json:"fifo,omitempty"`
	ContentDedup    bool              `json:"contentDedup,omitempty"`
	DedupScope      string            `json:"dedupScope"`
	Throughput      string            `json:"throughput"`
	Stored          map[string]string `json:"stored,omitempty"`
}

type messageSnapshot struct {
	ID               string                      `json:"id"`
	Body             string                      `json:"body"`
	MD5OfBody        string                      `json:"md5OfBody"`
	Attributes       map[string]MessageAttribute `json:"attributes,omitempty"`
	TraceHeader      string                      `json:"traceHeader,omitempty"`
	GroupID          string                      `json:"groupId,omitempty"`
	DedupID          string                      `json:"dedupId,omitempty"`
	Sequence         string                      `json:"sequence,omitempty"`
	DeadLetterSource string                      `json:"deadLetterSource,omitempty"`
	SentAt           time.Time                   `json:"sentAt"`
	VisibleAt        time.Time                   `json:"visibleAt"`
	FirstReceivedAt  time.Time                   `json:"firstReceivedAt"`
	ReceiveCount     int                         `json:"receiveCount"`
	Receipt          string                      `json:"receipt,omitempty"`
}

type dedupSnapshot struct {
	Key     string     `json:"key"`
	Output  SendOutput `json:"output"`
	Expires time.Time  `json:"expires"`
}

// Snapshot returns every queue with its messages, for Restore to bring back
// in another emulator.
// keep says which resources, by name, to include; nil includes every one.
func (s *Service) Snapshot(keep func(name string) bool) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{Format: snapshotFormat, Queues: make([]queueSnapshot, 0, len(s.queues))}
	for _, q := range s.queues {
		if keep != nil && !keep(q.name) {
			continue
		}
		snap.Queues = append(snap.Queues, snapshotQueue(q))
	}
	sort.Slice(snap.Queues, func(i, j int) bool { return snap.Queues[i].Name < snap.Queues[j].Name })
	return json.Marshal(snap)
}

func snapshotQueue(q *queue) queueSnapshot {
	c := q.cfg
	qs := queueSnapshot{
		Name: q.name, CreatedAt: q.createdAt, ModifiedAt: q.modifiedAt,
		Config: configSnapshot{
			Visibility: c.visibility, Delay: c.delay, ReceiveWait: c.receiveWait, Retention: c.retention,
			MaxSize: c.maxSize, FIFO: c.fifo, ContentDedup: c.contentDedup, DedupScope: c.dedupScope,
			Throughput: c.throughput, Stored: maps.Clone(c.stored),
		},
		Tags:         maps.Clone(q.tags),
		FIFOSequence: q.fifo.sequence,
	}
	if c.redrive != nil {
		qs.Config.RedriveTarget, qs.Config.MaxReceiveCount = c.redrive.target, c.redrive.maxReceiveCount
	}
	for _, m := range q.messages {
		if m.deleted {
			continue
		}
		qs.Messages = append(qs.Messages, messageSnapshot{
			ID: m.id, Body: m.body, MD5OfBody: m.md5OfBody, Attributes: m.attributes,
			TraceHeader: m.traceHeader, GroupID: m.groupID, DedupID: m.dedupID, Sequence: m.sequence,
			DeadLetterSource: m.deadLetterSource, SentAt: m.sentAt, VisibleAt: m.visibleAt,
			FirstReceivedAt: m.firstReceivedAt, ReceiveCount: m.receiveCount, Receipt: m.receipt,
		})
	}
	for _, r := range q.fifo.order {
		if e, ok := q.fifo.seen[r.key]; ok {
			qs.Dedup = append(qs.Dedup, dedupSnapshot{Key: r.key, Output: e.out, Expires: e.expires})
		}
	}
	for _, t := range q.moveTasks {
		qs.MoveTasks = append(qs.MoveTasks, moveTaskSnapshot{
			TaskHandle:     t.taskHandle,
			Status:         t.status,
			SourceARN:      t.sourceARN,
			DestinationARN: t.destinationARN,
			MaxPerSecond:   cloneIntPtr(t.maxPerSecond),
			Moved:          t.moved,
			StartedAt:      t.startedAt,
		})
	}
	return qs
}

// Restore replaces every queue with those of a snapshot.
// keep says which resources of the snapshot, by name, to bring back; nil brings every one.
func (s *Service) Restore(data []byte, keep func(name string) bool) error {
	kept := func(name string) bool { return keep == nil || keep(name) }
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("reading the SQS state: %w", err)
	}
	if snap.Format != snapshotFormat {
		return fmt.Errorf("the SQS state is in format %d; this stratus reads format %d", snap.Format, snapshotFormat)
	}
	queues := make(map[string]*queue, len(snap.Queues))
	for _, qs := range snap.Queues {
		if !kept(qs.Name) {
			continue
		}
		queues[qs.Name] = restoreQueue(qs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		close(q.changed)
	}
	s.queues = queues
	return nil
}

func restoreQueue(qs queueSnapshot) *queue {
	c := qs.Config
	cfg := config{
		visibility: c.Visibility, delay: c.Delay, receiveWait: c.ReceiveWait, retention: c.Retention,
		maxSize: c.MaxSize, fifo: c.FIFO, contentDedup: c.ContentDedup, dedupScope: c.DedupScope,
		throughput: c.Throughput, stored: maps.Clone(c.Stored),
	}
	if cfg.stored == nil {
		cfg.stored = map[string]string{}
	}
	if c.RedriveTarget != "" {
		cfg.redrive = &redrive{target: c.RedriveTarget, maxReceiveCount: c.MaxReceiveCount}
	}
	q := &queue{
		name: qs.Name, createdAt: qs.CreatedAt, modifiedAt: qs.ModifiedAt, cfg: cfg,
		receipts: map[string]*message{}, tags: maps.Clone(qs.Tags), changed: make(chan struct{}),
		fifo: fifoState{sequence: qs.FIFOSequence, seen: map[string]dedupEntry{}},
	}
	if q.tags == nil {
		q.tags = map[string]string{}
	}
	for _, ms := range qs.Messages {
		m := &message{
			id: ms.ID, body: ms.Body, md5OfBody: ms.MD5OfBody, attributes: ms.Attributes,
			traceHeader: ms.TraceHeader, groupID: ms.GroupID, dedupID: ms.DedupID, sequence: ms.Sequence,
			deadLetterSource: ms.DeadLetterSource, sentAt: ms.SentAt, visibleAt: ms.VisibleAt,
			firstReceivedAt: ms.FirstReceivedAt, receiveCount: ms.ReceiveCount, receipt: ms.Receipt,
		}
		q.messages = append(q.messages, m)
		if m.receipt != "" {
			q.receipts[m.receipt] = m
		}
		if q.oldestSentAt.IsZero() || m.sentAt.Before(q.oldestSentAt) {
			q.oldestSentAt = m.sentAt
		}
	}
	for _, d := range qs.Dedup {
		q.fifo.seen[d.Key] = dedupEntry{out: d.Output, expires: d.Expires}
		q.fifo.order = append(q.fifo.order, dedupRecord{key: d.Key, expires: d.Expires})
	}
	for _, mt := range qs.MoveTasks {
		q.moveTasks = append(q.moveTasks, moveTask{
			taskHandle:     mt.TaskHandle,
			status:         mt.Status,
			sourceARN:      mt.SourceARN,
			destinationARN: mt.DestinationARN,
			maxPerSecond:   cloneIntPtr(mt.MaxPerSecond),
			moved:          mt.Moved,
			startedAt:      mt.StartedAt,
		})
	}
	return q
}

func cloneIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
