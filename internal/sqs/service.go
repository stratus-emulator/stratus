// Package sqs emulates Amazon SQS standard and FIFO queues over the JSON protocol.
package sqs

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
)

var (
	validName     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	validFifoName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,75}\.fifo$`)
)

const fifoSuffix = ".fifo"

// Service holds every queue of one emulator.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock

	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	name       string
	createdAt  time.Time
	modifiedAt time.Time
	cfg        config
	messages   []*message // in delivery order; for a FIFO queue, in sequence order
	// receipts finds a message by its current receipt handle, so that deleting
	// one or changing its visibility does not walk the queue.
	receipts map[string]*message
	// deleted counts the messages that are marked deleted but still in messages:
	// they are removed from the front at once and from the rest in one pass
	// once they are half of the queue.
	deleted int
	// oldestSentAt is no later than the sentAt of every message, so that a
	// queue with nothing past its retention is not walked.
	oldestSentAt time.Time
	tags         map[string]string
	fifo         fifoState
	moveTasks    []moveTask // most recent message move tasks, oldest first (up to maxMoveTaskHistory)
	// changed is closed, and replaced, whenever a waiting receiver should look
	// at the queue again.
	changed chan struct{}
}

type message struct {
	id               string
	body             string
	md5OfBody        string
	attributes       map[string]MessageAttribute // never changed after the send
	traceHeader      string                      // the AWSTraceHeader system attribute
	groupID          string                      // FIFO queues only
	dedupID          string                      // FIFO queues only
	sequence         string                      // FIFO queues only
	deadLetterSource string                      // ARN of the queue the message was moved from
	sentAt           time.Time
	visibleAt        time.Time
	firstReceivedAt  time.Time
	receiveCount     int
	receipt          string
	deleted          bool // gone from the queue; skipped until it is removed from messages
}

// MessageAttribute is a message attribute, in its wire form.
type MessageAttribute struct {
	DataType    string  `json:"DataType"`
	StringValue *string `json:"StringValue,omitempty"`
	BinaryValue []byte  `json:"BinaryValue,omitempty"`
}

// SendInput is one message to send.
type SendInput struct {
	Body       string
	Delay      *time.Duration // nil: the queue's delay
	Attributes map[string]MessageAttribute
	// SystemAttributes are the message system attributes; AWSTraceHeader is the
	// only one SQS accepts.
	SystemAttributes map[string]MessageAttribute
	// GroupID is required on a FIFO queue and ignored on a standard one.
	GroupID string
	// DeduplicationID is required on a FIFO queue without content-based
	// deduplication and rejected on a standard queue.
	DeduplicationID string
}

// SendOutput describes a sent message.
type SendOutput struct {
	MessageID             string
	MD5OfBody             string
	MD5OfAttributes       string
	MD5OfSystemAttributes string
	SequenceNumber        string // FIFO queues only
}

// ReceiveInput is one receive call.
type ReceiveInput struct {
	Max        int            // 0: one message
	Visibility *time.Duration // nil: the queue's visibility timeout
	Wait       *time.Duration // nil: the queue's receive wait time
}

// Received is a message handed to a consumer.
type Received struct {
	MessageID       string
	Receipt         string
	Body            string
	MD5OfBody       string
	Attributes      map[string]MessageAttribute
	SentAt          time.Time
	FirstReceivedAt time.Time
	ReceiveCount    int

	TraceHeader      string // AWSTraceHeader, when the sender set one
	GroupID          string // FIFO queues only
	DeduplicationID  string // FIFO queues only
	SequenceNumber   string // FIFO queues only
	DeadLetterSource string // ARN of the queue a dead-lettered message came from
}

func New(identity awsapi.Identity, c clock.Clock) *Service {
	return &Service{identity: identity, clock: c, queues: map[string]*queue{}}
}

func (s *Service) Name() string         { return "sqs" }
func (s *Service) TargetPrefix() string { return targetPrefix }

// Reset drops every queue; receivers waiting on them fail with QueueDoesNotExist.
func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		close(q.changed)
	}
	s.queues = map[string]*queue{}
}

func (s *Service) arn(name string) string { return s.identity.ARN("sqs", name) }

// CreateQueue creates a queue, or does nothing when one of that name already
// has exactly these attributes.
func (s *Service) CreateQueue(name string, attrs map[string]string) error {
	return s.CreateQueueWithTags(name, attrs, nil)
}

// CreateQueueWithTags is CreateQueue that also tags a new queue. The tags of a
// queue that already exists are left as they are.
func (s *Service) CreateQueueWithTags(name string, attrs, tags map[string]string) error {
	fifo, err := queueKind(name, attrs)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if q, ok := s.queues[name]; ok {
		cfg, err := s.withAttributes(q.cfg, attrs)
		if err != nil {
			return err
		}
		if !cfg.equal(q.cfg) {
			return errQueueNameExists()
		}
		return nil
	}
	base := defaultConfig()
	base.fifo = fifo
	cfg, err := s.withAttributes(base, attrs)
	if err != nil {
		return err
	}
	q := &queue{name: name, cfg: cfg, receipts: map[string]*message{}, tags: map[string]string{}, changed: make(chan struct{})}
	if err := q.addTags(tags); err != nil {
		return err
	}
	q.createdAt = s.clock.Now()
	q.modifiedAt = q.createdAt
	s.queues[name] = q
	return nil
}

// queueKind checks the name and the FifoQueue attribute against each other and
// says whether the queue is a FIFO one: the suffix and the attribute need each other.
func queueKind(name string, attrs map[string]string) (bool, error) {
	fifo := attrs["FifoQueue"] == "true"
	if v, ok := attrs["FifoQueue"]; ok && v != "true" && v != "false" {
		return false, errInvalidAttributeValue("FifoQueue", "must be true or false.")
	}
	if fifo != strings.HasSuffix(name, fifoSuffix) {
		return false, errInvalidParameter("The name of a FIFO queue can only include alphanumeric " +
			"characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length, " +
			"and a name ending in .fifo needs the FifoQueue attribute set to true.")
	}
	valid := validName
	if fifo {
		valid = validFifoName
	}
	if !valid.MatchString(name) {
		return false, errInvalidParameter("Queue name can only include alphanumeric characters, " +
			"hyphens, or underscores, 1 to 80 in length (FIFO queue names also end with .fifo).")
	}
	return fifo, nil
}

func (s *Service) DeleteQueue(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	close(q.changed)
	delete(s.queues, name)
	return nil
}

// Exists reports whether the queue exists.
func (s *Service) Exists(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.queues[name]
	return ok
}

func (s *Service) SetAttributes(name string, attrs map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	cfg, err := s.withAttributes(q.cfg, attrs)
	if err != nil {
		return err
	}
	q.cfg = cfg
	q.modifiedAt = s.clock.Now()
	return nil
}

// Attributes returns every attribute of the queue.
func (s *Service) Attributes(name string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, err
	}
	return s.attributesLocked(q, s.clock.Now()), nil
}

// queueLocked finds a queue and, since every operation goes through it, drops
// the messages that outlived the queue's retention period.
func (s *Service) queueLocked(name string) (*queue, error) {
	q, ok := s.queues[name]
	if !ok {
		return nil, errQueueNotFound()
	}
	dropExpired(q, s.clock.Now())
	return q, nil
}

func wake(q *queue) {
	close(q.changed)
	q.changed = make(chan struct{})
}
