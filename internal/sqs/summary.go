package sqs

import (
	"strings"
	"time"
)

// PeekedMessage is a message as a look at the queue shows it.
type PeekedMessage struct {
	ID           string
	Body         string
	SentAt       time.Time
	ReceiveCount int
	State        string // "visible", "in flight" or "delayed"
	GroupID      string // FIFO queues only
	Attributes   map[string]MessageAttribute
}

// Peek returns up to max messages of the queue, in delivery order, whose body
// or a string attribute contains contains (ignoring case; empty matches every
// message), and how many match in all. It does not receive them: their
// visibility and receive counts stay as they are.
func (s *Service) Peek(name, contains string, max int) ([]PeekedMessage, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, 0, err
	}
	now := s.clock.Now()
	needle := strings.ToLower(contains)
	var out []PeekedMessage
	matched := 0
	for _, m := range q.messages {
		if m.deleted || !messageContains(m, needle) {
			continue
		}
		matched++
		if len(out) == max {
			continue
		}
		state := "visible"
		switch {
		case !m.visibleAt.After(now):
		case m.receiveCount == 0:
			state = "delayed"
		default:
			state = "in flight"
		}
		out = append(out, PeekedMessage{
			ID: m.id, Body: m.body, SentAt: m.sentAt, ReceiveCount: m.receiveCount,
			State: state, GroupID: m.groupID, Attributes: m.attributes,
		})
	}
	return out, matched, nil
}

func messageContains(m *message, needle string) bool {
	if needle == "" || strings.Contains(strings.ToLower(m.body), needle) {
		return true
	}
	for name, a := range m.attributes {
		if strings.Contains(strings.ToLower(name), needle) ||
			(a.StringValue != nil && strings.Contains(strings.ToLower(*a.StringValue), needle)) {
			return true
		}
	}
	return false
}

// DeleteByID deletes a message by its id, whether or not it is in flight. It
// is an administrator's action: SQS itself deletes only by receipt handle.
func (s *Service) DeleteByID(name, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	for _, m := range q.messages {
		if !m.deleted && m.id == id {
			q.remove(m)
			wake(q)
			return nil
		}
	}
	return errInvalidParameter("No message with id " + id + " in queue " + name + ".")
}

// Redrive moves every message of the dead-letter queue dlq back to source, a
// queue whose redrive policy names dlq, as StartMessageMoveTask does: each
// arrives as a new message, visible at once and never received. It returns
// how many it moved.
func (s *Service) Redrive(dlq, source string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from, err := s.queueLocked(dlq)
	if err != nil {
		return 0, err
	}
	to, err := s.queueLocked(source)
	if err != nil {
		return 0, err
	}
	if to.cfg.redrive == nil || to.cfg.redrive.target != dlq {
		return 0, errInvalidParameter("Queue " + source + " does not send its failed messages to " + dlq + ".")
	}
	return s.redriveMessagesLocked(from, func(*message) (*queue, error) { return to, nil }, s.clock.Now())
}

// redriveMessagesLocked moves every message of from to the queue chosen by
// resolveDest, clearing delivery state so each arrives as a fresh message.
// Destinations are resolved before any queue is mutated so errors are atomic.
func (s *Service) redriveMessagesLocked(from *queue, resolveDest func(*message) (*queue, error), now time.Time) (int, error) {
	type move struct {
		msg *message
		to  *queue
	}
	var moves []move
	for _, m := range from.messages {
		if m.deleted {
			continue
		}
		to, err := resolveDest(m)
		if err != nil {
			return 0, err
		}
		moves = append(moves, move{msg: m, to: to})
	}
	woken := make(map[*queue]bool)
	for _, mv := range moves {
		c := *mv.msg
		c.receipt, c.deadLetterSource, c.receiveCount = "", "", 0
		c.firstReceivedAt, c.visibleAt, c.deleted = time.Time{}, now, false
		if mv.to.cfg.fifo {
			c.sequence = mv.to.fifo.nextSequence()
		}
		mv.to.append(&c)
		woken[mv.to] = true
	}
	from.clearMessages()
	for q := range woken {
		wake(q)
	}
	return len(moves), nil
}
