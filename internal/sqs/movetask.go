package sqs

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	moveStatusCompleted = "COMPLETED"
	maxMoveTaskHistory  = 10
	maxMoveTaskRate     = 500
)

type moveTask struct {
	taskHandle     string
	status         string
	sourceARN      string
	destinationARN string
	maxPerSecond   *int
	moved          int64
	startedAt      time.Time
}

// MoveTask is one entry returned by ListMessageMoveTasks.
type MoveTask struct {
	TaskHandle                        string
	Status                            string
	SourceArn                         string
	DestinationArn                    string
	MaxNumberOfMessagesPerSecond      *int
	ApproximateNumberOfMessagesMoved  int64
	ApproximateNumberOfMessagesToMove *int64
	FailureReason                     string
	StartedTimestamp                  int64
}

// StartMessageMoveTask moves every message from the dead-letter queue named by
// sourceARN back to its original source queue(s) or to destinationARN. In the
// emulator the move completes at once and is recorded as COMPLETED.
func (s *Service) StartMessageMoveTask(sourceARN, destinationARN string, maxPerSecond *int) (string, error) {
	if maxPerSecond != nil && (*maxPerSecond < 1 || *maxPerSecond > maxMoveTaskRate) {
		return "", errInvalidParameter("Value " + strconv.Itoa(*maxPerSecond) +
			" for parameter MaxNumberOfMessagesPerSecond is invalid. Reason: Must be between 1 and " +
			strconv.Itoa(maxMoveTaskRate) + ", if provided.")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	from, err := s.queueByARNLocked("SourceArn", sourceARN)
	if err != nil {
		return "", err
	}
	sources := s.deadLetterSourcesLocked(from.name)
	if len(sources) == 0 {
		return "", errUnsupportedOperation("The source queue must be configured as a dead-letter queue for another Amazon SQS queue.")
	}

	var resolveDest func(*message) (*queue, error)
	if destinationARN != "" {
		to, err := s.queueByARNLocked("DestinationArn", destinationARN)
		if err != nil {
			return "", err
		}
		if to == from {
			return "", errInvalidParameter("DestinationArn cannot be the same as SourceArn.")
		}
		if to.cfg.fifo != from.cfg.fifo {
			return "", errUnsupportedOperation("The destination queue must be of the same type as the source queue (FIFO or standard).")
		}
		resolveDest = func(*message) (*queue, error) { return to, nil }
	} else {
		resolveDest = func(m *message) (*queue, error) {
			if m.deadLetterSource != "" {
				if srcName, err := s.parseQueueARN("DeadLetterQueueSourceArn", m.deadLetterSource); err == nil {
					if src, err := s.queueLocked(srcName); err == nil && src != from && src.cfg.fifo == from.cfg.fifo {
						return src, nil
					}
				}
			}
			if len(sources) == 1 {
				return sources[0], nil
			}
			if m.deadLetterSource != "" {
				return nil, errResourceNotFound("The original source queue for a message no longer exists; specify DestinationArn.")
			}
			return nil, errUnsupportedOperation("Cannot determine the destination queue for all messages; specify DestinationArn.")
		}
	}

	now := s.clock.Now()
	moved, err := s.redriveMessagesLocked(from, resolveDest, now)
	if err != nil {
		return "", err
	}

	handle := newTaskHandle(sourceARN)
	recordMoveTaskLocked(from, moveTask{
		taskHandle:     handle,
		status:         moveStatusCompleted,
		sourceARN:      sourceARN,
		destinationARN: destinationARN,
		maxPerSecond:   cloneIntPtr(maxPerSecond),
		moved:          int64(moved),
		startedAt:      now,
	})
	return handle, nil
}

// ListMessageMoveTasks returns up to maxResults (default 1, max 10) most recent
// message movement tasks for the queue named by sourceARN, newest first.
func (s *Service) ListMessageMoveTasks(sourceARN string, maxResults *int) ([]MoveTask, error) {
	limit := 1
	if maxResults != nil {
		if *maxResults < 1 || *maxResults > maxMoveTaskHistory {
			return nil, errInvalidParameter("Value " + strconv.Itoa(*maxResults) +
				" for parameter MaxResults is invalid. Reason: MaxResults must be an integer between 1 and " +
				strconv.Itoa(maxMoveTaskHistory) + ".")
		}
		limit = *maxResults
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	q, err := s.queueByARNLocked("SourceArn", sourceARN)
	if err != nil {
		return nil, err
	}
	count := min(limit, len(q.moveTasks))
	out := make([]MoveTask, 0, count)
	for i := len(q.moveTasks) - 1; i >= 0 && len(out) < limit; i-- {
		t := q.moveTasks[i]
		toMove := int64(0)
		entry := MoveTask{
			Status:                            t.status,
			SourceArn:                         t.sourceARN,
			DestinationArn:                    t.destinationARN,
			MaxNumberOfMessagesPerSecond:      cloneIntPtr(t.maxPerSecond),
			ApproximateNumberOfMessagesMoved:  t.moved,
			ApproximateNumberOfMessagesToMove: &toMove,
			StartedTimestamp:                  t.startedAt.UnixMilli(),
		}
		// AWS SQS populates TaskHandle in ListMessageMoveTasks only for RUNNING tasks.
		if t.status == "RUNNING" {
			entry.TaskHandle = t.taskHandle
		}
		out = append(out, entry)
	}
	return out, nil
}

// CancelMessageMoveTask rejects cancellation of completed or unknown tasks with
// ResourceNotFoundException, matching SQS (tasks complete immediately here).
func (s *Service) CancelMessageMoveTask(taskHandle string) (int64, error) {
	if taskHandle == "" {
		return 0, errMissingParameter("TaskHandle")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, q := range s.queues {
		for _, t := range q.moveTasks {
			if t.taskHandle == taskHandle {
				return 0, errResourceNotFound("Message move task cannot be cancelled because its status is " + t.status + ".")
			}
		}
	}
	return 0, errResourceNotFound("The specified message move task handle does not exist.")
}

func (s *Service) parseQueueARN(param, arn string) (string, error) {
	if arn == "" {
		return "", errMissingParameter(param)
	}
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[1] == "" || parts[2] != "sqs" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
		return "", errInvalidAddress("Value " + arn + " for parameter " + param + " is invalid. Reason: Must be a valid SQS queue ARN.")
	}
	if parts[1] != "aws" || parts[3] != s.identity.Region || parts[4] != s.identity.Account {
		return "", errResourceNotFound("The specified queue does not exist.")
	}
	return parts[5], nil
}

func (s *Service) queueByARNLocked(param, arn string) (*queue, error) {
	name, err := s.parseQueueARN(param, arn)
	if err != nil {
		return nil, err
	}
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, errResourceNotFound("The specified queue does not exist.")
	}
	return q, nil
}

func (s *Service) deadLetterSourcesLocked(dlqName string) []*queue {
	var sources []*queue
	for _, q := range s.queues {
		if q.name != dlqName && q.cfg.redrive != nil && q.cfg.redrive.target == dlqName {
			sources = append(sources, q)
		}
	}
	slices.SortFunc(sources, func(a, b *queue) int {
		return strings.Compare(a.name, b.name)
	})
	return sources
}

func newTaskHandle(sourceARN string) string {
	payload, _ := json.Marshal(struct {
		TaskID    string `json:"taskId"`
		SourceARN string `json:"sourceArn"`
	}{
		TaskID:    awsapi.NewID(),
		SourceARN: sourceARN,
	})
	return base64.StdEncoding.EncodeToString(payload)
}

func recordMoveTaskLocked(q *queue, t moveTask) {
	q.moveTasks = append(q.moveTasks, t)
	if len(q.moveTasks) > maxMoveTaskHistory {
		copy(q.moveTasks, q.moveTasks[len(q.moveTasks)-maxMoveTaskHistory:])
		q.moveTasks = q.moveTasks[:maxMoveTaskHistory]
	}
}
