package sqs

import "net/http"

type startMessageMoveTaskInput struct {
	SourceArn                    string
	DestinationArn               string
	MaxNumberOfMessagesPerSecond *int
}

func (s *Service) startMessageMoveTask(_ *http.Request, in startMessageMoveTaskInput) (any, error) {
	handle, err := s.StartMessageMoveTask(in.SourceArn, in.DestinationArn, in.MaxNumberOfMessagesPerSecond)
	if err != nil {
		return nil, err
	}
	return struct {
		TaskHandle string
	}{TaskHandle: handle}, nil
}

type listMessageMoveTasksInput struct {
	SourceArn  string
	MaxResults *int
}

type listMessageMoveTasksEntry struct {
	TaskHandle                        string `json:",omitempty"`
	Status                            string
	SourceArn                         string
	DestinationArn                    string `json:",omitempty"`
	MaxNumberOfMessagesPerSecond      *int   `json:",omitempty"`
	ApproximateNumberOfMessagesMoved  int64
	ApproximateNumberOfMessagesToMove *int64 `json:",omitempty"`
	FailureReason                     string `json:",omitempty"`
	StartedTimestamp                  int64
}

func (s *Service) listMessageMoveTasks(_ *http.Request, in listMessageMoveTasksInput) (any, error) {
	tasks, err := s.ListMessageMoveTasks(in.SourceArn, in.MaxResults)
	if err != nil {
		return nil, err
	}
	entries := make([]listMessageMoveTasksEntry, 0, len(tasks))
	for _, t := range tasks {
		entries = append(entries, listMessageMoveTasksEntry(t))
	}
	return struct {
		Results []listMessageMoveTasksEntry
	}{Results: entries}, nil
}

type cancelMessageMoveTaskInput struct {
	TaskHandle string
}

func (s *Service) cancelMessageMoveTask(_ *http.Request, in cancelMessageMoveTaskInput) (any, error) {
	moved, err := s.CancelMessageMoveTask(in.TaskHandle)
	if err != nil {
		return nil, err
	}
	return struct {
		ApproximateNumberOfMessagesMoved int64
	}{ApproximateNumberOfMessagesMoved: moved}, nil
}
