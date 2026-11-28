package sqs

import (
	"errors"
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// restored is a new service, on the same clock, given s's snapshot.
func restored(t *testing.T, s *Service) *Service {
	t.Helper()
	data, err := s.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	next := New(testIdentity, s.clock)
	if err := next.Restore(data, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return next
}

func TestASnapshotKeepsQueuesAndMessages(t *testing.T) {
	s, clk := newService(t)
	mustCreate(t, s, "dlq", nil)
	mustCreate(t, s, "jobs", map[string]string{
		"VisibilityTimeout": "60",
		"RedrivePolicy":     `{"deadLetterTargetArn":"` + s.arn("dlq") + `","maxReceiveCount":"2"}`,
	})
	if err := s.TagQueue("jobs", map[string]string{"team": "mail"}); err != nil {
		t.Fatal(err)
	}
	mustSend(t, s, "jobs", "waiting")
	mustSend(t, s, "jobs", "in flight")
	got := receive(t, s, "jobs", 1)
	if len(got) != 1 {
		t.Fatalf("received %d messages, want 1", len(got))
	}
	inFlight := got[0]

	next := restored(t, s)

	attrs, err := next.Attributes("jobs")
	if err != nil {
		t.Fatalf("Attributes after Restore: %v", err)
	}
	before, _ := s.Attributes("jobs")
	for _, name := range []string{"VisibilityTimeout", "RedrivePolicy", "ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp", "QueueArn"} {
		if attrs[name] != before[name] {
			t.Errorf("%s = %q after Restore, %q before", name, attrs[name], before[name])
		}
	}
	tags, _ := next.ListQueueTags("jobs")
	if tags["team"] != "mail" {
		t.Errorf("tags after Restore = %v", tags)
	}
	// The in-flight message keeps its receipt handle and its visibility timeout.
	if err := next.ChangeVisibility("jobs", inFlight.Receipt, 0); err != nil {
		t.Fatalf("the receipt handle of a message in flight is not valid after Restore: %v", err)
	}
	clk.Advance(time.Second)
	again := receive(t, next, "jobs", 10)
	if len(again) != 2 {
		t.Fatalf("received %d messages after Restore, want both", len(again))
	}
	for _, m := range again {
		if m.MessageID == inFlight.MessageID && m.ReceiveCount != 2 {
			t.Errorf("receive count of the message in flight = %d, want 2", m.ReceiveCount)
		}
	}
}

func TestASnapshotKeepsFIFODeduplication(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "ledger.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	first, err := s.Send("ledger.fifo", SendInput{Body: "x", GroupID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	next := restored(t, s)
	dup, err := next.Send("ledger.fifo", SendInput{Body: "x", GroupID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if dup.MessageID != first.MessageID {
		t.Errorf("a duplicate after Restore got message %s, want the first send's %s", dup.MessageID, first.MessageID)
	}
	other, err := next.Send("ledger.fifo", SendInput{Body: "y", GroupID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if other.SequenceNumber <= first.SequenceNumber {
		t.Errorf("sequence number after Restore %s is not after %s", other.SequenceNumber, first.SequenceNumber)
	}
}

func TestRestoreRefusesAnotherFormat(t *testing.T) {
	s, _ := newService(t)
	err := s.Restore([]byte(`{"format":99}`), nil)
	var apiErr *awsapi.Error
	if err == nil || errors.As(err, &apiErr) {
		t.Fatalf("Restore of format 99: %v, want a plain error", err)
	}
}

func TestSnapshotKeepsMoveTasks(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "dlq", nil)
	mustCreate(t, s, "jobs", map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"` + s.arn("dlq") + `","maxReceiveCount":"1"}`,
	})
	mustSend(t, s, "dlq", "stuck")

	rate := 50
	handle, err := s.StartMessageMoveTask(s.arn("dlq"), s.arn("jobs"), &rate)
	if err != nil {
		t.Fatalf("StartMessageMoveTask: %v", err)
	}

	next := restored(t, s)

	tasks, err := next.ListMessageMoveTasks(s.arn("dlq"), nil)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("ListMessageMoveTasks after Restore = %+v, %v", tasks, err)
	}
	if tasks[0].Status != "COMPLETED" ||
		tasks[0].DestinationArn != s.arn("jobs") ||
		tasks[0].ApproximateNumberOfMessagesMoved != 1 ||
		tasks[0].MaxNumberOfMessagesPerSecond == nil || *tasks[0].MaxNumberOfMessagesPerSecond != 50 {
		t.Fatalf("restored task = %+v", tasks[0])
	}
	_, err = next.CancelMessageMoveTask(handle)
	wantCode(t, err, "ResourceNotFoundException")
}
