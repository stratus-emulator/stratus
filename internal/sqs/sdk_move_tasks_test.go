package sqs_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/gateway"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

func queueARN(t *testing.T, c *awssqs.Client, queueURL *string) string {
	t.Helper()
	attrs, err := c.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	return attrs.Attributes["QueueArn"]
}

func deadLetterOne(t *testing.T, c *awssqs.Client, srcURL *string, body string) {
	t.Helper()
	if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl:    srcURL,
		MessageBody: aws.String(body),
	}); err != nil {
		t.Fatalf("SendMessage(%q): %v", body, err)
	}
	got, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: srcURL})
	if err != nil || len(got.Messages) != 1 {
		t.Fatalf("first receive(%q): %+v, %v", body, got, err)
	}
	if _, err := c.ChangeMessageVisibility(t.Context(), &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: srcURL, ReceiptHandle: got.Messages[0].ReceiptHandle, VisibilityTimeout: 0,
	}); err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	// maxReceiveCount=1: the second receive moves the message to the DLQ.
	if again, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: srcURL}); err != nil || len(again.Messages) != 0 {
		t.Fatalf("second receive(%q): %+v, %v", body, again, err)
	}
}

func wantAPIErrorCode(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %q, got nil", wantCode)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != wantCode {
		t.Fatalf("error = %v, want code %q", err, wantCode)
	}
}

func TestSDKMessageMoveTasksDefaultAndExplicitDestination(t *testing.T) {
	c := newSDKClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	dlqARN := queueARN(t, c, dlq)
	policy := `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"1"}`
	src := createQueue(t, c, "src", map[string]string{"RedrivePolicy": policy})
	srcARN := queueARN(t, c, src)
	custom := createQueue(t, c, "custom", nil)
	customARN := queueARN(t, c, custom)

	// Empty list before any move task.
	initial, err := c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{SourceArn: aws.String(dlqARN)})
	if err != nil || len(initial.Results) != 0 {
		t.Fatalf("initial ListMessageMoveTasks = %+v, %v", initial, err)
	}

	// One message dead-lettered from src, one sent directly to the DLQ.
	deadLetterOne(t, c, src, "from-src")
	if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: dlq, MessageBody: aws.String("direct"),
	}); err != nil {
		t.Fatal(err)
	}

	startOut, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(dlqARN),
	})
	if err != nil || aws.ToString(startOut.TaskHandle) == "" {
		t.Fatalf("StartMessageMoveTask (default dest): %+v, %v", startOut, err)
	}

	// DLQ should now be empty, and src should hold both messages with fresh delivery counts.
	if left, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: dlq, MaxNumberOfMessages: 10}); err != nil || len(left.Messages) != 0 {
		t.Fatalf("DLQ after move = %+v, %v", left, err)
	}
	got, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl:                    src,
		MaxNumberOfMessages:         10,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil || len(got.Messages) != 2 {
		t.Fatalf("src after move = %+v, %v", got, err)
	}
	for _, m := range got.Messages {
		if m.Attributes["ApproximateReceiveCount"] != "1" || m.Attributes["DeadLetterQueueSourceArn"] != "" {
			t.Fatalf("moved message kept stale attributes: %+v", m.Attributes)
		}
	}

	listed, err := c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{SourceArn: aws.String(dlqARN)})
	if err != nil || len(listed.Results) != 1 {
		t.Fatalf("ListMessageMoveTasks = %+v, %v", listed, err)
	}
	entry := listed.Results[0]
	if aws.ToString(entry.Status) != "COMPLETED" ||
		aws.ToString(entry.SourceArn) != dlqARN ||
		entry.DestinationArn != nil ||
		entry.TaskHandle != nil ||
		entry.MaxNumberOfMessagesPerSecond != nil ||
		entry.ApproximateNumberOfMessagesMoved != 2 ||
		aws.ToInt64(entry.ApproximateNumberOfMessagesToMove) != 0 ||
		entry.StartedTimestamp <= 0 {
		t.Fatalf("unexpected ListMessageMoveTasks entry: %+v", entry)
	}

	// Cancelling an already completed task returns ResourceNotFoundException.
	_, err = c.CancelMessageMoveTask(t.Context(), &awssqs.CancelMessageMoveTaskInput{TaskHandle: startOut.TaskHandle})
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Fatalf("CancelMessageMoveTask on completed task = %v, want *types.ResourceNotFoundException", err)
	}

	// Explicit custom DestinationArn and MaxNumberOfMessagesPerSecond.
	if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: dlq, MessageBody: aws.String("to-custom"),
	}); err != nil {
		t.Fatal(err)
	}
	start2, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn:                    aws.String(dlqARN),
		DestinationArn:               aws.String(customARN),
		MaxNumberOfMessagesPerSecond: aws.Int32(25),
	})
	if err != nil || aws.ToString(start2.TaskHandle) == "" || aws.ToString(start2.TaskHandle) == aws.ToString(startOut.TaskHandle) {
		t.Fatalf("second StartMessageMoveTask = %+v, %v", start2, err)
	}
	customGot, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: custom, MaxNumberOfMessages: 10})
	if err != nil || len(customGot.Messages) != 1 || aws.ToString(customGot.Messages[0].Body) != "to-custom" {
		t.Fatalf("custom destination = %+v, %v", customGot, err)
	}

	// Default MaxResults=1 returns only the most recent task.
	latestOnly, err := c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{SourceArn: aws.String(dlqARN)})
	if err != nil || len(latestOnly.Results) != 1 || aws.ToString(latestOnly.Results[0].DestinationArn) != customARN ||
		aws.ToInt32(latestOnly.Results[0].MaxNumberOfMessagesPerSecond) != 25 || latestOnly.Results[0].ApproximateNumberOfMessagesMoved != 1 {
		t.Fatalf("latest ListMessageMoveTasks = %+v, %v", latestOnly, err)
	}

	// Explicit MaxResults=10 returns newest first.
	both, err := c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{
		SourceArn:  aws.String(dlqARN),
		MaxResults: aws.Int32(10),
	})
	if err != nil || len(both.Results) != 2 || aws.ToString(both.Results[0].DestinationArn) != customARN || both.Results[1].DestinationArn != nil {
		t.Fatalf("ListMessageMoveTasks(MaxResults=10) = %+v, %v", both, err)
	}
	_ = srcARN
}

func TestSDKMessageMoveTasksMultipleSourcesAndFifo(t *testing.T) {
	c := newSDKClient(t)
	dlq := createQueue(t, c, "shared-dlq", nil)
	dlqARN := queueARN(t, c, dlq)
	policy := `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"1"}`
	srcA := createQueue(t, c, "src-a", map[string]string{"RedrivePolicy": policy})
	srcB := createQueue(t, c, "src-b", map[string]string{"RedrivePolicy": policy})

	deadLetterOne(t, c, srcA, "msg-a")
	deadLetterOne(t, c, srcB, "msg-b")

	if _, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(dlqARN),
	}); err != nil {
		t.Fatalf("StartMessageMoveTask multi-source: %v", err)
	}

	gotA, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: srcA, MaxNumberOfMessages: 10})
	if err != nil || len(gotA.Messages) != 1 || aws.ToString(gotA.Messages[0].Body) != "msg-a" {
		t.Fatalf("src-a = %+v, %v", gotA, err)
	}
	gotB, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: srcB, MaxNumberOfMessages: 10})
	if err != nil || len(gotB.Messages) != 1 || aws.ToString(gotB.Messages[0].Body) != "msg-b" {
		t.Fatalf("src-b = %+v, %v", gotB, err)
	}

	// Ambiguous direct message without DestinationArn fails atomically without moving messages.
	if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{QueueUrl: dlq, MessageBody: aws.String("ambiguous")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{SourceArn: aws.String(dlqARN)})
	var unsupported *types.UnsupportedOperation
	if !errors.As(err, &unsupported) {
		t.Fatalf("ambiguous multi-source move = %v, want *types.UnsupportedOperation", err)
	}
	stillInDLQ, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: dlq, MaxNumberOfMessages: 10})
	if err != nil || len(stillInDLQ.Messages) != 1 {
		t.Fatalf("message should remain in DLQ on failure: %+v, %v", stillInDLQ, err)
	}

	// FIFO DLQ to FIFO source queue preserves MessageGroupId and delivers cleanly.
	fifoDLQ := createQueue(t, c, "dlq.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	fifoDLQARN := queueARN(t, c, fifoDLQ)
	fifoSrc := createQueue(t, c, "src.fifo", map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "true",
		"RedrivePolicy":             `{"deadLetterTargetArn":"` + fifoDLQARN + `","maxReceiveCount":"1"}`,
	})
	if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: fifoSrc, MessageBody: aws.String("fifo-poison"), MessageGroupId: aws.String("grp-1"),
	}); err != nil {
		t.Fatal(err)
	}
	firstFifo, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: fifoSrc})
	if err != nil || len(firstFifo.Messages) != 1 {
		t.Fatalf("first fifo receive: %+v, %v", firstFifo, err)
	}
	if _, err := c.ChangeMessageVisibility(t.Context(), &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: fifoSrc, ReceiptHandle: firstFifo.Messages[0].ReceiptHandle, VisibilityTimeout: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if again, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: fifoSrc}); err != nil || len(again.Messages) != 0 {
		t.Fatalf("second fifo receive: %+v, %v", again, err)
	}
	if _, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{SourceArn: aws.String(fifoDLQARN)}); err != nil {
		t.Fatalf("StartMessageMoveTask FIFO: %v", err)
	}
	redrivenFifo, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl:                    fifoSrc,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil || len(redrivenFifo.Messages) != 1 || aws.ToString(redrivenFifo.Messages[0].Body) != "fifo-poison" ||
		redrivenFifo.Messages[0].Attributes["MessageGroupId"] != "grp-1" {
		t.Fatalf("redriven FIFO = %+v, %v", redrivenFifo, err)
	}
}

func TestSDKMessageMoveTasksErrorsAndHistoryCap(t *testing.T) {
	c := newSDKClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	dlqARN := queueARN(t, c, dlq)
	src := createQueue(t, c, "src", map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"1"}`,
	})
	_ = src
	notDLQ := createQueue(t, c, "plain", nil)
	notDLQARN := queueARN(t, c, notDLQ)
	fifoQueue := createQueue(t, c, "other.fifo", map[string]string{"FifoQueue": "true"})
	fifoARN := queueARN(t, c, fifoQueue)
	missingARN := "arn:aws:sqs:" + awsapi.DefaultRegion + ":" + awsapi.DefaultAccount + ":does-not-exist"

	// Nonexistent SourceArn.
	_, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{SourceArn: aws.String(missingARN)})
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Fatalf("StartMessageMoveTask missing source = %v, want *types.ResourceNotFoundException", err)
	}
	_, err = c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{SourceArn: aws.String(missingARN)})
	if !errors.As(err, &notFound) {
		t.Fatalf("ListMessageMoveTasks missing source = %v, want *types.ResourceNotFoundException", err)
	}

	// Nonexistent DestinationArn.
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(missingARN),
	})
	if !errors.As(err, &notFound) {
		t.Fatalf("StartMessageMoveTask missing dest = %v, want *types.ResourceNotFoundException", err)
	}

	// Malformed ARN.
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{SourceArn: aws.String("not-an-arn")})
	var invalidAddr *types.InvalidAddress
	if !errors.As(err, &invalidAddr) {
		t.Fatalf("StartMessageMoveTask malformed ARN = %v, want *types.InvalidAddress", err)
	}
	_, err = c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{SourceArn: aws.String("not-an-arn")})
	if !errors.As(err, &invalidAddr) {
		t.Fatalf("ListMessageMoveTasks malformed ARN = %v, want *types.InvalidAddress", err)
	}

	// Source queue is not a DLQ.
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{SourceArn: aws.String(notDLQARN)})
	var unsupported *types.UnsupportedOperation
	if !errors.As(err, &unsupported) {
		t.Fatalf("StartMessageMoveTask non-DLQ = %v, want *types.UnsupportedOperation", err)
	}

	// Destination FIFO/standard mismatch.
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(fifoARN),
	})
	if !errors.As(err, &unsupported) {
		t.Fatalf("StartMessageMoveTask FIFO mismatch = %v, want *types.UnsupportedOperation", err)
	}

	// DestinationArn == SourceArn.
	_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(dlqARN),
	})
	wantAPIErrorCode(t, err, "InvalidParameterValue")

	// Invalid MaxNumberOfMessagesPerSecond (<=0 or >500).
	for _, badRate := range []int32{0, -3, 501} {
		_, err = c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
			SourceArn:                    aws.String(dlqARN),
			MaxNumberOfMessagesPerSecond: aws.Int32(badRate),
		})
		wantAPIErrorCode(t, err, "InvalidParameterValue")
	}

	// Invalid MaxResults (<1 or >10).
	for _, badLimit := range []int32{0, -1, 11} {
		_, err = c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{
			SourceArn:  aws.String(dlqARN),
			MaxResults: aws.Int32(badLimit),
		})
		wantAPIErrorCode(t, err, "InvalidParameterValue")
	}

	// Invalid/unknown TaskHandle on CancelMessageMoveTask.
	_, err = c.CancelMessageMoveTask(t.Context(), &awssqs.CancelMessageMoveTaskInput{TaskHandle: aws.String("nonexistent-handle")})
	if !errors.As(err, &notFound) {
		t.Fatalf("CancelMessageMoveTask unknown handle = %v, want *types.ResourceNotFoundException", err)
	}
	_, err = c.CancelMessageMoveTask(t.Context(), &awssqs.CancelMessageMoveTaskInput{TaskHandle: aws.String("")})
	wantAPIErrorCode(t, err, "MissingParameter")

	// Empty DLQ move succeeds and history is capped at 10 entries, newest first.
	for i := range 12 {
		if _, err := c.StartMessageMoveTask(t.Context(), &awssqs.StartMessageMoveTaskInput{
			SourceArn:                    aws.String(dlqARN),
			MaxNumberOfMessagesPerSecond: aws.Int32(int32(i + 1)),
		}); err != nil {
			t.Fatalf("empty StartMessageMoveTask %d: %v", i, err)
		}
	}
	history, err := c.ListMessageMoveTasks(t.Context(), &awssqs.ListMessageMoveTasksInput{
		SourceArn:  aws.String(dlqARN),
		MaxResults: aws.Int32(10),
	})
	if err != nil || len(history.Results) != 10 {
		t.Fatalf("history len = %d, err = %v; want 10", len(history.Results), err)
	}
	if aws.ToInt32(history.Results[0].MaxNumberOfMessagesPerSecond) != 12 ||
		aws.ToInt32(history.Results[9].MaxNumberOfMessagesPerSecond) != 3 {
		t.Fatalf("history rates = newest:%v oldest:%v, want 12 and 3",
			history.Results[0].MaxNumberOfMessagesPerSecond, history.Results[9].MaxNumberOfMessagesPerSecond)
	}
}

func TestQueryProtocolMessageMoveTasks(t *testing.T) {
	svc := sqs.New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clock.System{})
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)

	call := func(kv ...string) (int, string) {
		form := url.Values{}
		for i := 0; i+1 < len(kv); i += 2 {
			form.Set(kv[i], kv[i+1])
		}
		return queryCall(t, srv, "/", form)
	}

	dlqARN := "arn:aws:sqs:" + awsapi.DefaultRegion + ":" + awsapi.DefaultAccount + ":dlq"
	srcARN := "arn:aws:sqs:" + awsapi.DefaultRegion + ":" + awsapi.DefaultAccount + ":src"

	if status, body := call("Action", "CreateQueue", "QueueName", "dlq"); status != http.StatusOK {
		t.Fatalf("CreateQueue(dlq): %d %s", status, body)
	}
	if status, body := call("Action", "CreateQueue", "QueueName", "src",
		"Attribute.1.Name", "RedrivePolicy",
		"Attribute.1.Value", `{"deadLetterTargetArn":"`+dlqARN+`","maxReceiveCount":"1"}`,
	); status != http.StatusOK {
		t.Fatalf("CreateQueue(src): %d %s", status, body)
	}

	dlqURL := srv.URL + "/" + awsapi.DefaultAccount + "/dlq"
	if status, body := call("Action", "SendMessage", "QueueUrl", dlqURL, "MessageBody", "payload"); status != http.StatusOK {
		t.Fatalf("SendMessage: %d %s", status, body)
	}

	status, body := call("Action", "StartMessageMoveTask",
		"SourceArn", dlqARN,
		"DestinationArn", srcARN,
		"MaxNumberOfMessagesPerSecond", "20",
	)
	if status != http.StatusOK {
		t.Fatalf("StartMessageMoveTask status = %d: %s", status, body)
	}
	wantContains(t, body, "<StartMessageMoveTaskResponse", "<StartMessageMoveTaskResult>", "<TaskHandle>", "</TaskHandle>")

	start := strings.Index(body, "<TaskHandle>") + len("<TaskHandle>")
	end := strings.Index(body, "</TaskHandle>")
	handle := body[start:end]

	status, body = call("Action", "ListMessageMoveTasks", "SourceArn", dlqARN, "MaxResults", "5")
	if status != http.StatusOK {
		t.Fatalf("ListMessageMoveTasks status = %d: %s", status, body)
	}
	wantContains(t, body,
		"<ListMessageMoveTasksResponse",
		"<ListMessageMoveTasksResult>",
		"<ListMessageMoveTasksResultEntry>",
		"<Status>COMPLETED</Status>",
		"<SourceArn>"+dlqARN+"</SourceArn>",
		"<DestinationArn>"+srcARN+"</DestinationArn>",
		"<MaxNumberOfMessagesPerSecond>20</MaxNumberOfMessagesPerSecond>",
		"<ApproximateNumberOfMessagesMoved>1</ApproximateNumberOfMessagesMoved>",
		"<ApproximateNumberOfMessagesToMove>0</ApproximateNumberOfMessagesToMove>",
		"<StartedTimestamp>",
	)
	if strings.Contains(body, "<TaskHandle>") {
		t.Fatalf("ListMessageMoveTasks should omit TaskHandle for COMPLETED tasks:\n%s", body)
	}

	status, body = call("Action", "CancelMessageMoveTask", "TaskHandle", handle)
	if status != http.StatusNotFound {
		t.Fatalf("CancelMessageMoveTask status = %d, want %d: %s", status, http.StatusNotFound, body)
	}
	wantContains(t, body, "<ErrorResponse", "<Type>Sender</Type>", "<Code>ResourceNotFoundException</Code>")

	status, body = call("Action", "StartMessageMoveTask", "SourceArn", srcARN)
	if status != http.StatusBadRequest {
		t.Fatalf("StartMessageMoveTask non-DLQ status = %d, want %d: %s", status, http.StatusBadRequest, body)
	}
	wantContains(t, body, "<Code>AWS.SimpleQueueService.UnsupportedOperation</Code>")

	// Verify message actually landed in src.
	status, body = call("Action", "ReceiveMessage", "QueueUrl", srv.URL+"/"+awsapi.DefaultAccount+"/src",
		"MaxNumberOfMessages", strconv.Itoa(10))
	if status != http.StatusOK {
		t.Fatalf("ReceiveMessage status = %d: %s", status, body)
	}
	wantContains(t, body, "<Body>payload</Body>")
}
