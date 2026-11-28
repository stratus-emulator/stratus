package sqs

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	targetPrefix = "AmazonSQS."
	contentType  = "application/x-amz-json-1.0"
	namespace    = "com.amazonaws.sqs"

	// maxRequestBytes leaves room for a full-size message and its JSON framing.
	maxRequestBytes = 2 * maxMessageBytes
)

type operation func(s *Service, r *http.Request, body []byte) (any, error)

var operations = map[string]operation{
	"CreateQueue":             op((*Service).createQueue),
	"DeleteQueue":             op((*Service).deleteQueue),
	"GetQueueUrl":             op((*Service).getQueueURL),
	"ListQueues":              op((*Service).listQueues),
	"GetQueueAttributes":      op((*Service).getQueueAttributes),
	"SetQueueAttributes":      op((*Service).setQueueAttributes),
	"PurgeQueue":              op((*Service).purgeQueue),
	"SendMessage":             op((*Service).sendMessage),
	"ReceiveMessage":          op((*Service).receiveMessage),
	"DeleteMessage":           op((*Service).deleteMessage),
	"ChangeMessageVisibility": op((*Service).changeMessageVisibility),

	"SendMessageBatch":             op((*Service).sendMessageBatch),
	"DeleteMessageBatch":           op((*Service).deleteMessageBatch),
	"ChangeMessageVisibilityBatch": op((*Service).changeMessageVisibilityBatch),
	"ListDeadLetterSourceQueues":   op((*Service).listDeadLetterSourceQueues),
	"TagQueue":                     op((*Service).tagQueue),
	"UntagQueue":                   op((*Service).untagQueue),
	"ListQueueTags":                op((*Service).listQueueTags),
	"StartMessageMoveTask":         op((*Service).startMessageMoveTask),
	"ListMessageMoveTasks":         op((*Service).listMessageMoveTasks),
	"CancelMessageMoveTask":        op((*Service).cancelMessageMoveTask),
}

// op adapts a typed operation: it decodes the request body into In.
func op[In any](fn func(*Service, *http.Request, In) (any, error)) operation {
	return func(s *Service, r *http.Request, body []byte) (any, error) {
		var in In
		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, &awsapi.Error{
					Status:  http.StatusBadRequest,
					Code:    "SerializationException",
					Message: "The request body is not valid JSON for this operation: " + err.Error(),
				}
			}
		}
		return fn(s, r, in)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Amz-Target") == "" && isQueryRequest(r) {
		s.serveQuery(w, r)
		return
	}
	out, err := s.serve(w, r)
	if err != nil {
		var apiErr *awsapi.Error
		if !errors.As(err, &apiErr) {
			apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
		}
		awsapi.WriteJSONError(w, contentType, namespace, apiErr)
		return
	}
	awsapi.WriteJSON(w, contentType, http.StatusOK, out)
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) (any, error) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, &awsapi.Error{
			Status:  http.StatusBadRequest,
			Code:    "UnsupportedProtocol",
			Message: "stratus speaks the SQS JSON protocol and the query protocol: the request has no X-Amz-Target header and no Action.",
		}
	}
	name := strings.TrimPrefix(target, targetPrefix)
	handle, ok := operations[name]
	if !ok {
		return nil, &awsapi.Error{
			Status:  http.StatusBadRequest,
			Code:    "InvalidAction",
			Message: "stratus does not implement the SQS operation " + name + ".",
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return nil, errInvalidParameter("The request body could not be read or is too large.")
	}
	return handle(s, r, body)
}

type empty struct{}

type createQueueInput struct {
	QueueName  string
	Attributes map[string]string
	Tags       map[string]string // the wire name is "tags"; decoding ignores case
}

func (s *Service) createQueue(r *http.Request, in createQueueInput) (any, error) {
	if err := s.CreateQueueWithTags(in.QueueName, in.Attributes, in.Tags); err != nil {
		return nil, err
	}
	return map[string]string{"QueueUrl": queueURL(r, s.identity.Account, in.QueueName)}, nil
}

type queueInput struct {
	QueueUrl string //nolint:revive // the wire name
}

func (s *Service) deleteQueue(_ *http.Request, in queueInput) (any, error) {
	return empty{}, s.DeleteQueue(queueName(in.QueueUrl))
}

func (s *Service) purgeQueue(_ *http.Request, in queueInput) (any, error) {
	return empty{}, s.Purge(queueName(in.QueueUrl))
}

type getQueueURLInput struct {
	QueueName string
}

func (s *Service) getQueueURL(r *http.Request, in getQueueURLInput) (any, error) {
	if !s.Exists(in.QueueName) {
		return nil, errQueueNotFound()
	}
	return map[string]string{"QueueUrl": queueURL(r, s.identity.Account, in.QueueName)}, nil
}

type listQueuesInput struct {
	QueueNamePrefix string
	MaxResults      *int
	NextToken       string
}

func (s *Service) listQueues(r *http.Request, in listQueuesInput) (any, error) {
	limit, err := pageSize(in.MaxResults)
	if err != nil {
		return nil, err
	}
	names, next, err := s.ListQueuesPage(in.QueueNamePrefix, limit, in.NextToken)
	if err != nil {
		return nil, err
	}
	return s.queueURLs(r, names, next), nil
}

type getQueueAttributesInput struct {
	QueueUrl       string //nolint:revive // the wire name
	AttributeNames []string
}

func (s *Service) getQueueAttributes(_ *http.Request, in getQueueAttributesInput) (any, error) {
	all, err := s.Attributes(queueName(in.QueueUrl))
	if err != nil {
		return nil, err
	}
	if len(in.AttributeNames) == 0 {
		return empty{}, nil
	}
	if !slices.Contains(in.AttributeNames, "All") {
		for name := range all {
			if !slices.Contains(in.AttributeNames, name) {
				delete(all, name)
			}
		}
	}
	return map[string]map[string]string{"Attributes": all}, nil
}

type setQueueAttributesInput struct {
	QueueUrl   string //nolint:revive // the wire name
	Attributes map[string]string
}

func (s *Service) setQueueAttributes(_ *http.Request, in setQueueAttributesInput) (any, error) {
	return empty{}, s.SetAttributes(queueName(in.QueueUrl), in.Attributes)
}

func optionalSeconds(name string, value *int, limit time.Duration) (*time.Duration, error) {
	if value == nil {
		return nil, nil
	}
	d := time.Duration(*value) * time.Second
	if *value < 0 || d > limit {
		return nil, errInvalidParameter("Value " + strconv.Itoa(*value) + " for parameter " + name +
			" is invalid. Reason: Must be between 0 and " + seconds(limit) + ", if provided.")
	}
	return &d, nil
}

// queueURL builds a queue's URL on the host the client used, so that the same
// queue is reachable as localhost and under a container name.
func queueURL(r *http.Request, account, name string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/" + account + "/" + name
}

// queueName takes the queue's name out of its URL; any host is accepted.
func queueName(queueURL string) string {
	u, err := url.Parse(queueURL)
	if err != nil || u.Path == "" {
		return ""
	}
	return path.Base(u.Path)
}
