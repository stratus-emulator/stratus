package kinesis

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	contentType = "application/x-amz-json-1.1"
	cborType    = "application/x-amz-cbor-1.1"

	// maxRequestBytes leaves room for a 5 MiB PutRecords batch in base64.
	maxRequestBytes = 16 << 20
)

type operation func(s *Service, body []byte) (any, error)

var operations = map[string]operation{
	"CreateStream":                  op((*Service).createStreamOp),
	"DeleteStream":                  op((*Service).deleteStreamOp),
	"DescribeStream":                op((*Service).describeStream),
	"DescribeStreamSummary":         op((*Service).describeStreamSummary),
	"ListStreams":                   op((*Service).listStreams),
	"ListShards":                    op((*Service).listShards),
	"PutRecord":                     op((*Service).putRecord),
	"PutRecords":                    op((*Service).putRecords),
	"GetShardIterator":              op((*Service).getShardIterator),
	"GetRecords":                    op((*Service).getRecordsOp),
	"IncreaseStreamRetentionPeriod": op((*Service).increaseRetention),
	"DecreaseStreamRetentionPeriod": op((*Service).decreaseRetention),
	"StartStreamEncryption":         op((*Service).startStreamEncryption),
	"StopStreamEncryption":          op((*Service).stopStreamEncryption),
	"AddTagsToStream":               op((*Service).addTags),
	"RemoveTagsFromStream":          op((*Service).removeTags),
	"ListTagsForStream":             op((*Service).listTags),
}

// op adapts a typed operation: it decodes the request body into In.
func op[In any](fn func(*Service, In) (any, error)) operation {
	return func(s *Service, body []byte) (any, error) {
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
		return fn(s, in)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out, err := s.serve(w, r)
	if err != nil {
		var apiErr *awsapi.Error
		if !errors.As(err, &apiErr) {
			apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
		}
		writeError(w, apiErr)
		return
	}
	awsapi.WriteJSON(w, contentType, http.StatusOK, out)
}

// writeError sends the awsJson1.1 error body. Kinesis names the shape without
// a namespace, so this does not go through awsapi.WriteJSONError, which adds one.
func writeError(w http.ResponseWriter, e *awsapi.Error) {
	awsapi.WriteJSON(w, contentType, e.Status, map[string]string{"__type": e.Code, "message": e.Message})
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) (any, error) {
	if strings.HasPrefix(r.Header.Get("Content-Type"), cborType) {
		return nil, errInvalidArgument("stratus speaks only the JSON encoding of Kinesis (application/x-amz-json-1.1); CBOR is not supported.")
	}
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, &awsapi.Error{
			Status:  http.StatusBadRequest,
			Code:    "UnsupportedProtocol",
			Message: "stratus speaks only the Kinesis JSON protocol: the request has no X-Amz-Target header.",
		}
	}
	name := strings.TrimPrefix(target, "Kinesis_20131202.")
	handle, ok := operations[name]
	if !ok {
		return nil, errUnsupported(name)
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return nil, errInvalidArgument("The request body could not be read or is too large.")
	}
	return handle(s, body)
}

type empty struct{}

// streamRef is how every operation names its stream.
type streamRef struct {
	StreamName string
	StreamARN  string
}

func (s *Service) resolve(ref streamRef) (*stream, error) {
	return s.lookup(ref.StreamName, ref.StreamARN)
}

// epoch renders a time as the epoch seconds, with milliseconds, the JSON
// protocol uses for timestamps.
func epoch(t time.Time) float64 { return float64(t.UnixMilli()) / 1000 }

// fromEpoch is the inverse of epoch.
func fromEpoch(sec float64) time.Time { return time.UnixMilli(int64(sec * 1000)) }
