package sqs

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// The legacy query protocol is served by translating a request into the JSON
// form the operations already decode, and the JSON result back into XML, so that
// both protocols share one implementation of every operation.

const (
	queryNamespace   = "http://queue.amazonaws.com/doc/2012-11-05/"
	formContentType  = "application/x-www-form-urlencoded"
	xmlContentType   = "text/xml"
	maxFormParameter = 4096
)

// isQueryRequest tells a legacy request, which carries an Action, from a request
// that is not SQS at all.
func isQueryRequest(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Content-Type"), formContentType) || r.URL.Query().Has("Action")
}

// intMembers are the request members that are numbers in the JSON form.
var intMembers = map[string]bool{
	"DelaySeconds": true, "VisibilityTimeout": true, "MaxNumberOfMessages": true,
	"WaitTimeSeconds": true, "MaxResults": true, "MaxNumberOfMessagesPerSecond": true,
}

// pairMembers are the lists of name/value pairs the query protocol uses for the
// maps of the JSON form: member, JSON name, name of the key field.
var pairMembers = []struct{ query, json, key string }{
	{"Attribute", "Attributes", "Name"},
	{"Tag", "Tags", "Key"},
	{"MessageAttribute", "MessageAttributes", "Name"},
	{"MessageSystemAttribute", "MessageSystemAttributes", "Name"},
}

var listMembers = map[string]string{
	"AttributeName":                            "AttributeNames",
	"MessageSystemAttributeName":               "MessageSystemAttributeNames",
	"MessageAttributeName":                     "MessageAttributeNames",
	"TagKey":                                   "TagKeys",
	"SendMessageBatchRequestEntry":             "Entries",
	"DeleteMessageBatchRequestEntry":           "Entries",
	"ChangeMessageVisibilityBatchRequestEntry": "Entries",
}

// noResult lists the operations whose query response has no result element.
var noResult = map[string]bool{
	"DeleteQueue": true, "PurgeQueue": true, "SetQueueAttributes": true, "DeleteMessage": true,
	"ChangeMessageVisibility": true, "TagQueue": true, "UntagQueue": true,
}

func (s *Service) serveQuery(w http.ResponseWriter, r *http.Request) {
	requestID := w.Header().Get(awsapi.RequestIDHeader)
	if requestID == "" {
		requestID = awsapi.NewID()
	}
	action, out, err := s.runQuery(w, r)
	if err != nil {
		writeQueryError(w, requestID, err)
		return
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?>`)
	b.WriteString("<" + action + `Response xmlns="` + queryNamespace + `">`)
	if !noResult[action] {
		b.WriteString("<" + action + "Result>")
		writeFields(&b, action, out)
		b.WriteString("</" + action + "Result>")
	}
	b.WriteString("<ResponseMetadata><RequestId>" + requestID + "</RequestId></ResponseMetadata>")
	b.WriteString("</" + action + "Response>")
	w.Header().Set("Content-Type", xmlContentType)
	_, _ = io.WriteString(w, b.String())
}

// runQuery decodes the form, runs the operation and returns its JSON result.
func (s *Service) runQuery(w http.ResponseWriter, r *http.Request) (string, map[string]any, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	if err := r.ParseForm(); err != nil {
		return "", nil, errInvalidParameter("The request body could not be read or is too large.")
	}
	action := r.Form.Get("Action")
	handle, ok := operations[action]
	if !ok {
		return action, nil, &awsapi.Error{
			Status: http.StatusBadRequest, Code: "InvalidAction",
			Message: "stratus does not implement the SQS operation " + action + ".",
		}
	}
	params := formTree(r.Form)
	if _, has := params["QueueUrl"]; !has && strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 1 {
		params["QueueUrl"] = schemeOf(r) + "://" + r.Host + r.URL.Path
	}
	body, err := json.Marshal(rewrite(params))
	if err != nil {
		return action, nil, errInvalidParameter("The request parameters are malformed.")
	}
	result, err := handle(s, r, body)
	if err != nil {
		return action, nil, err
	}
	raw, _ := json.Marshal(result)
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	_ = dec.Decode(&out)
	return action, out, nil
}

func schemeOf(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// formTree turns "A.1.B=x" parameters into nested maps; Action and Version
// are the envelope, not members.
func formTree(form url.Values) map[string]any {
	root := map[string]any{}
	for key, values := range form {
		if key == "Action" || key == "Version" || len(values) == 0 || strings.Count(key, ".") > 8 {
			continue
		}
		node := root
		parts := strings.Split(key, ".")
		for _, part := range parts[:len(parts)-1] {
			child, ok := node[part].(map[string]any)
			if !ok {
				child = map[string]any{}
				node[part] = child
			}
			node = child
		}
		node[parts[len(parts)-1]] = values[0]
	}
	return root
}

// rewrite gives the tree the shape of the JSON request: lists instead of
// numbered maps, name/value pair lists as maps, numbers as numbers.
func rewrite(node map[string]any) map[string]any {
	for key, value := range node {
		node[key] = normalize(key, value)
	}
	for _, p := range pairMembers {
		if list, ok := node[p.query].([]any); ok {
			delete(node, p.query)
			node[p.json] = pairsToMap(list, p.key)
		}
	}
	for query, name := range listMembers {
		if list, ok := node[query]; ok {
			delete(node, query)
			node[name] = list
		}
	}
	return node
}

func normalize(key string, value any) any {
	switch v := value.(type) {
	case string:
		if n, err := strconv.Atoi(v); err == nil && intMembers[key] {
			return json.Number(strconv.Itoa(n))
		}
		return v
	case map[string]any:
		if list, ok := asList(v); ok {
			for i, item := range list {
				if m, ok := item.(map[string]any); ok {
					list[i] = rewrite(m)
				} else {
					list[i] = normalize(key, item)
				}
			}
			return list
		}
		return rewrite(v)
	}
	return value
}

// asList reads a map whose keys are all 1..n as the list a numbered query
// parameter stands for.
func asList(m map[string]any) ([]any, bool) {
	if len(m) == 0 {
		return nil, false
	}
	indexes := make([]int, 0, len(m))
	for key := range m {
		n, err := strconv.Atoi(key)
		if err != nil || n < 1 || n > maxFormParameter {
			return nil, false
		}
		indexes = append(indexes, n)
	}
	slices.Sort(indexes)
	list := make([]any, 0, len(indexes))
	for _, n := range indexes {
		list = append(list, m[strconv.Itoa(n)])
	}
	return list, true
}

func pairsToMap(list []any, keyField string) map[string]any {
	out := map[string]any{}
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			if key, ok := m[keyField].(string); ok {
				out[key] = m["Value"]
			}
		}
	}
	return out
}

func writeQueryError(w http.ResponseWriter, requestID string, err error) {
	var apiErr *awsapi.Error
	if !errors.As(err, &apiErr) {
		apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
	}
	code, fault := apiErr.Code, "Sender"
	if apiErr.QueryCode != "" {
		code = apiErr.QueryCode
	}
	if apiErr.Status >= http.StatusInternalServerError {
		fault = "Receiver"
	}
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0"?><ErrorResponse xmlns="` + queryNamespace + `"><Error><Type>` + fault +
		"</Type><Code>" + escape(code) + "</Code><Message>" + escape(apiErr.Message) + "</Message><Detail/></Error>" +
		"<RequestId>" + requestID + "</RequestId></ErrorResponse>")
	w.Header().Set("Content-Type", xmlContentType)
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write(b.Bytes())
}

func escape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
