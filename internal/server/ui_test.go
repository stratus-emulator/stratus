package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stratus-emulator/stratus/internal/manifest"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

const uiManifest = `
sqs:
  queues:
    - name: jobs
kinesis:
  streams:
    - name: events
s3:
  buckets:
    - name: files
dynamodb:
  tables:
    - name: locks
      partitionKey: {name: id}
      ttlAttribute: expires_at
stepFunctions:
  stateMachines:
    - name: hello
      definition: '{"StartAt": "Done", "States": {"Done": {"Type": "Pass", "End": true}}}'
`

func TestUIMachineDetailShowsExecutions(t *testing.T) {
	srv, ts := newUIServer(t)
	if _, err := srv.targets.SFN.StartExecution(srv.targets.SFN.StateMachineARN("hello"), "first-run", `{"greeting": "hi"}`); err != nil {
		t.Fatalf("StartExecution: %v", err)
	}
	code, page := get(t, ts.URL+"/_stratus/ui/states/hello")
	for _, want := range []string{"first-run", "greeting", "&#34;StartAt&#34;"} {
		if code != http.StatusOK || !strings.Contains(page, want) {
			t.Errorf("machine detail = %d, without %q", code, want)
		}
	}
}

func newUIServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	m, err := manifest.Parse([]byte(uiManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	srv, err := New(Config{Manifest: m, DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func uiPost(t *testing.T, target string, form url.Values, htmx bool) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if htmx {
		req.Header.Set("HX-Request", "true")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", target, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestUIShowsEveryResource(t *testing.T) {
	_, ts := newUIServer(t)
	code, page := get(t, ts.URL+"/_stratus/ui")
	if code != http.StatusOK {
		t.Fatalf("GET /_stratus/ui = %d", code)
	}
	for _, want := range []string{"jobs", "events", "files", "locks", "expires_at", "/_stratus/ui/htmx.min.js"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page does not show %q", want)
		}
	}
	if code, js := get(t, ts.URL+"/_stratus/ui/htmx.min.js"); code != http.StatusOK || !strings.Contains(js, "htmx") {
		t.Errorf("GET htmx.min.js = %d", code)
	}
	if code, _ := get(t, ts.URL+"/_stratus/ui/resources"); code != http.StatusOK {
		t.Errorf("GET resources = %d", code)
	}
}

// Without the header htmx sends, a post is refused: a form on another site
// must not be able to purge or reset the emulator.
func TestUIActionsNeedHTMX(t *testing.T) {
	_, ts := newUIServer(t)
	for _, path := range []string{"/_stratus/ui/reset", "/_stratus/ui/expire-ttl", "/_stratus/ui/sqs/purge", "/_stratus/ui/s3/empty"} {
		if code, _ := uiPost(t, ts.URL+path, url.Values{"name": {"jobs"}}, false); code != http.StatusForbidden {
			t.Errorf("POST %s without HX-Request = %d, want 403", path, code)
		}
	}
}

func TestUIPurgesAQueueAndEmptiesABucket(t *testing.T) {
	srv, ts := newUIServer(t)
	if _, err := srv.targets.SQS.Send("jobs", sqs.SendInput{Body: "hello"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := srv.targets.S3.PutObject("files", "a.txt", []byte("x"), s3.ObjectOptions{}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if q := srv.queueSummaries(); len(q) != 1 || q[0].Visible != 1 {
		t.Fatalf("queue summary = %+v, want jobs with 1 visible", q)
	}

	code, body := uiPost(t, ts.URL+"/_stratus/ui/sqs/purge", url.Values{"name": {"jobs"}}, true)
	if code != http.StatusOK || !strings.Contains(body, "Purged queue jobs.") {
		t.Fatalf("purge = %d, body without the notice", code)
	}
	if q := srv.queueSummaries(); q[0].Visible != 0 {
		t.Fatalf("after purge the queue has %d visible messages", q[0].Visible)
	}

	code, body = uiPost(t, ts.URL+"/_stratus/ui/s3/empty", url.Values{"name": {"files"}}, true)
	if code != http.StatusOK || !strings.Contains(body, "1 keys removed") {
		t.Fatalf("empty = %d, body without the notice", code)
	}
	if b := srv.targets.S3.Buckets(); len(b) != 1 || b[0].Objects != 0 {
		t.Fatalf("after empty the bucket summary is %+v", b)
	}

	code, body = uiPost(t, ts.URL+"/_stratus/ui/s3/empty", url.Values{"name": {"missing"}}, true)
	if code != http.StatusOK || !strings.Contains(body, "Failed:") {
		t.Fatalf("emptying a missing bucket = %d, want the failure shown", code)
	}
}

// A look at a queue shows its messages and leaves them as they were.
func TestUIQueueDetailPeeksWithoutReceiving(t *testing.T) {
	srv, ts := newUIServer(t)
	if _, err := srv.targets.SQS.Send("jobs", sqs.SendInput{Body: `{"order": 42}`}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	code, page := get(t, ts.URL+"/_stratus/ui/sqs/jobs")
	if code != http.StatusOK || !strings.Contains(page, "&#34;order&#34;: 42") || !strings.Contains(page, "visible") {
		t.Fatalf("queue detail = %d, without the message", code)
	}
	if q := srv.queueSummaries(); q[0].Visible != 1 || q[0].InFlight != 0 {
		t.Fatalf("after a look the queue is %+v, want the message still visible", q[0])
	}
}

func TestUIDetailsOfEveryKind(t *testing.T) {
	srv, ts := newUIServer(t)
	if err := srv.targets.S3.PutObject("files", "dir/report.txt", []byte("hello from s3"), s3.ObjectOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	for path, want := range map[string]string{
		"/_stratus/ui/kinesis/events":                       "No records.",
		"/_stratus/ui/dynamodb/locks":                       "The table is empty.",
		"/_stratus/ui/s3/files":                             "dir/report.txt",
		"/_stratus/ui/s3/files?prefix=other/":               "No objects match.",
		"/_stratus/ui/s3/files/object?key=dir%2Freport.txt": "hello from s3",
	} {
		code, page := get(t, ts.URL+path)
		if code != http.StatusOK || !strings.Contains(page, want) {
			t.Errorf("GET %s = %d, without %q", path, code, want)
		}
	}
	if code, _ := get(t, ts.URL+"/_stratus/ui/sqs/missing"); code != http.StatusNotFound {
		t.Errorf("a missing queue = %d, want 404", code)
	}
}

func TestUIDetailPollReturnsOnlyTheBody(t *testing.T) {
	_, ts := newUIServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/_stratus/ui/sqs/jobs", nil)
	req.Header.Set("HX-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "<html") || !strings.Contains(string(body), "The queue is empty.") {
		t.Fatalf("poll answered with the whole page or without the body: %.200s", body)
	}
}

// A downloaded object is a file to save, never a page shown in the UI.
func TestUIDownloadIsAnAttachment(t *testing.T) {
	srv, ts := newUIServer(t)
	if err := srv.targets.S3.PutObject("files", "a/evil.html", []byte("<script>alert(1)</script>"), s3.ObjectOptions{ContentType: "text/html"}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	resp, err := http.Get(ts.URL + "/_stratus/ui/s3/files/download?key=a%2Fevil.html")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	h := resp.Header
	if h.Get("Content-Type") != "application/octet-stream" || h.Get("X-Content-Type-Options") != "nosniff" ||
		h.Get("Content-Disposition") != `attachment; filename=evil.html` || string(body) != "<script>alert(1)</script>" {
		t.Fatalf("download headers %v, body %q", h, body)
	}
	if code, _ := get(t, ts.URL+"/_stratus/ui/s3/files/download?key=missing"); code != http.StatusNotFound {
		t.Errorf("downloading a missing key = %d, want 404", code)
	}
}

func TestFormatBytes(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1288490188: "1.2 GiB"} {
		if got := formatBytes(n); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestUIObjectListAndDetailShowTags(t *testing.T) {
	_, ts := newUIServer(t)
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/files/tagged.txt", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-tagging", "owner=alice&env=prod")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutObject = %d, want 200", resp.StatusCode)
	}
	code, listPage := get(t, ts.URL+"/_stratus/ui/s3/files")
	if code != http.StatusOK || !strings.Contains(listPage, "<th>Tags</th>") ||
		!strings.Contains(listPage, "<div><code>env</code> = <code>prod</code></div><div><code>owner</code> = <code>alice</code></div>") {
		t.Fatalf("bucket object list = %d, without object tags", code)
	}
	code, objPage := get(t, ts.URL+"/_stratus/ui/s3/files/object?key=tagged.txt")
	if code != http.StatusOK ||
		!strings.Contains(objPage, "<dt>Tags</dt><dd><div><code>env</code> = <code>prod</code></div><div><code>owner</code> = <code>alice</code></div></dd>") {
		t.Fatalf("object detail = %d, without object tags", code)
	}
}
