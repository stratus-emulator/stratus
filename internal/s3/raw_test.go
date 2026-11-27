package s3_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/s3"
)

// placeholderAuth is the unsigned Authorization header a hand-written client
// sends; only its credential scope matters, to route the request.
const placeholderAuth = "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/s3/aws4_request, SignedHeaders=host, Signature=e2e"

// raw sends a request with the placeholder Authorization header and returns
// the status, headers and body.
func raw(t *testing.T, e *env, method, path, body string, headers map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", placeholderAuth)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp := do(t, req)
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(data)
}

func TestRawPutGetCopy(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "raw", s3.BucketOptions{})
	status, h, _ := raw(t, e, http.MethodPut, "/raw/a%20dir//x%2B..%2Fy.txt", "raw body",
		map[string]string{"Content-Type": "text/plain", "x-amz-meta-origin": "raw"})
	if status != http.StatusOK || h.Get("ETag") == "" {
		t.Fatalf("PUT: %d %v", status, h)
	}
	status, h, body := raw(t, e, http.MethodGet, "/raw/a%20dir//x%2B..%2Fy.txt", "", nil)
	if status != http.StatusOK || body != "raw body" || h.Get("Content-Type") != "text/plain" || h.Get("x-amz-meta-origin") != "raw" {
		t.Fatalf("GET: %d %q %v", status, body, h)
	}
	status, _, body = raw(t, e, http.MethodPut, "/raw/copy", "", map[string]string{"x-amz-copy-source": "/raw/a%20dir//x%2B..%2Fy.txt"})
	if status != http.StatusOK || !strings.Contains(body, "<CopyObjectResult") {
		t.Fatalf("copy PUT: %d %s", status, body)
	}
	if status, _, body = raw(t, e, http.MethodGet, "/raw/copy", "", nil); status != http.StatusOK || body != "raw body" {
		t.Fatalf("GET copy: %d %q", status, body)
	}
	_, _, body = raw(t, e, http.MethodGet, "/raw?list-type=2&encoding-type=url&prefix=a%20", "", nil)
	if !strings.Contains(body, "<Key>a+dir//x%2B../y.txt</Key>") || !strings.Contains(body, "<EncodingType>url</EncodingType>") {
		t.Fatalf("url-encoded listing: %s", body)
	}
}

func TestRawErrorShapes(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "raw", s3.BucketOptions{})
	status, h, body := raw(t, e, http.MethodGet, "/raw/missing", "", nil)
	for _, want := range []string{"<Error>", "<Code>NoSuchKey</Code>", "<Message>", "<Resource>/raw/missing</Resource>", "<RequestId>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("error body lacks %s: %s", want, body)
		}
	}
	if status != http.StatusNotFound || h.Get("x-amz-request-id") == "" {
		t.Fatalf("status %d, headers %v", status, h)
	}
	if status, _, body := raw(t, e, http.MethodHead, "/raw/missing", "", nil); status != http.StatusNotFound || body != "" {
		t.Fatalf("HEAD: %d %q", status, body)
	}
	if status, _, body := raw(t, e, http.MethodPatch, "/raw/k", "", nil); status != http.StatusMethodNotAllowed ||
		!strings.Contains(body, "MethodNotAllowed") {
		t.Fatalf("PATCH: %d %s", status, body)
	}
	if status, _, body := raw(t, e, http.MethodPut, "/raw/k", "abc", map[string]string{"Content-MD5": "XrY7u+Ae7tCTyyK7j1rNww=="}); status != http.StatusBadRequest ||
		!strings.Contains(body, "BadDigest") {
		t.Fatalf("Content-MD5 mismatch: %d %s", status, body)
	}
	if status, _, body := raw(t, e, http.MethodGet, "/raw/k?acl", "", nil); status != http.StatusNotImplemented ||
		!strings.Contains(body, "GetObjectAcl") {
		t.Fatalf("acl: %d %s", status, body)
	}
}

func TestRawConditionalGet(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "raw", s3.BucketOptions{})
	_, h, _ := raw(t, e, http.MethodPut, "/raw/k", "data", nil)
	etag := h.Get("ETag")
	_, h, _ = raw(t, e, http.MethodHead, "/raw/k", "", nil)
	modified := h.Get("Last-Modified")
	tests := []struct {
		headers map[string]string
		want    int
	}{
		{map[string]string{"If-None-Match": etag}, http.StatusNotModified},
		{map[string]string{"If-None-Match": `"other"`}, http.StatusOK},
		{map[string]string{"If-Match": `"other"`}, http.StatusPreconditionFailed},
		{map[string]string{"If-Match": etag}, http.StatusOK},
		{map[string]string{"If-Modified-Since": modified}, http.StatusNotModified},
		{map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"}, http.StatusPreconditionFailed},
		{map[string]string{"If-Match": etag, "If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"}, http.StatusOK},
	}
	for _, tt := range tests {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			if status, _, _ := raw(t, e, method, "/raw/k", "", tt.headers); status != tt.want {
				t.Errorf("%s %v: %d, want %d", method, tt.headers, status, tt.want)
			}
		}
	}
}

// chunk frames one aws-chunked chunk; signed adds a (never verified) signature.
func chunk(data string, signed bool) string {
	ext := ""
	if signed {
		ext = ";chunk-signature=" + strings.Repeat("0", 64)
	}
	return strconv.FormatInt(int64(len(data)), 16) + ext + "\r\n" + data + "\r\n"
}

func TestRawAWSChunkedBodies(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "raw", s3.BucketOptions{})
	const crc32OfHelloWorld = "DUoRhQ==" // CRC32 of "hello world"
	tests := []struct {
		name, sha, body, trailer, wantCode string
	}{
		{name: "signed", sha: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			body: chunk("hello ", true) + chunk("world", true) + chunk("", true) + "\r\n"},
		{name: "signed trailer", sha: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", trailer: "x-amz-checksum-crc32",
			body: chunk("hello world", true) + chunk("", true) + "x-amz-checksum-crc32:" + crc32OfHelloWorld +
				"\r\nx-amz-trailer-signature:" + strings.Repeat("1", 64) + "\r\n\r\n"},
		{name: "unsigned trailer", sha: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", trailer: "x-amz-checksum-crc32",
			body: chunk("hello", false) + chunk(" world", false) + "0\r\nx-amz-checksum-crc32:" + crc32OfHelloWorld + "\r\n\r\n"},
		{name: "wrong trailing checksum", sha: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", trailer: "x-amz-checksum-crc32",
			body: chunk("hello world", false) + "0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n", wantCode: "BadDigest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := map[string]string{
				"x-amz-content-sha256": tt.sha, "Content-Encoding": "aws-chunked",
				"x-amz-decoded-content-length": "11",
			}
			if tt.trailer != "" {
				headers["x-amz-trailer"] = tt.trailer
			}
			status, _, body := raw(t, e, http.MethodPut, "/raw/"+tt.name, tt.body, headers)
			if tt.wantCode != "" {
				if status != http.StatusBadRequest || !strings.Contains(body, tt.wantCode) {
					t.Fatalf("status %d body %s, want %s", status, body, tt.wantCode)
				}
				return
			}
			if status != http.StatusOK {
				t.Fatalf("PUT: %d %s", status, body)
			}
			status, h, got := raw(t, e, http.MethodGet, "/raw/"+strings.ReplaceAll(tt.name, " ", "%20"), "", nil)
			if status != http.StatusOK || got != "hello world" || h.Get("Content-Encoding") != "" {
				t.Fatalf("GET: %d %q encoding %q", status, got, h.Get("Content-Encoding"))
			}
		})
	}
}

// Racing create-if-absent writes: the condition is checked under the lock
// that stores the object, so exactly one wins.
func TestRawConcurrentCreateIfAbsent(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "raw", s3.BucketOptions{})
	const writers = 16
	statuses := make(chan int, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, e.srv.URL+"/raw/lock", strings.NewReader(strconv.Itoa(i)))
			req.Header.Set("Authorization", placeholderAuth)
			req.Header.Set("If-None-Match", "*")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				statuses <- 0
				return
			}
			_ = resp.Body.Close()
			statuses <- resp.StatusCode
		})
	}
	wg.Wait()
	close(statuses)
	won := 0
	for status := range statuses {
		switch status {
		case http.StatusOK:
			won++
		case http.StatusPreconditionFailed:
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if won != 1 {
		t.Fatalf("%d writers won, want 1", won)
	}
}
