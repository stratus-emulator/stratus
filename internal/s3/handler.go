package s3

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// maxPresignExpiry is the longest validity a presigned URL may have: 7 days.
const maxPresignExpiry = 7 * 24 * time.Hour

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", w.Header().Get(awsapi.RequestIDHeader))
	if err := s.serve(w, r); err != nil {
		writeError(w, r, err)
	}
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) error {
	if err := s.checkPresignedExpiry(r.URL.Query()); err != nil {
		return err
	}
	bucketName, key, err := address(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	switch {
	case bucketName == "" && r.Method == http.MethodGet:
		return s.listBuckets(w, q)
	case bucketName == "":
		return errMethodNotAllowed()
	case key == "":
		return s.routeBucket(w, r, bucketName, q)
	default:
		return s.routeObject(w, r, bucketName, key, q)
	}
}

// address splits a request into bucket and key. The key is the raw escaped
// path, unescaped once and never cleaned, so "a//b", "../x" and " " survive.
// A virtual-hosted request (<bucket>.s3.<domain>, <bucket>.localhost) names
// the bucket in the host and the key in the whole path.
func address(r *http.Request) (string, string, error) {
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	bucketPart, keyPart := "", path
	if b := virtualHostBucket(r.Host); b != "" {
		bucketPart = b
	} else {
		bucketPart, keyPart, _ = strings.Cut(path, "/")
	}
	bucketName, err := url.PathUnescape(bucketPart)
	if err != nil {
		return "", "", errInvalidBucketName()
	}
	key, err := url.PathUnescape(keyPart)
	if err != nil {
		return "", "", errInvalidArgument("The object key is not a valid URL path.")
	}
	return bucketName, key, nil
}

func virtualHostBucket(hostport string) string {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	if b, _, ok := strings.Cut(host, ".s3."); ok && b != "" && !strings.Contains(b, ":") {
		return b
	}
	if b, ok := strings.CutSuffix(host, ".localhost"); ok && b != "" && b != "s3" {
		return b
	}
	return ""
}

// checkPresignedExpiry enforces X-Amz-Expires of a presigned URL against the
// injected clock. The signature itself is never checked.
func (s *Service) checkPresignedExpiry(q url.Values) error {
	if !q.Has("X-Amz-Expires") {
		return nil
	}
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	if err != nil {
		return errAccessDenied("X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\"")
	}
	seconds, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || seconds < 1 || time.Duration(seconds)*time.Second > maxPresignExpiry {
		return errAccessDenied("X-Amz-Expires must be between 1 and 604800 seconds")
	}
	if s.clock.Now().After(signed.Add(time.Duration(seconds) * time.Second)) {
		return errAccessDenied("Request has expired")
	}
	return nil
}

func (s *Service) routeBucket(w http.ResponseWriter, r *http.Request, name string, q url.Values) error {
	if op := unsupportedOperation(r.Method, q, bucketSubresources); op != "" {
		return errNotImplemented(op)
	}
	switch r.Method {
	case http.MethodGet:
		return s.routeBucketGet(w, r, name, q)
	case http.MethodPut:
		switch {
		case q.Has("versioning"):
			return s.putBucketVersioning(w, r, name)
		case q.Has("lifecycle"):
			return s.putBucketLifecycle(w, r, name)
		case q.Has("notification"):
			return s.putBucketNotification(w, r, name)
		}
		return s.createBucket(w, r, name)
	case http.MethodHead:
		return s.headBucket(w, name)
	case http.MethodDelete:
		if q.Has("lifecycle") {
			return s.deleteBucketLifecycle(w, name)
		}
		return s.deleteBucket(w, name)
	case http.MethodPost:
		if q.Has("delete") {
			return s.deleteObjects(w, r, name)
		}
	}
	return errMethodNotAllowed()
}

func (s *Service) routeBucketGet(w http.ResponseWriter, r *http.Request, name string, q url.Values) error {
	switch {
	case q.Has("location"):
		return s.getBucketLocation(w, name)
	case q.Has("versioning"):
		return s.getBucketVersioning(w, name)
	case q.Has("lifecycle"):
		return s.getBucketLifecycle(w, name)
	case q.Has("notification"):
		return s.getBucketNotification(w, name)
	case q.Has("uploads"):
		return s.listMultipartUploads(w, name, q)
	case q.Has("versions"):
		return s.listObjectVersions(w, name, q)
	case q.Get("list-type") == "2":
		return s.listObjectsV2(w, name, q)
	}
	return s.listObjectsV1(w, name, q)
}

func (s *Service) routeObject(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	if op := unsupportedOperation(r.Method, q, objectSubresources); op != "" {
		return errNotImplemented(op)
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if r.Method == http.MethodGet {
			switch {
			case q.Has("uploadId"):
				return s.listParts(w, bucketName, key, q)
			case q.Has("tagging"):
				return s.getObjectTagging(w, bucketName, key, q)
			}
		}
		return s.getObject(w, r, bucketName, key, q)
	case http.MethodPut:
		switch {
		case q.Has("tagging"):
			return s.putObjectTagging(w, r, bucketName, key, q)
		case q.Has("uploadId") && r.Header.Get("x-amz-copy-source") != "":
			return s.uploadPartCopy(w, r, bucketName, key, q)
		case q.Has("uploadId"):
			return s.uploadPart(w, r, bucketName, key, q)
		case r.Header.Get("x-amz-copy-source") != "":
			return s.copyObject(w, r, bucketName, key)
		}
		return s.putObject(w, r, bucketName, key)
	case http.MethodDelete:
		switch {
		case q.Has("tagging"):
			return s.deleteObjectTagging(w, bucketName, key, q)
		case q.Has("uploadId"):
			return s.abortMultipartUpload(w, bucketName, key, q)
		}
		return s.deleteObjectHTTP(w, r, bucketName, key, q)
	case http.MethodPost:
		switch {
		case q.Has("uploads"):
			return s.createMultipartUpload(w, r, bucketName, key)
		case q.Has("uploadId"):
			return s.completeMultipartUpload(w, r, bucketName, key, q)
		}
	}
	return errMethodNotAllowed()
}

// bucketSubresources and objectSubresources name the operations S3 has and
// this emulator does not, by the query parameter that selects them; a POST
// entry is the full operation name, the others are prefixed with the verb.
var (
	bucketSubresources = map[string]string{
		"acl": "BucketAcl", "policy": "BucketPolicy", "policyStatus": "BucketPolicyStatus", "cors": "BucketCors",
		"tagging": "BucketTagging", "encryption": "BucketEncryption", "website": "BucketWebsite",
		"logging": "BucketLogging", "ownershipControls": "BucketOwnershipControls",
		"publicAccessBlock": "PublicAccessBlock", "object-lock": "ObjectLockConfiguration",
		"replication": "BucketReplication", "accelerate": "BucketAccelerateConfiguration",
		"requestPayment": "BucketRequestPayment", "analytics": "BucketAnalyticsConfiguration",
		"inventory": "BucketInventoryConfiguration", "metrics": "BucketMetricsConfiguration",
		"intelligent-tiering": "BucketIntelligentTieringConfiguration",
	}
	objectSubresources = map[string]string{
		"acl": "ObjectAcl", "retention": "ObjectRetention",
		"legal-hold": "ObjectLegalHold", "attributes": "ObjectAttributes", "torrent": "ObjectTorrent",
		"restore": "RestoreObject", "select": "SelectObjectContent", "renameObject": "RenameObject",
	}
	verbs = map[string]string{http.MethodGet: "Get", http.MethodHead: "Get", http.MethodPut: "Put", http.MethodDelete: "Delete"}
)

// unsupportedOperation names the operation a request asks for when it is one
// this emulator does not implement, or returns "".
func unsupportedOperation(method string, q url.Values, subresources map[string]string) string {
	for param, op := range subresources {
		if q.Has(param) {
			return verbs[method] + op
		}
	}
	return ""
}

// sourceOf is what an event notification says about a request.
func sourceOf(w http.ResponseWriter, r *http.Request) eventSource {
	ip := r.RemoteAddr
	if h, _, err := net.SplitHostPort(ip); err == nil {
		ip = h
	}
	return eventSource{requestID: w.Header().Get(awsapi.RequestIDHeader), sourceIP: ip}
}
