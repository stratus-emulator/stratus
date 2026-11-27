package s3

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// putObject stores the request body. The bucket is checked before the body
// is read, so that a missing bucket fails without receiving gigabytes; the
// conditional write is checked again under the lock that stores it.
func (s *Service) putObject(w http.ResponseWriter, r *http.Request, bucketName, key string) error {
	cond, err := writeConditionOf(r.Header)
	if err != nil {
		return err
	}
	if _, err := withBucket(s, bucketName, func(*bucket) bool { return true }); err != nil {
		return err
	}
	var tags map[string]string
	if rawTags := r.Header.Get("x-amz-tagging"); rawTags != "" {
		var err error
		tags, err = parseTaggingHeader(rawTags, maxObjectTags)
		if err != nil {
			return err
		}
	}
	p, err := readPayload(r, maxObjectSize)
	if err != nil {
		return err
	}
	sum := p.checksum
	if sum.algorithm == "" {
		sum = defaultChecksum(p.data)
	}
	body, err := s.blobs.put(p.data)
	if err != nil {
		return err
	}
	obj := &object{key: key, body: body, etag: etagOf(p.data), meta: metaFromHeaders(r.Header), checksum: sum}
	stored, err := s.commitWithTags(bucketName, obj, cond, eventPut, sourceOf(w, r),
		func(*bucket) error { return nil },
		func(*bucket) map[string]string { return tags })
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("ETag", stored.etag)
	setVersionHeader(h, stored)
	setChecksumHeaders(h, stored.checksum)
	w.WriteHeader(http.StatusOK)
	return nil
}

// metaFromHeaders reads the metadata a client sets on an object.
func metaFromHeaders(h http.Header) objectMeta {
	m := objectMeta{
		contentType:        h.Get("Content-Type"),
		contentEncoding:    storedContentEncoding(h),
		contentDisposition: h.Get("Content-Disposition"),
		contentLanguage:    h.Get("Content-Language"),
		cacheControl:       h.Get("Cache-Control"),
		expires:            h.Get("Expires"),
	}
	for name, values := range h {
		if k, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-"); ok && len(values) > 0 {
			if m.user == nil {
				m.user = map[string]string{}
			}
			m.user[k] = values[0]
		}
	}
	return m
}

func setVersionHeader(h http.Header, o *object) {
	if o.versionID != "" {
		h.Set("x-amz-version-id", o.versionID)
	}
}

func setChecksumHeaders(h http.Header, c checksum) {
	if c.algorithm == "" {
		return
	}
	h.Set(checksumHeader(c.algorithm), c.value)
	h.Set("x-amz-checksum-type", c.typ)
}

// getObject serves GET and HEAD: versions, delete markers, preconditions,
// a Range or a partNumber, response-* overrides and checksums. The body is
// written straight from the stored slice.
func (s *Service) getObject(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	o, err := s.lookup(bucketName, key, q.Get("versionId"))
	if err != nil {
		return err
	}
	h := w.Header()
	if o.deleteMarker {
		h.Set("x-amz-delete-marker", "true")
		setVersionHeader(h, o)
		if q.Get("versionId") != "" {
			return errMethodNotAllowed()
		}
		return errNoSuchKey()
	}
	switch readConditionsOf(r.Header).evaluate(o) {
	case http.StatusPreconditionFailed:
		return errPreconditionFailed()
	case http.StatusNotModified:
		h.Set("ETag", o.etag)
		h.Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	rng, partial, err := selectRange(r, q, o)
	if err != nil {
		return err
	}
	writeObjectHeaders(h, o, r.Header.Get("x-amz-checksum-mode") == "ENABLED" && !partial)
	if tagCount, _ := withBucket(s, bucketName, func(b *bucket) int {
		return len(b.objectTags[tagTarget{key: o.key, versionID: o.versionID}])
	}); tagCount > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(tagCount))
	}
	applyOverrides(h, q)
	off, n, status := int64(0), o.size(), http.StatusOK
	if partial {
		off, n, status = rng.start, rng.end+1-rng.start, http.StatusPartialContent
		h.Set("Content-Range", rng.contentRange(o.size()))
	}
	h.Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		// Past the header nothing can be reported: a client that went away is
		// not an error of ours, and a read error can only cut the body short.
		_, _ = o.body.copyTo(w, off, n)
	}
	return nil
}

// selectRange picks the bytes to serve: those of a part when partNumber is
// given, else those of the Range header, else all of them (partial=false).
func selectRange(r *http.Request, q url.Values, o *object) (byteRange, bool, error) {
	if !q.Has("partNumber") {
		return parseRange(r.Header.Get("Range"), o.size())
	}
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > maxPartNum {
		return byteRange{}, false, errInvalidArgument("Part number must be an integer between 1 and 10000, inclusive")
	}
	if o.partSizes == nil {
		if n != 1 {
			return byteRange{}, false, errInvalidPartNumber()
		}
		return byteRange{}, false, nil
	}
	if n > len(o.partSizes) {
		return byteRange{}, false, errInvalidPartNumber()
	}
	var start int64
	for _, size := range o.partSizes[:n-1] {
		start += size
	}
	return byteRange{start: start, end: start + o.partSizes[n-1] - 1}, true, nil
}

func writeObjectHeaders(h http.Header, o *object, withChecksum bool) {
	contentType := o.meta.contentType
	if contentType == "" {
		contentType = "binary/octet-stream"
	}
	h.Set("Content-Type", contentType)
	h.Set("ETag", o.etag)
	h.Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	for name, value := range map[string]string{
		"Content-Encoding":    o.meta.contentEncoding,
		"Content-Disposition": o.meta.contentDisposition,
		"Content-Language":    o.meta.contentLanguage,
		"Cache-Control":       o.meta.cacheControl,
		"Expires":             o.meta.expires,
	} {
		if value != "" {
			h.Set(name, value)
		}
	}
	for k, v := range o.meta.user {
		h["X-Amz-Meta-"+k] = []string{v}
	}
	setVersionHeader(h, o)
	if o.partSizes != nil {
		h.Set("x-amz-mp-parts-count", strconv.Itoa(len(o.partSizes)))
	}
	if withChecksum {
		setChecksumHeaders(h, o.checksum)
	}
}

// applyOverrides applies the response-* query parameters a (typically
// presigned) GET may carry.
func applyOverrides(h http.Header, q url.Values) {
	for param, header := range map[string]string{
		"response-content-type":        "Content-Type",
		"response-content-disposition": "Content-Disposition",
		"response-content-encoding":    "Content-Encoding",
		"response-content-language":    "Content-Language",
		"response-cache-control":       "Cache-Control",
		"response-expires":             "Expires",
	} {
		if v := q.Get(param); v != "" {
			h.Set(header, v)
		}
	}
}

func (s *Service) deleteObjectHTTP(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	res, err := s.deleteObject(bucketName, key, q.Get("versionId"), sourceOf(w, r))
	if err != nil {
		return err
	}
	if res.deleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if res.versionID != "" {
		w.Header().Set("x-amz-version-id", res.versionID)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
