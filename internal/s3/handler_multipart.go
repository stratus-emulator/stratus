package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
)

type initiateMultipartUploadResult struct {
	XMLName  xml.Name
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

func (s *Service) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, key string) error {
	algorithm, typ, err := newUploadChecksum(r.Header.Get("x-amz-checksum-algorithm"), r.Header.Get("x-amz-checksum-type"))
	if err != nil {
		return err
	}
	var tags map[string]string
	if rawTags := r.Header.Get("x-amz-tagging"); rawTags != "" {
		tags, err = parseTaggingHeader(rawTags, maxObjectTags)
		if err != nil {
			return err
		}
	}
	u := &upload{key: key, meta: metaFromHeaders(r.Header), tags: tags, algorithm: algorithm, checksumType: typ}
	if err := s.createUpload(bucketName, u); err != nil {
		return err
	}
	if algorithm != "" {
		w.Header().Set("x-amz-checksum-algorithm", algorithm)
		w.Header().Set("x-amz-checksum-type", typ)
	}
	writeXML(w, initiateMultipartUploadResult{
		XMLName: xmlName("InitiateMultipartUploadResult"), Bucket: bucketName, Key: key, UploadID: u.id,
	})
	return nil
}

func partNumberOf(q url.Values) (int, error) {
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > maxPartNum {
		return 0, errInvalidArgument("Part number must be an integer between 1 and 10000, inclusive")
	}
	return n, nil
}

// uploadPart stores a part. The upload is checked before the body is read
// and again, under the lock, when the part is stored.
func (s *Service) uploadPart(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	n, err := partNumberOf(q)
	if err != nil {
		return err
	}
	u, err := s.findUpload(bucketName, key, q.Get("uploadId"))
	if err != nil {
		return err
	}
	p, err := readPayload(r, maxObjectSize)
	if err != nil {
		return err
	}
	sum, err := partChecksum(u, p.checksum, p.data)
	if err != nil {
		return err
	}
	body, err := s.blobs.put(p.data)
	if err != nil {
		return err
	}
	stored := &part{number: n, body: body, etag: etagOf(p.data), checksum: sum}
	if err := s.putPart(bucketName, key, u.id, stored); err != nil {
		return err
	}
	w.Header().Set("ETag", stored.etag)
	if sum != "" {
		w.Header().Set(checksumHeader(u.algorithm), sum)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

type copyPartResult struct {
	XMLName      xml.Name
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
	checksumFields
}

// uploadPartCopy makes a part of an existing object, or of a byte range of
// it given by x-amz-copy-source-range.
func (s *Service) uploadPartCopy(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	n, err := partNumberOf(q)
	if err != nil {
		return err
	}
	u, err := s.findUpload(bucketName, key, q.Get("uploadId"))
	if err != nil {
		return err
	}
	src, _, err := s.sourceObject(r)
	if err != nil {
		return err
	}
	off, size := int64(0), src.size()
	if v := r.Header.Get("x-amz-copy-source-range"); v != "" {
		rng, ok, err := parseRange(v, src.size())
		if err != nil || !ok {
			return errInvalidArgument("The x-amz-copy-source-range value must be of the form bytes=first-last " +
				"where first and last are the zero-based offsets of the first and last bytes to copy")
		}
		off, size = rng.start, rng.end+1-rng.start
	}
	data, err := src.body.read(off, size)
	if err != nil {
		return err
	}
	sum, err := partChecksum(u, checksum{}, data)
	if err != nil {
		return err
	}
	body, err := s.blobs.put(data)
	if err != nil {
		return err
	}
	stored := &part{number: n, body: body, etag: etagOf(data), checksum: sum}
	if err := s.putPart(bucketName, key, u.id, stored); err != nil {
		return err
	}
	writeXML(w, copyPartResult{
		XMLName: xmlName("CopyPartResult"), ETag: stored.etag, LastModified: xmlTime(stored.lastModified),
		checksumFields: checksumXML(u.algorithm, sum, ""),
	})
	return nil
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
	checksumFields
}

// completeMultipartUpload validates the part list, assembles the object
// outside the lock, then stores it and drops the upload in one step, after
// the conditional-write check.
func (s *Service) completeMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	cond, err := writeConditionOf(r.Header)
	if err != nil {
		return err
	}
	objectSize, err := mpObjectSize(r.Header)
	if err != nil {
		return err
	}
	var req completeMultipartUpload
	if err := readXML(r, &req); err != nil {
		return err
	}
	uploadID := q.Get("uploadId")
	u, parts, err := s.selectParts(bucketName, key, uploadID, req.Parts)
	if err != nil {
		return err
	}
	sent, err := checksumFromHeaders(r.Header, nil)
	if err != nil {
		return err
	}
	obj, err := s.assemble(u, parts, objectSize, sent)
	if err != nil {
		return err
	}
	stored, err := s.commitWithTags(bucketName, obj, cond, eventComplete, sourceOf(w, r), func(b *bucket) error {
		if b.uploads[uploadID] != u {
			return errNoSuchUpload()
		}
		delete(b.uploads, uploadID)
		return nil
	}, func(*bucket) map[string]string { return u.tags })
	if err != nil {
		return err
	}
	setVersionHeader(w.Header(), stored)
	c := stored.checksum
	writeXML(w, completeMultipartUploadResult{
		XMLName: xmlName("CompleteMultipartUploadResult"), Location: "/" + bucketName + "/" + key,
		Bucket: bucketName, Key: key, ETag: stored.etag, checksumFields: checksumXML(c.algorithm, c.value, c.typ),
	})
	return nil
}

// mpObjectSize reads x-amz-mp-object-size, or -1 when it is absent.
func mpObjectSize(h http.Header) (int64, error) {
	v := h.Get("x-amz-mp-object-size")
	if v == "" {
		return -1, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, errInvalidRequest("The x-amz-mp-object-size header is not a valid size.")
	}
	return n, nil
}

func (s *Service) abortMultipartUpload(w http.ResponseWriter, bucketName, key string, q url.Values) error {
	if err := s.abortUpload(bucketName, key, q.Get("uploadId")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
