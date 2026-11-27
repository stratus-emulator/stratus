package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
)

// copySource is the object an x-amz-copy-source header names.
type copySource struct {
	bucket, key, versionID string
}

// parseCopySource reads "[/]<bucket>/<key>[?versionId=<id>]", URL-encoded.
func parseCopySource(value string) (copySource, error) {
	path, query, _ := strings.Cut(strings.TrimPrefix(value, "/"), "?")
	bad := errInvalidArgument("Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	unescaped, err := url.PathUnescape(path)
	if err != nil {
		return copySource{}, bad
	}
	bucketName, key, ok := strings.Cut(unescaped, "/")
	if !ok || bucketName == "" || key == "" {
		return copySource{}, bad
	}
	params, err := url.ParseQuery(query)
	if err != nil {
		return copySource{}, bad
	}
	return copySource{bucket: bucketName, key: key, versionID: params.Get("versionId")}, nil
}

// sourceObject resolves a copy source and checks its x-amz-copy-source-if-*
// preconditions, any failure of which is 412.
func (s *Service) sourceObject(r *http.Request) (*object, copySource, error) {
	src, err := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		return nil, src, err
	}
	o, err := s.lookup(src.bucket, src.key, src.versionID)
	if err != nil {
		return nil, src, err
	}
	if o.deleteMarker {
		if src.versionID != "" {
			return nil, src, errInvalidRequest("The source of a copy request may not specifically refer to a delete marker by version id.")
		}
		return nil, src, errNoSuchKey()
	}
	if copySourceConditionsOf(r.Header).evaluate(o) != 0 {
		return nil, src, errPreconditionFailed()
	}
	return o, src, nil
}

type copyObjectResult struct {
	XMLName      xml.Name
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
	checksumFields
}

// copyObject copies an object, sharing its immutable bytes. The metadata is
// the source's (COPY, the default) or the request's (REPLACE).
func (s *Service) copyObject(w http.ResponseWriter, r *http.Request, bucketName, key string) error {
	src, from, err := s.sourceObject(r)
	if err != nil {
		return err
	}
	directive := strings.ToUpper(r.Header.Get("x-amz-metadata-directive"))
	if directive != "" && directive != "COPY" && directive != "REPLACE" {
		return errInvalidArgument("Unknown metadata directive.")
	}
	tagDirective := strings.ToUpper(r.Header.Get("x-amz-tagging-directive"))
	if tagDirective != "" && tagDirective != "COPY" && tagDirective != "REPLACE" {
		return errInvalidArgument("Unknown tagging directive.")
	}
	var replaceTags map[string]string
	if tagDirective == "REPLACE" {
		if raw := r.Header.Get("x-amz-tagging"); raw != "" {
			replaceTags, err = parseTaggingHeader(raw, maxObjectTags)
			if err != nil {
				return err
			}
		}
	}
	algorithm := r.Header.Get("x-amz-checksum-algorithm")
	if from.bucket == bucketName && from.key == key && from.versionID == "" && directive != "REPLACE" && tagDirective != "REPLACE" && algorithm == "" {
		return errInvalidRequest("This copy request is illegal because it is trying to copy an object to itself " +
			"without changing the object's metadata, storage class, website redirect location or encryption attributes.")
	}
	obj, err := copiedObject(src, key, directive, algorithm, r.Header)
	if err != nil {
		return err
	}
	cond, err := writeConditionOf(r.Header)
	if err != nil {
		return err
	}
	stored, err := s.commitWithTags(bucketName, obj, cond, eventCopy, sourceOf(w, r),
		func(*bucket) error { return nil },
		func(dst *bucket) map[string]string {
			if tagDirective == "REPLACE" {
				return replaceTags
			}
			srcBucket := s.buckets[from.bucket]
			if srcBucket == nil {
				return nil
			}
			return srcBucket.objectTags[tagTarget{key: src.key, versionID: src.versionID}]
		})
	if err != nil {
		return err
	}
	setVersionHeader(w.Header(), stored)
	if src.versionID != "" {
		w.Header().Set("x-amz-copy-source-version-id", src.versionID)
	}
	c := stored.checksum
	writeXML(w, copyObjectResult{
		XMLName: xmlName("CopyObjectResult"), ETag: stored.etag, LastModified: xmlTime(stored.lastModified),
		checksumFields: checksumXML(c.algorithm, c.value, c.typ),
	})
	return nil
}

// copiedObject builds the copy of src. A multipart source becomes a single
// object, so its ETag is recomputed; a checksum algorithm in the request
// recomputes the checksum in that algorithm.
func copiedObject(src *object, key, directive, algorithmName string, h http.Header) (*object, error) {
	// The copy shares the source's blob: both are immutable.
	obj := &object{key: key, body: src.body, etag: src.etag, meta: src.meta, checksum: src.checksum}
	if directive == "REPLACE" {
		obj.meta = metaFromHeaders(h)
	}
	algorithm := ""
	if algorithmName != "" {
		var ok bool
		if algorithm, ok = normalizeAlgorithm(algorithmName); !ok {
			return nil, errInvalidRequest("Checksum algorithm " + algorithmName + " is not supported.")
		}
	}
	if src.partSizes == nil && algorithm == "" {
		return obj, nil
	}
	data, err := src.body.bytes()
	if err != nil {
		return nil, err
	}
	if src.partSizes != nil {
		obj.etag = etagOf(data)
		if src.checksum.typ == checksumComposite {
			obj.checksum = defaultChecksum(data)
		}
	}
	if algorithm != "" {
		obj.checksum = checksum{algorithm: algorithm, value: computeChecksum(algorithm, data), typ: checksumFullObject}
	}
	return obj, nil
}

type deleteRequest struct {
	XMLName xml.Name
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"Object"`
}

type deleteResultXML struct {
	XMLName xml.Name
	Deleted []deletedXML     `xml:"Deleted"`
	Errors  []deleteErrorXML `xml:"Error"`
}

type deletedXML struct {
	Key                   string `xml:"Key"`
	VersionID             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
}

type deleteErrorXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

// deleteObjects deletes up to 1000 keys or versions; Quiet mode reports
// only the failures.
func (s *Service) deleteObjects(w http.ResponseWriter, r *http.Request, bucketName string) error {
	var req deleteRequest
	if err := readXML(r, &req); err != nil {
		return err
	}
	if len(req.Objects) == 0 || len(req.Objects) > defaultMaxKeys {
		return errMalformedXML()
	}
	out := deleteResultXML{XMLName: xmlName("DeleteResult")}
	src := sourceOf(w, r)
	for _, o := range req.Objects {
		res, err := s.deleteObject(bucketName, o.Key, o.VersionID, src)
		if err != nil {
			if errorCode(err) == "NoSuchBucket" {
				return err
			}
			out.Errors = append(out.Errors, deleteErrorXML{Key: o.Key, VersionID: o.VersionID, Code: errorCode(err), Message: err.Error()})
			continue
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedEntry(o.Key, o.VersionID, res))
		}
	}
	writeXML(w, out)
	return nil
}

func deletedEntry(key, versionID string, res deleteResult) deletedXML {
	d := deletedXML{Key: key, VersionID: versionID, DeleteMarker: res.deleteMarker}
	if res.deleteMarker {
		d.DeleteMarkerVersionID = res.versionID
	}
	return d
}
