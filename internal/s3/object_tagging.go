package s3

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxObjectTags     = 10
	maxObjTagKeyLen   = 128
	maxObjTagValueLen = 256
)

type objectTaggingXML struct {
	XMLName xml.Name        `xml:"Tagging"`
	TagSet  objectTagSetXML `xml:"TagSet"`
}

type objectTagSetXML struct {
	Tags []objectTagXML `xml:"Tag"`
}

type objectTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

func isValidObjTagText(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == ' ' || strings.ContainsRune("_.:/=+-@", r) {
			continue
		}
		return false
	}
	return true
}

func validateObjectTags(tags map[string]string) error {
	if len(tags) > maxObjectTags {
		return errInvalidTag("Object tags cannot be greater than 10 tags")
	}
	for k, v := range tags {
		if k == "" || utf8.RuneCountInString(k) > maxObjTagKeyLen || strings.HasPrefix(k, "aws:") || !isValidObjTagText(k) {
			return errInvalidTag("The TagKey you have provided is invalid.")
		}
		if utf8.RuneCountInString(v) > maxObjTagValueLen || !isValidObjTagText(v) {
			return errInvalidTag("The TagValue you have provided is invalid.")
		}
	}
	return nil
}

func parseObjectTaggingXML(r *http.Request) (map[string]string, error) {
	var doc objectTaggingXML
	if err := readXML(r, &doc); err != nil {
		return nil, err
	}
	if len(doc.TagSet.Tags) > maxObjectTags {
		return nil, errInvalidTag("Object tags cannot be greater than 10 tags")
	}
	tags := make(map[string]string, len(doc.TagSet.Tags))
	for _, t := range doc.TagSet.Tags {
		if _, dup := tags[t.Key]; dup {
			return nil, errInvalidTag("Cannot provide multiple Tags with the same key.")
		}
		tags[t.Key] = t.Value
	}
	if err := validateObjectTags(tags); err != nil {
		return nil, err
	}
	return tags, nil
}

func parseTaggingHeader(raw string, maxTags int) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}
	tags := map[string]string{}
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			return nil, errInvalidArgument("The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.")
		}
		rawKey, rawVal, hasVal := strings.Cut(pair, "=")
		key, errK := url.QueryUnescape(rawKey)
		if errK != nil {
			return nil, errInvalidArgument("The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.")
		}
		var val string
		if hasVal {
			var errV error
			val, errV = url.QueryUnescape(rawVal)
			if errV != nil {
				return nil, errInvalidArgument("The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.")
			}
		}
		if _, dup := tags[key]; dup {
			return nil, errInvalidTag("Cannot provide multiple Tags with the same key.")
		}
		tags[key] = val
	}
	if len(tags) > maxTags {
		return nil, errInvalidTag("Object tags cannot be greater than 10 tags")
	}
	if err := validateObjectTags(tags); err != nil {
		return nil, err
	}
	return tags, nil
}

func resolveTagTarget(b *bucket, key, wireVersionID string) (*object, error) {
	var o *object
	if wireVersionID != "" {
		o = b.version(key, wireVersionID)
		if o == nil {
			return nil, errNoSuchVersion()
		}
		if o.deleteMarker {
			return nil, errMethodNotAllowed()
		}
		return o, nil
	}
	o = b.latest(key)
	if o == nil || o.deleteMarker {
		return nil, errNoSuchKey()
	}
	return o, nil
}

func (s *Service) getObjectTagging(w http.ResponseWriter, bucketName, key string, q url.Values) error {
	wireVersionID := q.Get("versionId")
	s.mu.RLock()
	b, err := s.bucketOf(bucketName)
	var o *object
	var tags map[string]string
	if err == nil {
		o, err = resolveTagTarget(b, key, wireVersionID)
	}
	if err == nil {
		tags = maps.Clone(b.objectTags[tagTarget{key: o.key, versionID: o.versionID}])
	}
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	setVersionHeader(w.Header(), o)
	keys := slices.Sorted(maps.Keys(tags))
	out := objectTaggingXML{
		XMLName: xmlName("Tagging"),
		TagSet:  objectTagSetXML{Tags: make([]objectTagXML, 0, len(keys))},
	}
	for _, k := range keys {
		out.TagSet.Tags = append(out.TagSet.Tags, objectTagXML{Key: k, Value: tags[k]})
	}
	writeXML(w, out)
	return nil
}

func (s *Service) putObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string, q url.Values) error {
	tags, err := parseObjectTaggingXML(r)
	if err != nil {
		return err
	}
	wireVersionID := q.Get("versionId")
	var target *object
	if err := s.updateBucket(bucketName, func(b *bucket) error {
		o, err := resolveTagTarget(b, key, wireVersionID)
		if err != nil {
			return err
		}
		keyTarget := tagTarget{key: o.key, versionID: o.versionID}
		if len(tags) == 0 {
			delete(b.objectTags, keyTarget)
		} else {
			b.objectTags[keyTarget] = tags
		}
		target = o
		return nil
	}); err != nil {
		return err
	}
	setVersionHeader(w.Header(), target)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) deleteObjectTagging(w http.ResponseWriter, bucketName, key string, q url.Values) error {
	wireVersionID := q.Get("versionId")
	var target *object
	if err := s.updateBucket(bucketName, func(b *bucket) error {
		o, err := resolveTagTarget(b, key, wireVersionID)
		if err != nil {
			return err
		}
		delete(b.objectTags, tagTarget{key: o.key, versionID: o.versionID})
		target = o
		return nil
	}); err != nil {
		return err
	}
	setVersionHeader(w.Header(), target)
	w.WriteHeader(http.StatusNoContent)
	return nil
}
