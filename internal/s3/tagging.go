package s3

import (
	"encoding/xml"
	"maps"
	"net/http"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxBucketTags  = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

func isValidTagText(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == ' ' || strings.ContainsRune("_.:/=+-@", r) {
			continue
		}
		return false
	}
	return true
}

type taggingXML struct {
	XMLName xml.Name
	TagSet  tagSetXML `xml:"TagSet"`
}

type tagSetXML struct {
	Tags []tagXML `xml:"Tag"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// parseTagging reads and validates an S3 XML Tagging body against maxTags and
// S3 key/value constraints.
func parseTagging(r *http.Request, maxTags int) (map[string]string, error) {
	var doc taggingXML
	if err := readXML(r, &doc); err != nil {
		return nil, err
	}
	if len(doc.TagSet.Tags) > maxTags {
		return nil, errInvalidTag("Number of tags cannot exceed the maximum allowed.")
	}
	tags := make(map[string]string, len(doc.TagSet.Tags))
	for _, t := range doc.TagSet.Tags {
		if t.Key == "" || utf8.RuneCountInString(t.Key) > maxTagKeyLen || strings.HasPrefix(t.Key, "aws:") || !isValidTagText(t.Key) {
			return nil, errInvalidTag("The TagKey you have provided is invalid.")
		}
		if utf8.RuneCountInString(t.Value) > maxTagValueLen || !isValidTagText(t.Value) {
			return nil, errInvalidTag("The TagValue you have provided is invalid.")
		}
		if _, dup := tags[t.Key]; dup {
			return nil, errInvalidTag("Cannot provide multiple Tags with the same key.")
		}
		tags[t.Key] = t.Value
	}
	return tags, nil
}

func writeTagging(w http.ResponseWriter, tags map[string]string) {
	keys := slices.Sorted(maps.Keys(tags))
	out := taggingXML{
		XMLName: xmlName("Tagging"),
		TagSet:  tagSetXML{Tags: make([]tagXML, 0, len(keys))},
	}
	for _, k := range keys {
		out.TagSet.Tags = append(out.TagSet.Tags, tagXML{Key: k, Value: tags[k]})
	}
	writeXML(w, out)
}

func (s *Service) putBucketTagging(w http.ResponseWriter, r *http.Request, name string) error {
	tags, err := parseTagging(r, maxBucketTags)
	if err != nil {
		return err
	}
	if len(tags) == 0 {
		tags = nil
	}
	if err := s.updateBucket(name, func(b *bucket) error { b.tags = tags; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) getBucketTagging(w http.ResponseWriter, name string) error {
	tags, err := withBucket(s, name, func(b *bucket) map[string]string { return maps.Clone(b.tags) })
	if err != nil {
		return err
	}
	if tags == nil {
		return errNoSuchTagSet()
	}
	writeTagging(w, tags)
	return nil
}

func (s *Service) deleteBucketTagging(w http.ResponseWriter, name string) error {
	if err := s.updateBucket(name, func(b *bucket) error { b.tags = nil; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
