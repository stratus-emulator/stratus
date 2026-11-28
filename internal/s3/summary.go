package s3

import (
	"io"
	"maps"
	"slices"
	"strings"
	"time"
)

// BucketSummary is what an overview of the emulator shows of a bucket.
type BucketSummary struct {
	Name       string
	Objects    int   // keys whose latest version is not a delete marker
	Bytes      int64 // the size of those latest versions
	Versions   int   // every version, delete markers included
	Uploads    int   // multipart uploads in progress
	Versioning string
}

// Buckets summarises every bucket, by name.
func (s *Service) Buckets() []BucketSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BucketSummary, 0, len(s.buckets))
	for _, b := range s.buckets {
		sum := BucketSummary{Name: b.name, Uploads: len(b.uploads), Versioning: b.versioning}
		for key, versions := range b.objects {
			sum.Versions += len(versions)
			if o := b.current(key); o != nil {
				sum.Objects++
				sum.Bytes += o.size()
			}
		}
		out = append(out, sum)
	}
	slices.SortFunc(out, func(a, b BucketSummary) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// ObjectView is an object as a look at its bucket shows it: the version a
// plain GET of its key sees.
type ObjectView struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	VersionID    string // empty: the null version
}

func viewOf(o *object) ObjectView {
	return ObjectView{Key: o.key, Size: o.size(), LastModified: o.lastModified, ETag: o.etag,
		ContentType: o.meta.contentType, VersionID: o.versionID}
}

// BucketInfo is a bucket's configuration.
type BucketInfo struct {
	Created       time.Time
	Region        string
	Versioning    string // empty: never enabled
	Lifecycle     []LifecycleRule
	Notifications []NotificationView
	Tags          map[string]string
}

// NotificationView is one queue a bucket tells about object events.
type NotificationView struct {
	QueueARN       string
	Events         []string
	Prefix, Suffix string
}

// Bucket describes one bucket.
func (s *Service) Bucket(name string) (BucketInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(name)
	if err != nil {
		return BucketInfo{}, err
	}
	info := BucketInfo{
		Created:    b.createdAt,
		Region:     b.region,
		Versioning: b.versioning,
		Lifecycle:  slices.Clone(b.lifecycle),
		Tags:       maps.Clone(b.tags),
	}
	for _, q := range b.notifications.Queues {
		n := NotificationView{QueueARN: q.Queue, Events: slices.Clone(q.Events)}
		if q.Filter != nil {
			for _, r := range q.Filter.Rules {
				switch strings.ToLower(r.Name) {
				case "prefix":
					n.Prefix = r.Value
				case "suffix":
					n.Suffix = r.Value
				}
			}
		}
		info.Notifications = append(info.Notifications, n)
	}
	return info, nil
}

// ListObjectViews returns up to max current objects whose key starts with
// prefix and contains contains, in key order, and whether more match.
func (s *Service) ListObjectViews(bucketName, prefix, contains string, max int) ([]ObjectView, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(bucketName)
	if err != nil {
		return nil, false, err
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) && strings.Contains(k, contains) && b.current(k) != nil {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	more := len(keys) > max
	out := make([]ObjectView, 0, min(len(keys), max))
	for _, k := range keys[:min(len(keys), max)] {
		out = append(out, viewOf(b.current(k)))
	}
	return out, more, nil
}

// currentObject is the version a plain GET of key sees. Its bytes may be
// read after the lock is released: an object never changes once stored.
func (s *Service) currentObject(bucketName, key string) (*object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(bucketName)
	if err != nil {
		return nil, err
	}
	o := b.current(key)
	if o == nil {
		return nil, errNoSuchKey()
	}
	return o, nil
}

// ObjectHead returns the first max bytes of a key's current version, its view
// and whether those bytes are all of it.
func (s *Service) ObjectHead(bucketName, key string, max int64) ([]byte, ObjectView, bool, error) {
	o, err := s.currentObject(bucketName, key)
	if err != nil {
		return nil, ObjectView{}, false, err
	}
	n := min(max, o.size())
	data, err := o.body.read(0, n)
	return data, viewOf(o), n == o.size(), err
}

// WriteObject writes all of a key's current version to w and returns its view.
// The view is returned before any byte is written, through before, so that a
// caller can set headers from it.
func (s *Service) WriteObject(w io.Writer, bucketName, key string, before func(ObjectView)) error {
	o, err := s.currentObject(bucketName, key)
	if err != nil {
		return err
	}
	before(viewOf(o))
	_, err = o.body.copyTo(w, 0, o.size())
	return err
}

// EmptyBucket removes every object version and multipart upload of a bucket
// and returns how many keys it held. Unlike deleting objects one by one it
// sends no notifications: it is a way to start over, not traffic.
func (s *Service) EmptyBucket(name string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucketOf(name)
	if err != nil {
		return 0, err
	}
	n := len(b.objects)
	b.objects = map[string][]*object{}
	b.uploads = map[string]*upload{}
	return n, nil
}
