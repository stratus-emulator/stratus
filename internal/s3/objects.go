package s3

import (
	"maps"
	"net/http"
	"strings"
	"time"
)

// writeCondition is the precondition of a conditional write: If-None-Match: *
// creates only an absent key, If-Match: <etag> overwrites only that version.
type writeCondition struct {
	ifMatch     string
	ifNoneMatch string
}

func writeConditionOf(h http.Header) (writeCondition, error) {
	c := writeCondition{ifMatch: h.Get("If-Match"), ifNoneMatch: h.Get("If-None-Match")}
	if c.ifNoneMatch != "" && c.ifNoneMatch != "*" {
		return writeCondition{}, errNotImplemented("conditional write with If-None-Match other than *")
	}
	return c, nil
}

// check evaluates the condition against the key's current object, under the
// same lock as the write it guards, so that two racing creates cannot both win.
func (c writeCondition) check(current *object) error {
	if c.ifNoneMatch == "*" && current != nil {
		return errPreconditionFailed()
	}
	if c.ifMatch == "" {
		return nil
	}
	if current == nil {
		return errNoSuchKey()
	}
	if !etagMatches(c.ifMatch, current.etag) {
		return errPreconditionFailed()
	}
	return nil
}

// etagMatches compares an If-Match / If-None-Match list with an ETag; "*"
// matches any, and quotes and weak prefixes are ignored.
func etagMatches(list, etag string) bool {
	want := strings.Trim(etag, `"`)
	for _, candidate := range strings.Split(list, ",") {
		candidate = strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
		if candidate == "*" || strings.Trim(candidate, `"`) == want {
			return true
		}
	}
	return false
}

// eventSource is what a notification says about the request behind an event.
type eventSource struct {
	requestID string
	sourceIP  string
}

// store makes obj, which the caller built and nobody else references, the
// latest version of its key, after checking the write condition. It sends
// the bucket's notifications for event once the lock is released.
func (s *Service) store(bucketName string, obj *object, cond writeCondition, event string, src eventSource) (*object, error) {
	return s.commit(bucketName, obj, cond, event, src, func(*bucket) error { return nil })
}

// commit is store with a claim: a last check, made under the lock after the
// write condition, that may also update the bucket (completing an upload
// removes it). The write happens only when the claim succeeds.
func (s *Service) commit(bucketName string, obj *object, cond writeCondition, event string, src eventSource,
	claim func(*bucket) error,
) (*object, error) {
	return s.commitWithTags(bucketName, obj, cond, event, src, claim, nil)
}

func (s *Service) commitWithTags(bucketName string, obj *object, cond writeCondition, event string, src eventSource,
	claim func(*bucket) error, resolveTags func(*bucket) map[string]string,
) (*object, error) {
	s.mu.Lock()
	b, err := s.bucketOf(bucketName)
	if err == nil {
		err = cond.check(b.current(obj.key))
	}
	if err == nil {
		err = claim(b)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var tags map[string]string
	if resolveTags != nil {
		tags = resolveTags(b)
	}
	obj.versionID = b.newVersionID()
	obj.lastModified = s.clock.Now()
	b.add(obj)
	if len(tags) > 0 {
		b.objectTags[tagTarget{key: obj.key, versionID: obj.versionID}] = maps.Clone(tags)
	}
	deliveries := s.eventsLocked(b, obj, event, src)
	s.mu.Unlock()

	s.deliver(deliveries)
	return obj, nil
}

// lookup returns the version a GET or HEAD addresses: the given version, or
// the latest one. The result may be a delete marker; a missing key or
// version is an error.
func (s *Service) lookup(bucketName, key, wireVersionID string) (*object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(bucketName)
	if err != nil {
		return nil, err
	}
	if wireVersionID != "" {
		if o := b.version(key, wireVersionID); o != nil {
			return o, nil
		}
		return nil, errNoSuchVersion()
	}
	if o := b.latest(key); o != nil {
		return o, nil
	}
	return nil, errNoSuchKey()
}

// deleteResult describes what a DeleteObject did.
type deleteResult struct {
	versionID    string // wire form; empty when no version was involved
	deleteMarker bool   // a delete marker was created, or removed
}

// deleteObject deletes a key, or exactly one of its versions. Deleting
// something that does not exist succeeds, as in S3.
func (s *Service) deleteObject(bucketName, key, wireVersionID string, src eventSource) (deleteResult, error) {
	s.mu.Lock()
	b, err := s.bucketOf(bucketName)
	if err != nil {
		s.mu.Unlock()
		return deleteResult{}, err
	}
	res, deliveries := s.deleteLocked(b, key, wireVersionID, s.clock.Now(), src)
	s.mu.Unlock()

	s.deliver(deliveries)
	return res, nil
}

func (s *Service) deleteLocked(b *bucket, key, wireVersionID string, now time.Time, src eventSource) (deleteResult, []delivery) {
	if wireVersionID != "" {
		removed := b.removeVersion(key, wireVersionID)
		if removed == nil {
			return deleteResult{versionID: wireVersionID}, nil
		}
		return deleteResult{versionID: wireVersionID, deleteMarker: removed.deleteMarker},
			s.eventsLocked(b, removed, eventDelete, src)
	}
	existed := b.latest(key) != nil
	marker := b.deleteKey(key, now)
	if marker != nil {
		return deleteResult{versionID: marker.wireVersionID(), deleteMarker: true},
			s.eventsLocked(b, marker, eventDeleteMarker, src)
	}
	if !existed {
		return deleteResult{}, nil
	}
	return deleteResult{}, s.eventsLocked(b, &object{key: key}, eventDelete, src)
}
