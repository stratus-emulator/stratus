package s3

import (
	"encoding/xml"
	"strings"
	"time"
)

const day = 24 * time.Hour

// lifecycleConfiguration is the wire form of a lifecycle configuration.
type lifecycleConfiguration struct {
	XMLName xml.Name
	Rules   []lifecycleRuleXML `xml:"Rule"`
}

type lifecycleRuleXML struct {
	ID                             string              `xml:"ID,omitempty"`
	Prefix                         *string             `xml:"Prefix"` // the legacy, filter-less form
	Filter                         *lifecycleFilterXML `xml:"Filter"`
	Status                         string              `xml:"Status"`
	Expiration                     *expirationXML      `xml:"Expiration"`
	NoncurrentVersionExpiration    *noncurrentXML      `xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload *abortXML           `xml:"AbortIncompleteMultipartUpload"`
	Transitions                    []struct{}          `xml:"Transition"`
	NoncurrentVersionTransitions   []struct{}          `xml:"NoncurrentVersionTransition"`
}

type lifecycleFilterXML struct {
	Prefix                *string          `xml:"Prefix"`
	Tag                   *struct{}        `xml:"Tag"`
	And                   *lifecycleAndXML `xml:"And"`
	ObjectSizeGreaterThan int64            `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    int64            `xml:"ObjectSizeLessThan,omitempty"`
}

type lifecycleAndXML struct {
	Prefix                string     `xml:"Prefix,omitempty"`
	Tags                  []struct{} `xml:"Tag"`
	ObjectSizeGreaterThan int64      `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    int64      `xml:"ObjectSizeLessThan,omitempty"`
}

type expirationXML struct {
	Days                      int    `xml:"Days,omitempty"`
	Date                      string `xml:"Date,omitempty"`
	ExpiredObjectDeleteMarker bool   `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

type noncurrentXML struct {
	NoncurrentDays          int `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions int `xml:"NewerNoncurrentVersions,omitempty"`
}

type abortXML struct {
	DaysAfterInitiation int `xml:"DaysAfterInitiation"`
}

// rulesFromXML converts a wire configuration into rules. Transitions, tag
// filters and NewerNoncurrentVersions are refused rather than silently lost.
func rulesFromXML(cfg lifecycleConfiguration) ([]LifecycleRule, error) {
	if len(cfg.Rules) == 0 {
		return nil, errMalformedXML()
	}
	rules := make([]LifecycleRule, 0, len(cfg.Rules))
	for _, x := range cfg.Rules {
		if len(x.Transitions)+len(x.NoncurrentVersionTransitions) > 0 {
			return nil, errNotImplemented("PutBucketLifecycleConfiguration with storage class transitions")
		}
		r, err := ruleFromXML(x)
		if err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, validateLifecycle(rules)
}

func ruleFromXML(x lifecycleRuleXML) (LifecycleRule, error) {
	if x.Status != "Enabled" && x.Status != "Disabled" {
		return LifecycleRule{}, errMalformedXML()
	}
	r := LifecycleRule{ID: x.ID, Disabled: x.Status == "Disabled"}
	if x.Prefix != nil {
		r.Prefix = *x.Prefix
	}
	if f := x.Filter; f != nil {
		if f.Tag != nil || (f.And != nil && len(f.And.Tags) > 0) {
			return LifecycleRule{}, errNotImplemented("PutBucketLifecycleConfiguration with tag filters")
		}
		if f.Prefix != nil {
			r.Prefix = *f.Prefix
		}
		r.ObjectSizeGreaterThan, r.ObjectSizeLessThan = f.ObjectSizeGreaterThan, f.ObjectSizeLessThan
		if f.And != nil {
			r.Prefix = f.And.Prefix
			r.ObjectSizeGreaterThan, r.ObjectSizeLessThan = f.And.ObjectSizeGreaterThan, f.And.ObjectSizeLessThan
		}
	}
	if e := x.Expiration; e != nil {
		r.ExpirationDays, r.ExpiredObjectDeleteMarker = e.Days, e.ExpiredObjectDeleteMarker
		if e.Date != "" {
			date, err := time.Parse(time.RFC3339, e.Date)
			if err != nil {
				return LifecycleRule{}, errInvalidArgument("The lifecycle expiration date is not an ISO 8601 date.")
			}
			r.ExpirationDate = date
		}
	}
	if n := x.NoncurrentVersionExpiration; n != nil {
		if n.NewerNoncurrentVersions > 0 {
			return LifecycleRule{}, errNotImplemented("PutBucketLifecycleConfiguration with NewerNoncurrentVersions")
		}
		r.NoncurrentDays = n.NoncurrentDays
	}
	if a := x.AbortIncompleteMultipartUpload; a != nil {
		r.AbortIncompleteUploadDays = a.DaysAfterInitiation
	}
	return r, nil
}

func validateLifecycle(rules []LifecycleRule) error {
	for _, r := range rules {
		if r.ExpirationDays < 0 || r.NoncurrentDays < 0 || r.AbortIncompleteUploadDays < 0 {
			return errInvalidArgument("Lifecycle day counts must be positive.")
		}
		if r.ExpirationDays > 0 && !r.ExpirationDate.IsZero() {
			return errMalformedXML()
		}
	}
	return nil
}

// rulesToXML is the wire form a GET returns.
func rulesToXML(rules []LifecycleRule) lifecycleConfiguration {
	var cfg lifecycleConfiguration
	for _, r := range rules {
		x := lifecycleRuleXML{ID: r.ID, Status: "Enabled", Filter: &lifecycleFilterXML{}}
		if r.Disabled {
			x.Status = "Disabled"
		}
		if r.ObjectSizeGreaterThan > 0 || r.ObjectSizeLessThan > 0 {
			x.Filter.And = &lifecycleAndXML{Prefix: r.Prefix,
				ObjectSizeGreaterThan: r.ObjectSizeGreaterThan, ObjectSizeLessThan: r.ObjectSizeLessThan}
		} else {
			x.Filter.Prefix = &r.Prefix
		}
		if r.ExpirationDays > 0 || !r.ExpirationDate.IsZero() || r.ExpiredObjectDeleteMarker {
			x.Expiration = &expirationXML{Days: r.ExpirationDays, ExpiredObjectDeleteMarker: r.ExpiredObjectDeleteMarker}
			if !r.ExpirationDate.IsZero() {
				x.Expiration.Date = r.ExpirationDate.UTC().Format(timeFormatXML)
			}
		}
		if r.NoncurrentDays > 0 {
			x.NoncurrentVersionExpiration = &noncurrentXML{NoncurrentDays: r.NoncurrentDays}
		}
		if r.AbortIncompleteUploadDays > 0 {
			x.AbortIncompleteMultipartUpload = &abortXML{DaysAfterInitiation: r.AbortIncompleteUploadDays}
		}
		cfg.Rules = append(cfg.Rules, x)
	}
	return cfg
}

// ExpireLifecycle applies the Expiration, NoncurrentVersionExpiration and
// AbortIncompleteMultipartUpload rules of every bucket at the injected
// clock's time, and returns how many current objects it expired plus how
// many versions, delete markers and uploads it removed. Nothing expires in
// the background: a test or a host calls this when it wants time to act.
func (s *Service) ExpireLifecycle() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	n := 0
	for _, b := range s.buckets {
		for _, r := range b.lifecycle {
			if !r.Disabled {
				n += b.applyRule(r, now)
			}
		}
	}
	return n
}

func (b *bucket) applyRule(r LifecycleRule, now time.Time) int {
	n := 0
	for _, key := range b.sortedKeys() {
		if strings.HasPrefix(key, r.Prefix) {
			n += b.expireKey(r, key, now)
		}
	}
	for id, u := range b.uploads {
		if r.AbortIncompleteUploadDays > 0 && strings.HasPrefix(u.key, r.Prefix) &&
			!now.Before(expiryOf(u.initiated, r.AbortIncompleteUploadDays)) {
			delete(b.uploads, id)
			n++
		}
	}
	return n
}

func (b *bucket) expireKey(r LifecycleRule, key string, now time.Time) int {
	n := 0
	if cur := b.current(key); cur != nil && r.sizeMatches(cur.size()) && r.currentExpired(cur.lastModified, now) {
		b.deleteKey(key, now)
		n++
	}
	if r.NoncurrentDays > 0 {
		versions := b.objects[key]
		kept := make([]*object, 0, len(versions))
		for i, v := range versions {
			// A version became noncurrent when its successor was written.
			if i < len(versions)-1 && r.sizeMatches(v.size()) &&
				!now.Before(expiryOf(versions[i+1].lastModified, r.NoncurrentDays)) {
				n++
				continue
			}
			kept = append(kept, v)
		}
		b.setVersions(key, kept)
	}
	if versions := b.objects[key]; r.ExpiredObjectDeleteMarker && len(versions) == 1 && versions[0].deleteMarker {
		b.setVersions(key, nil)
		n++
	}
	return n
}

func (r LifecycleRule) sizeMatches(size int64) bool {
	return (r.ObjectSizeGreaterThan == 0 || size > r.ObjectSizeGreaterThan) &&
		(r.ObjectSizeLessThan == 0 || size < r.ObjectSizeLessThan)
}

func (r LifecycleRule) currentExpired(created, now time.Time) bool {
	if !r.ExpirationDate.IsZero() && !now.Before(r.ExpirationDate) {
		return true
	}
	return r.ExpirationDays > 0 && !now.Before(expiryOf(created, r.ExpirationDays))
}

// expiryOf is start plus days, rounded up to the next midnight UTC, which is
// how S3 schedules lifecycle actions.
func expiryOf(start time.Time, days int) time.Time {
	t := start.UTC().Add(time.Duration(days) * day)
	midnight := t.Truncate(day)
	if midnight.Equal(t) {
		return t
	}
	return midnight.Add(day)
}
