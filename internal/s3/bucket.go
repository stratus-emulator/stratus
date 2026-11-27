package s3

import (
	"slices"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// Versioning states. A bucket that was never versioned has the empty state.
const (
	versioningEnabled   = "Enabled"
	versioningSuspended = "Suspended"
)

type tagTarget struct {
	key       string
	versionID string
}

type bucket struct {
	name          string
	region        string
	createdAt     time.Time
	versioning    string
	lifecycle     []LifecycleRule // nil: no lifecycle configuration
	notifications notificationConfiguration
	// objects holds every version of every key, oldest first; the last one is
	// the latest, which may be a delete marker.
	objects    map[string][]*object
	objectTags map[tagTarget]map[string]string
	uploads    map[string]*upload
}

// object is one version of a key. Nothing in it changes once it is stored, so
// readers may use it, and its bytes, after releasing the lock.
type object struct {
	key          string
	versionID    string // empty: the null version
	deleteMarker bool
	body         *blob  // nil only for a delete marker
	etag         string // quoted
	lastModified time.Time
	meta         objectMeta
	checksum     checksum
	partSizes    []int64 // the part sizes of a multipart object
}

// objectMeta is the metadata a client sets on an object.
type objectMeta struct {
	contentType        string
	contentEncoding    string
	contentDisposition string
	contentLanguage    string
	cacheControl       string
	expires            string
	user               map[string]string // lower-cased names without x-amz-meta-
}

func newBucket(name, region string, now time.Time) *bucket {
	return &bucket{
		name:       name,
		region:     region,
		createdAt:  now,
		objects:    map[string][]*object{},
		objectTags: map[tagTarget]map[string]string{},
		uploads:    map[string]*upload{},
	}
}

func (o *object) size() int64 {
	if o.body == nil {
		return 0
	}
	return o.body.size
}

// wireVersionID is the version id as S3 shows it: "null" for the null version.
func (o *object) wireVersionID() string {
	if o.versionID == "" {
		return "null"
	}
	return o.versionID
}

// latest returns the newest version of key, possibly a delete marker, or nil.
func (b *bucket) latest(key string) *object {
	versions := b.objects[key]
	if len(versions) == 0 {
		return nil
	}
	return versions[len(versions)-1]
}

// current returns the object a plain GET of key sees, or nil.
func (b *bucket) current(key string) *object {
	if o := b.latest(key); o != nil && !o.deleteMarker {
		return o
	}
	return nil
}

// version returns the version of key with a wire version id ("null" is the
// null version), or nil.
func (b *bucket) version(key, wireID string) *object {
	id := internalVersionID(wireID)
	for _, o := range b.objects[key] {
		if o.versionID == id {
			return o
		}
	}
	return nil
}

func internalVersionID(wireID string) string {
	if wireID == "null" {
		return ""
	}
	return wireID
}

// newVersionID is the version id a new write gets: a fresh one when
// versioning is enabled, the null version otherwise.
func (b *bucket) newVersionID() string {
	if b.versioning != versioningEnabled {
		return ""
	}
	return strings.ReplaceAll(awsapi.NewID(), "-", "")
}

// add makes o the latest version of its key. A null-version write replaces
// the existing null version, as in an unversioned or suspended bucket.
func (b *bucket) add(o *object) {
	versions := b.objects[o.key]
	if o.versionID == "" {
		versions = slices.DeleteFunc(slices.Clone(versions), func(v *object) bool { return v.versionID == "" })
		delete(b.objectTags, tagTarget{key: o.key, versionID: ""})
	}
	b.objects[o.key] = append(versions, o)
}

// removeVersion deletes one version of key and returns it, or nil.
func (b *bucket) removeVersion(key, wireID string) *object {
	id := internalVersionID(wireID)
	versions := b.objects[key]
	i := slices.IndexFunc(versions, func(v *object) bool { return v.versionID == id })
	if i < 0 {
		return nil
	}
	removed := versions[i]
	delete(b.objectTags, tagTarget{key: key, versionID: id})
	b.setVersions(key, slices.Delete(slices.Clone(versions), i, i+1))
	return removed
}

func (b *bucket) setVersions(key string, versions []*object) {
	if len(versions) == 0 {
		for _, v := range b.objects[key] {
			delete(b.objectTags, tagTarget{key: key, versionID: v.versionID})
		}
		delete(b.objects, key)
		return
	}
	for _, old := range b.objects[key] {
		if !slices.Contains(versions, old) {
			delete(b.objectTags, tagTarget{key: key, versionID: old.versionID})
		}
	}
	b.objects[key] = versions
}

// deleteKey is a DeleteObject without a version id. An unversioned bucket
// drops the key; a versioned one gets a delete marker, which it returns.
func (b *bucket) deleteKey(key string, now time.Time) *object {
	if b.versioning == "" {
		delete(b.objects, key)
		delete(b.objectTags, tagTarget{key: key, versionID: ""})
		return nil
	}
	marker := &object{key: key, versionID: b.newVersionID(), deleteMarker: true, lastModified: now}
	b.add(marker)
	return marker
}

// sortedKeys returns the keys that have any version, in UTF-8 byte order.
func (b *bucket) sortedKeys() []string {
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// isEmpty tells whether the bucket holds no version of any key.
func (b *bucket) isEmpty() bool { return len(b.objects) == 0 }
