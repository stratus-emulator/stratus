package s3

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sort"
	"time"
)

// snapshotFormat is the version of what Snapshot writes. Restore reads only
// this one: a state written by another version is refused, never half read.
const snapshotFormat = 1

// A snapshot holds the metadata; the object bytes stay in the files of the
// data directory, which a snapshot names. Restore must be given a service on
// the same directory.
type snapshot struct {
	Format   int              `json:"format"`
	Sequence uint64           `json:"sequence"`
	Buckets  []bucketSnapshot `json:"buckets"`
}

type bucketSnapshot struct {
	Name          string                    `json:"name"`
	Region        string                    `json:"region"`
	CreatedAt     time.Time                 `json:"createdAt"`
	Versioning    string                    `json:"versioning,omitempty"`
	Lifecycle     []LifecycleRule           `json:"lifecycle,omitempty"`
	Notifications notificationConfiguration `json:"notifications"`
	Objects       []objectSnapshot          `json:"objects"` // every version of every key, oldest first per key
	Uploads       []uploadSnapshot          `json:"uploads,omitempty"`
}

type blobRef struct {
	File string `json:"file,omitempty"` // empty: the empty blob
	Size int64  `json:"size"`
}

type metaSnapshot struct {
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	Expires            string            `json:"expires,omitempty"`
	User               map[string]string `json:"user,omitempty"`
}

type checksumSnapshot struct {
	Algorithm string `json:"algorithm,omitempty"`
	Value     string `json:"value,omitempty"`
	Type      string `json:"type,omitempty"`
}

type objectSnapshot struct {
	Key          string            `json:"key"`
	VersionID    string            `json:"versionId,omitempty"`
	DeleteMarker bool              `json:"deleteMarker,omitempty"`
	Body         *blobRef          `json:"body,omitempty"` // nil for a delete marker
	ETag         string            `json:"etag,omitempty"`
	LastModified time.Time         `json:"lastModified"`
	Meta         metaSnapshot      `json:"meta"`
	Checksum     checksumSnapshot  `json:"checksum"`
	PartSizes    []int64           `json:"partSizes,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

type uploadSnapshot struct {
	ID           string            `json:"id"`
	Key          string            `json:"key"`
	Initiated    time.Time         `json:"initiated"`
	Meta         metaSnapshot      `json:"meta"`
	Tags         map[string]string `json:"tags,omitempty"`
	Algorithm    string            `json:"algorithm,omitempty"`
	ChecksumType string            `json:"checksumType,omitempty"`
	Parts        []partSnapshot    `json:"parts,omitempty"`
}

type partSnapshot struct {
	Number       int       `json:"number"`
	Body         blobRef   `json:"body"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"lastModified"`
	Checksum     string    `json:"checksum,omitempty"`
}

// Snapshot returns every bucket with the metadata of its objects and uploads.
// keep says which resources, by name, to include; nil includes every one.
func (s *Service) Snapshot(keep func(name string) bool) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := snapshot{Format: snapshotFormat, Sequence: s.sequence, Buckets: make([]bucketSnapshot, 0, len(s.buckets))}
	for name, b := range s.buckets {
		if keep != nil && !keep(name) {
			continue
		}
		snap.Buckets = append(snap.Buckets, snapshotBucket(b))
	}
	sort.Slice(snap.Buckets, func(i, j int) bool { return snap.Buckets[i].Name < snap.Buckets[j].Name })
	return json.Marshal(snap)
}

func snapshotBucket(b *bucket) bucketSnapshot {
	bs := bucketSnapshot{
		Name: b.name, Region: b.region, CreatedAt: b.createdAt, Versioning: b.versioning,
		Lifecycle: b.lifecycle, Notifications: b.notifications,
	}
	for _, key := range b.sortedKeys() {
		for _, o := range b.objects[key] {
			ob := objectSnapshot{
				Key: o.key, VersionID: o.versionID, DeleteMarker: o.deleteMarker, ETag: o.etag,
				LastModified: o.lastModified, Meta: snapshotMeta(o.meta), PartSizes: o.partSizes,
				Checksum: checksumSnapshot{Algorithm: o.checksum.algorithm, Value: o.checksum.value, Type: o.checksum.typ},
				Tags:     maps.Clone(b.objectTags[tagTarget{key: o.key, versionID: o.versionID}]),
			}
			if o.body != nil {
				ob.Body = &blobRef{File: o.body.name(), Size: o.body.size}
			}
			bs.Objects = append(bs.Objects, ob)
		}
	}
	for _, u := range b.uploads {
		us := uploadSnapshot{ID: u.id, Key: u.key, Initiated: u.initiated, Meta: snapshotMeta(u.meta),
			Tags: maps.Clone(u.tags), Algorithm: u.algorithm, ChecksumType: u.checksumType}
		for _, p := range u.parts {
			us.Parts = append(us.Parts, partSnapshot{Number: p.number, Body: blobRef{File: p.body.name(), Size: p.body.size},
				ETag: p.etag, LastModified: p.lastModified, Checksum: p.checksum})
		}
		sort.Slice(us.Parts, func(i, j int) bool { return us.Parts[i].Number < us.Parts[j].Number })
		bs.Uploads = append(bs.Uploads, us)
	}
	sort.Slice(bs.Uploads, func(i, j int) bool { return bs.Uploads[i].ID < bs.Uploads[j].ID })
	return bs
}

func snapshotMeta(m objectMeta) metaSnapshot {
	return metaSnapshot{ContentType: m.contentType, ContentEncoding: m.contentEncoding,
		ContentDisposition: m.contentDisposition, ContentLanguage: m.contentLanguage,
		CacheControl: m.cacheControl, Expires: m.expires, User: maps.Clone(m.user)}
}

func restoreMeta(m metaSnapshot) objectMeta {
	return objectMeta{contentType: m.ContentType, contentEncoding: m.ContentEncoding,
		contentDisposition: m.ContentDisposition, contentLanguage: m.ContentLanguage,
		cacheControl: m.CacheControl, expires: m.Expires, user: maps.Clone(m.User)}
}

// Restore replaces every bucket with those of a snapshot, taking the object
// bytes from the files of the data directory the snapshot names. A version
// or a part whose file is gone, written by a store that stopped before its
// next snapshot, is dropped with a warning; a file no version names is
// removed.
// keep says which resources of the snapshot, by name, to bring back; nil brings every one.
func (s *Service) Restore(data []byte, keep func(name string) bool) error {
	kept := func(name string) bool { return keep == nil || keep(name) }
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("reading the S3 state: %w", err)
	}
	if snap.Format != snapshotFormat {
		return fmt.Errorf("the S3 state is in format %d; this stratus reads format %d", snap.Format, snapshotFormat)
	}
	files := map[string]bool{}
	buckets := make(map[string]*bucket, len(snap.Buckets))
	for _, bs := range snap.Buckets {
		if !kept(bs.Name) {
			continue
		}
		buckets[bs.Name] = s.restoreBucket(bs, files)
	}
	if err := s.blobs.sweep(files); err != nil {
		return fmt.Errorf("cleaning the S3 data directory: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = buckets
	s.sequence = snap.Sequence
	return nil
}

func (s *Service) restoreBucket(bs bucketSnapshot, keep map[string]bool) *bucket {
	b := newBucket(bs.Name, bs.Region, bs.CreatedAt)
	b.versioning, b.lifecycle, b.notifications = bs.Versioning, bs.Lifecycle, bs.Notifications
	for _, ob := range bs.Objects {
		o := &object{
			key: ob.Key, versionID: ob.VersionID, deleteMarker: ob.DeleteMarker, etag: ob.ETag,
			lastModified: ob.LastModified, meta: restoreMeta(ob.Meta), partSizes: ob.PartSizes,
			checksum: checksum{algorithm: ob.Checksum.Algorithm, value: ob.Checksum.Value, typ: ob.Checksum.Type},
		}
		if ob.Body != nil {
			body, err := s.blobs.adopt(ob.Body.File, ob.Body.Size)
			if err != nil {
				slog.Warn("an object's bytes are gone; the version is dropped",
					"bucket", bs.Name, "key", ob.Key, "version", ob.VersionID, "error", err)
				continue
			}
			keep[ob.Body.File] = true
			o.body = body
		}
		b.objects[o.key] = append(b.objects[o.key], o)
		if len(ob.Tags) > 0 {
			b.objectTags[tagTarget{key: o.key, versionID: o.versionID}] = maps.Clone(ob.Tags)
		}
	}
	for _, us := range bs.Uploads {
		u := &upload{id: us.ID, key: us.Key, initiated: us.Initiated, meta: restoreMeta(us.Meta),
			tags: maps.Clone(us.Tags), algorithm: us.Algorithm, checksumType: us.ChecksumType, parts: map[int]*part{}}
		for _, ps := range us.Parts {
			body, err := s.blobs.adopt(ps.Body.File, ps.Body.Size)
			if err != nil {
				slog.Warn("a part's bytes are gone; the part is dropped",
					"bucket", bs.Name, "upload", us.ID, "part", ps.Number, "error", err)
				continue
			}
			keep[ps.Body.File] = true
			u.parts[ps.Number] = &part{number: ps.Number, body: body, etag: ps.ETag,
				lastModified: ps.LastModified, checksum: ps.Checksum}
		}
		b.uploads[u.id] = u
	}
	return b
}
