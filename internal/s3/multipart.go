package s3

import (
	"encoding/base64"
	"encoding/xml"
	"hash"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	// minPartSize is the smallest size of any part but the last.
	minPartSize = 5 << 20
	maxPartNum  = 10000
)

// upload is a multipart upload in progress. Everything but parts is fixed at
// creation; parts changes under s.mu.
type upload struct {
	id           string
	key          string
	initiated    time.Time
	meta         objectMeta
	tags         map[string]string
	algorithm    string // the checksum algorithm named at creation; empty: none
	checksumType string // checksumFullObject or checksumComposite when algorithm is set
	parts        map[int]*part
}

// part is an uploaded part; immutable once stored.
type part struct {
	number       int
	body         *blob
	etag         string
	lastModified time.Time
	checksum     string // base64, in the upload's algorithm
}

// completeMultipartUpload is the body of CompleteMultipartUpload.
type completeMultipartUpload struct {
	XMLName xml.Name
	Parts   []completedPart `xml:"Part"`
}

type completedPart struct {
	PartNumber        int    `xml:"PartNumber"`
	ETag              string `xml:"ETag"`
	ChecksumCRC32     string `xml:"ChecksumCRC32"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      string `xml:"ChecksumSHA1"`
	ChecksumSHA256    string `xml:"ChecksumSHA256"`
}

// checksumOf is the checksum the completion body names for a part.
func (p completedPart) checksumOf(algorithm string) string {
	return map[string]string{
		algoCRC32: p.ChecksumCRC32, algoCRC32C: p.ChecksumCRC32C, algoCRC64NVME: p.ChecksumCRC64NVME,
		algoSHA1: p.ChecksumSHA1, algoSHA256: p.ChecksumSHA256,
	}[algorithm]
}

// newUploadChecksum validates the checksum algorithm and type a
// CreateMultipartUpload names. CRC64NVME is always full-object; the other
// CRCs default to composite; the SHAs can only be composite.
func newUploadChecksum(algorithmName, typ string) (string, string, error) {
	if algorithmName == "" {
		if typ != "" {
			return "", "", errInvalidRequest("x-amz-checksum-type requires x-amz-checksum-algorithm.")
		}
		return "", "", nil
	}
	algorithm, ok := normalizeAlgorithm(algorithmName)
	if !ok {
		return "", "", errInvalidRequest("Checksum algorithm " + algorithmName + " is not supported.")
	}
	switch {
	case typ == "" && algorithm == algoCRC64NVME:
		typ = checksumFullObject
	case typ == "":
		typ = checksumComposite
	case typ != checksumFullObject && typ != checksumComposite:
		return "", "", errInvalidRequest("x-amz-checksum-type must be FULL_OBJECT or COMPOSITE.")
	}
	if (typ == checksumFullObject && !isCRC(algorithm)) || (typ == checksumComposite && algorithm == algoCRC64NVME) {
		return "", "", errInvalidRequest("The " + typ + " checksum type is not supported for " + algorithm + ".")
	}
	return algorithm, typ, nil
}

// createUpload starts a multipart upload.
func (s *Service) createUpload(bucketName string, u *upload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucketOf(bucketName)
	if err != nil {
		return err
	}
	u.id = strings.ReplaceAll(awsapi.NewID(), "-", "")
	u.initiated = s.clock.Now()
	u.parts = map[int]*part{}
	b.uploads[u.id] = u
	return nil
}

// uploadOf returns an upload of a key; the caller holds s.mu.
func (s *Service) uploadOf(bucketName, key, uploadID string) (*bucket, *upload, error) {
	b, err := s.bucketOf(bucketName)
	if err != nil {
		return nil, nil, err
	}
	u, ok := b.uploads[uploadID]
	if !ok || u.key != key {
		return nil, nil, errNoSuchUpload()
	}
	return b, u, nil
}

// findUpload returns an upload for checks made before a part body is read.
func (s *Service) findUpload(bucketName, key, uploadID string) (*upload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, u, err := s.uploadOf(bucketName, key, uploadID)
	return u, err
}

// putPart stores a part, replacing an earlier one of the same number.
func (s *Service) putPart(bucketName, key, uploadID string, p *part) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, u, err := s.uploadOf(bucketName, key, uploadID)
	if err != nil {
		return err
	}
	p.lastModified = s.clock.Now()
	u.parts[p.number] = p
	return nil
}

// partChecksum is the checksum a part is stored with: the one it was sent
// with, which must be in the upload's algorithm, or one computed here.
func partChecksum(u *upload, sent checksum, data []byte) (string, error) {
	if u.algorithm == "" {
		return "", nil
	}
	if sent.algorithm != "" && sent.algorithm != u.algorithm {
		return "", errInvalidRequest("Checksum Type mismatch occurred, expected checksum Type: " +
			strings.ToLower(u.algorithm) + ", actual checksum Type: " + strings.ToLower(sent.algorithm))
	}
	if sent.value != "" {
		return sent.value, nil
	}
	return computeChecksum(u.algorithm, data), nil
}

// abortUpload drops an upload and its parts.
func (s *Service) abortUpload(bucketName, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _, err := s.uploadOf(bucketName, key, uploadID)
	if err != nil {
		return err
	}
	delete(b.uploads, uploadID)
	return nil
}

// selectParts validates a completion list against the uploaded parts and
// returns them in order: ascending numbers, matching ETags and checksums,
// and every part but the last at least minPartSize.
func (s *Service) selectParts(bucketName, key, uploadID string, wanted []completedPart) (*upload, []*part, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, u, err := s.uploadOf(bucketName, key, uploadID)
	if err != nil {
		return nil, nil, err
	}
	if len(wanted) == 0 {
		return nil, nil, errMalformedXML()
	}
	parts := make([]*part, 0, len(wanted))
	for i, w := range wanted {
		if i > 0 && w.PartNumber <= wanted[i-1].PartNumber {
			return nil, nil, errInvalidPartOrder()
		}
		p, ok := u.parts[w.PartNumber]
		if !ok || strings.Trim(w.ETag, `" `) != strings.Trim(p.etag, `"`) {
			return nil, nil, errInvalidPart("One or more of the specified parts could not be found. The part may not " +
				"have been uploaded, or the specified entity tag may not match the part's entity tag.")
		}
		if c := w.checksumOf(u.algorithm); c != "" && c != p.checksum {
			return nil, nil, errInvalidPart("The " + u.algorithm + " of part " + strconv.Itoa(w.PartNumber) +
				" does not match the checksum it was uploaded with.")
		}
		parts = append(parts, p)
	}
	for _, p := range parts[:len(parts)-1] {
		if p.body.size < minPartSize {
			return nil, nil, errEntityTooSmall()
		}
	}
	return u, parts, nil
}

// assemble builds the object of a completed upload: the parts' bytes copied
// once into one blob, the multipart ETag and the upload's checksum.
// objectSize is x-amz-mp-object-size, or -1; sent is a whole-object checksum
// the completion request carried, if any.
func (s *Service) assemble(u *upload, parts []*part, objectSize int64, sent requestedChecksum) (*object, error) {
	var total int64
	sizes := make([]int64, len(parts))
	etags := make([]string, len(parts))
	bodies := make([]*blob, len(parts))
	for i, p := range parts {
		sizes[i], etags[i], bodies[i] = p.body.size, p.etag, p.body
		total += sizes[i]
	}
	if objectSize >= 0 && objectSize != total {
		return nil, errInvalidRequest("The provided 'x-amz-mp-object-size' header value " +
			strconv.FormatInt(objectSize, 10) + " does not match what was computed: " + strconv.FormatInt(total, 10))
	}
	// The whole-object checksum is computed while the parts are copied.
	full := wholeObjectAlgorithm(u)
	var hashes []hash.Hash
	if full != "" {
		hashes = append(hashes, newHash(full))
	}
	body, err := s.blobs.concat(bodies, hashes...)
	if err != nil {
		return nil, err
	}
	fullValue := ""
	if full != "" {
		fullValue = base64.StdEncoding.EncodeToString(hashes[0].Sum(nil))
	}
	sum, err := uploadChecksum(u, parts, fullValue, sent)
	if err != nil {
		return nil, err
	}
	return &object{key: u.key, body: body, etag: multipartETag(etags), meta: u.meta, checksum: sum, partSizes: sizes}, nil
}

// wholeObjectAlgorithm is the algorithm of the checksum computed over all the
// bytes of a completed upload, or empty for a COMPOSITE one.
func wholeObjectAlgorithm(u *upload) string {
	switch {
	case u.algorithm == "":
		return algoCRC64NVME
	case u.checksumType == checksumFullObject:
		return u.algorithm
	}
	return ""
}

// uploadChecksum is the checksum of a completed upload: fullValue, computed
// over all the bytes, for FULL_OBJECT; over the part checksums for COMPOSITE.
func uploadChecksum(u *upload, parts []*part, fullValue string, sent requestedChecksum) (checksum, error) {
	if u.algorithm == "" {
		return checksum{algorithm: algoCRC64NVME, value: fullValue, typ: checksumFullObject}, nil
	}
	sum := checksum{algorithm: u.algorithm, typ: u.checksumType}
	if u.checksumType == checksumFullObject {
		sum.value = fullValue
	} else {
		partSums := make([]string, len(parts))
		for i, p := range parts {
			partSums[i] = p.checksum
		}
		var ok bool
		if sum.value, ok = compositeChecksum(u.algorithm, partSums); !ok {
			return checksum{}, errInvalidPart("A part checksum is not valid base64.")
		}
	}
	if sent.value != "" && sent.algorithm == u.algorithm && sent.value != sum.value {
		return checksum{}, errBadDigest("The " + u.algorithm + " you specified did not match the calculated checksum.")
	}
	return sum, nil
}

// sortedParts returns an upload's parts by number; the caller holds s.mu.
func (u *upload) sortedParts() []*part {
	parts := make([]*part, 0, len(u.parts))
	for _, p := range u.parts {
		parts = append(parts, p)
	}
	slices.SortFunc(parts, func(a, b *part) int { return a.number - b.number })
	return parts
}
