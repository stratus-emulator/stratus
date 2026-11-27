package kinesis

import (
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

func TestASnapshotKeepsStreamsRecordsAndCheckpoints(t *testing.T) {
	s, clk := newTestService(t, 2)
	st := mustStream(t, s)
	st.retentionHours = 48
	st.encryptionType = "KMS"
	st.keyID = "alias/stratus-test"
	st.tags = map[string]string{"team": "mail"}
	var checkpoint uint64
	for _, d := range []string{"a", "b", "c"} {
		checkpoint = mustPut(t, s, "k", d).seq
	}

	data, err := s.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	clk.Advance(time.Minute)
	next := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clk)
	if err := next.Restore(data, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := mustStream(t, next)
	if len(got.shards) != 2 || got.retentionHours != 48 || got.encryptionType != "KMS" || got.keyID != "alias/stratus-test" || got.tags["team"] != "mail" || !got.created.Equal(st.created) {
		t.Fatalf("restored stream: %d shards, %dh retention, enc %q/%q, tags %v, created %v", len(got.shards), got.retentionHours, got.encryptionType, got.keyID, got.tags, got.created)
	}
	// A consumer's checkpoint of the stream is still valid: it resumes after it.
	mustPut(t, next, "k", "d")
	token := iterate(t, next, shardOf(t, next, "k"), iteratorSpec{kind: "AFTER_SEQUENCE_NUMBER", seq: checkpoint})
	if recs := datas(read(t, next, token, 10).records); len(recs) != 1 || recs[0] != "d" {
		t.Fatalf("after the checkpoint: %v, want [d]", recs)
	}
	all := 0
	for _, sh := range got.shards {
		all += len(sh.records)
	}
	if all != 4 {
		t.Fatalf("%d records after Restore and one put, want 4", all)
	}
}

// shardOf is the shard a partition key lands on.
func shardOf(t *testing.T, s *Service, pk string) string {
	t.Helper()
	return mustStream(t, s).route(partitionHash(pk)).id
}

func TestKinesisRestoreRefusesAnotherFormat(t *testing.T) {
	s, _ := newTestService(t, 1)
	if err := s.Restore([]byte(`{"format":99}`), nil); err == nil {
		t.Fatal("Restore of format 99 succeeded")
	}
}

func TestStreamEncryptionValidationAndCanonicalStop(t *testing.T) {
	s, _ := newTestService(t, 1)
	st := mustStream(t, s)

	// Missing EncryptionType returns ValidationException (errConstraint).
	if _, err := s.startStreamEncryption(streamEncryptionInput{
		streamRef: streamRef{StreamName: st.name},
		KeyId:     "alias/test",
	}); code(err) != "ValidationException" {
		t.Fatalf("missing EncryptionType code = %q, want ValidationException", code(err))
	}

	if _, err := s.startStreamEncryption(streamEncryptionInput{
		streamRef:      streamRef{StreamName: st.name},
		EncryptionType: "KMS",
		KeyId:          "alias/test",
	}); err != nil {
		t.Fatalf("startStreamEncryption: %v", err)
	}
	if _, err := s.stopStreamEncryption(streamEncryptionInput{
		streamRef:      streamRef{StreamName: st.name},
		EncryptionType: "KMS",
		KeyId:          "alias/test",
	}); err != nil {
		t.Fatalf("stopStreamEncryption: %v", err)
	}
	if st.encryptionType != "" || st.keyID != "" {
		t.Fatalf("after Stop = (%q, %q), want empty canonical state", st.encryptionType, st.keyID)
	}
}
