package kinesis_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/gateway"
	"github.com/stratus-emulator/stratus/internal/kinesis"
)

var start = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type env struct {
	client *sdk.Client
	svc    *kinesis.Service
	clock  *clock.Fake
	url    string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	clk := clock.NewFake(start)
	svc := kinesis.New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clk)
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	client := sdk.New(sdk.Options{
		Region:       awsapi.DefaultRegion,
		BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
	return &env{client: client, svc: svc, clock: clk, url: srv.URL}
}

func (e *env) createStream(t *testing.T, name string, shards int32) {
	t.Helper()
	if _, err := e.client.CreateStream(t.Context(), &sdk.CreateStreamInput{
		StreamName: aws.String(name), ShardCount: aws.Int32(shards),
	}); err != nil {
		t.Fatalf("CreateStream(%s): %v", name, err)
	}
}

func (e *env) iterator(t *testing.T, stream, shard string, typ types.ShardIteratorType) *string {
	t.Helper()
	out, err := e.client.GetShardIterator(t.Context(), &sdk.GetShardIteratorInput{
		StreamName: aws.String(stream), ShardId: aws.String(shard), ShardIteratorType: typ,
	})
	if err != nil {
		t.Fatalf("GetShardIterator: %v", err)
	}
	return out.ShardIterator
}

// drain reads a shard from TRIM_HORIZON until a read comes back empty.
func (e *env) drain(t *testing.T, stream, shard string) []types.Record {
	t.Helper()
	it := e.iterator(t, stream, shard, types.ShardIteratorTypeTrimHorizon)
	var all []types.Record
	for {
		out, err := e.client.GetRecords(t.Context(), &sdk.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(7)})
		if err != nil {
			t.Fatalf("GetRecords: %v", err)
		}
		if len(out.Records) == 0 {
			return all
		}
		all = append(all, out.Records...)
		it = out.NextShardIterator
	}
}

func TestSDKStreamManagement(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "orders", 3)
	e.createStream(t, "audit", 1)

	desc, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("orders")})
	if err != nil {
		t.Fatal(err)
	}
	d := desc.StreamDescription
	if d.StreamStatus != types.StreamStatusActive || len(d.Shards) != 3 || *d.HasMoreShards ||
		*d.RetentionPeriodHours != 24 || *d.StreamARN != "arn:aws:kinesis:eu-central-1:000000000000:stream/orders" {
		t.Fatalf("DescribeStream = %+v", d)
	}
	if got := *d.Shards[0].HashKeyRange.StartingHashKey; got != "0" {
		t.Fatalf("first shard starts at %s", got)
	}
	if got := *d.Shards[2].HashKeyRange.EndingHashKey; got != "340282366920938463463374607431768211455" {
		t.Fatalf("last shard ends at %s", got)
	}
	if d.Shards[0].SequenceNumberRange.EndingSequenceNumber != nil {
		t.Fatal("an open shard has an ending sequence number")
	}

	page, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{
		StreamName: aws.String("orders"), Limit: aws.Int32(1), ExclusiveStartShardId: d.Shards[0].ShardId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := page.StreamDescription; len(p.Shards) != 1 || *p.Shards[0].ShardId != "shardId-000000000001" || !*p.HasMoreShards {
		t.Fatalf("DescribeStream page = %+v", p)
	}

	sum, err := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamARN: d.StreamARN})
	if err != nil {
		t.Fatal(err)
	}
	s := sum.StreamDescriptionSummary
	if *s.OpenShardCount != 3 || *s.RetentionPeriodHours != 24 || s.StreamStatus != types.StreamStatusActive ||
		s.StreamModeDetails.StreamMode != types.StreamModeProvisioned || !s.StreamCreationTimestamp.Equal(start) {
		t.Fatalf("DescribeStreamSummary = %+v", s)
	}

	list, err := e.client.ListStreams(ctx, &sdk.ListStreamsInput{Limit: aws.Int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.StreamNames) != 1 || list.StreamNames[0] != "audit" || !*list.HasMoreStreams || list.NextToken == nil {
		t.Fatalf("ListStreams page 1 = %+v", list)
	}
	list, err = e.client.ListStreams(ctx, &sdk.ListStreamsInput{ExclusiveStartStreamName: aws.String("audit")})
	if err != nil || len(list.StreamNames) != 1 || list.StreamNames[0] != "orders" || *list.HasMoreStreams {
		t.Fatalf("ListStreams after audit = %+v, %v", list, err)
	}

	if _, err := e.client.DeleteStream(ctx, &sdk.DeleteStreamInput{StreamARN: d.StreamARN}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("orders")}); err == nil {
		t.Fatal("a deleted stream is still described")
	}
}

func TestSDKListShardsPaging(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "wide", 5)

	var ids []string
	var token *string
	for {
		in := &sdk.ListShardsInput{MaxResults: aws.Int32(2), NextToken: token}
		if token == nil {
			in.StreamName = aws.String("wide")
		}
		out, err := e.client.ListShards(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range out.Shards {
			ids = append(ids, *s.ShardId)
		}
		if token = out.NextToken; token == nil {
			break
		}
	}
	if want := "shardId-000000000000,shardId-000000000001,shardId-000000000002,shardId-000000000003,shardId-000000000004"; strings.Join(ids, ",") != want {
		t.Fatalf("paged shards = %v", ids)
	}

	out, err := e.client.ListShards(ctx, &sdk.ListShardsInput{
		StreamName: aws.String("wide"), ExclusiveStartShardId: aws.String("shardId-000000000002"),
	})
	if err != nil || len(out.Shards) != 2 || *out.Shards[0].ShardId != "shardId-000000000003" {
		t.Fatalf("ListShards after shard 2 = %+v, %v", out, err)
	}
}

func TestSDKRetentionAndTags(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "orders", 1)
	name := aws.String("orders")

	if _, err := e.client.IncreaseStreamRetentionPeriod(ctx, &sdk.IncreaseStreamRetentionPeriodInput{
		StreamName: name, RetentionPeriodHours: aws.Int32(72),
	}); err != nil {
		t.Fatal(err)
	}
	sum, _ := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: name})
	if got := *sum.StreamDescriptionSummary.RetentionPeriodHours; got != 72 {
		t.Fatalf("retention = %d", got)
	}
	var invalid *types.InvalidArgumentException
	if _, err := e.client.IncreaseStreamRetentionPeriod(ctx, &sdk.IncreaseStreamRetentionPeriodInput{
		StreamName: name, RetentionPeriodHours: aws.Int32(48),
	}); !errors.As(err, &invalid) {
		t.Fatalf("shortening by Increase: %v", err)
	}
	if _, err := e.client.DecreaseStreamRetentionPeriod(ctx, &sdk.DecreaseStreamRetentionPeriodInput{
		StreamName: name, RetentionPeriodHours: aws.Int32(24),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.client.AddTagsToStream(ctx, &sdk.AddTagsToStreamInput{
		StreamName: name, Tags: map[string]string{"team": "a", "env": "dev"},
	}); err != nil {
		t.Fatal(err)
	}
	tags, err := e.client.ListTagsForStream(ctx, &sdk.ListTagsForStreamInput{StreamName: name})
	if err != nil || len(tags.Tags) != 2 || *tags.Tags[0].Key != "env" || *tags.Tags[1].Value != "a" {
		t.Fatalf("ListTagsForStream = %+v, %v", tags, err)
	}
	if _, err := e.client.RemoveTagsFromStream(ctx, &sdk.RemoveTagsFromStreamInput{StreamName: name, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = e.client.ListTagsForStream(ctx, &sdk.ListTagsForStreamInput{StreamName: name}); len(tags.Tags) != 1 {
		t.Fatalf("tags after removal = %+v", tags.Tags)
	}
}

func TestSDKProduceThenConsumeAcrossShards(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "events", 4)

	// Records of one partition key stay together, so each key has an order to
	// check: written[key] is the order it was put in.
	written := map[string][]string{}
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("user-%d", i%12)
		data := fmt.Sprintf("%s#%d", key, i)
		if i%3 == 0 {
			out, err := e.client.PutRecords(ctx, &sdk.PutRecordsInput{
				StreamName: aws.String("events"),
				Records:    []types.PutRecordsRequestEntry{{Data: []byte(data), PartitionKey: aws.String(key)}},
			})
			if err != nil || *out.FailedRecordCount != 0 || len(out.Records) != 1 || out.Records[0].ShardId == nil {
				t.Fatalf("PutRecords = %+v, %v", out, err)
			}
		} else {
			out, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{
				StreamName: aws.String("events"), Data: []byte(data), PartitionKey: aws.String(key),
			})
			if err != nil || len(*out.SequenceNumber) != 56 {
				t.Fatalf("PutRecord = %+v, %v", out, err)
			}
		}
		written[key] = append(written[key], data)
	}

	read := map[string][]string{}
	total, shardsWithData := 0, 0
	for i := 0; i < 4; i++ {
		recs := e.drain(t, "events", fmt.Sprintf("shardId-%012d", i))
		if len(recs) > 0 {
			shardsWithData++
		}
		prev := ""
		for _, r := range recs {
			if *r.SequenceNumber <= prev {
				t.Fatalf("shard %d: %s does not follow %s", i, *r.SequenceNumber, prev)
			}
			prev = *r.SequenceNumber
			read[*r.PartitionKey] = append(read[*r.PartitionKey], string(r.Data))
			if r.ApproximateArrivalTimestamp == nil || !r.ApproximateArrivalTimestamp.Equal(start) {
				t.Fatalf("arrival = %v", r.ApproximateArrivalTimestamp)
			}
			total++
		}
	}
	if total != 120 || shardsWithData < 2 {
		t.Fatalf("read %d records from %d shards", total, shardsWithData)
	}
	for key, want := range written {
		if got := read[key]; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s: read %v, wrote %v", key, got, want)
		}
	}
}

func TestSDKConsumerAtLatestSeesLaterRecords(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "live", 1)
	shard := "shardId-000000000000"
	if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("live"), Data: []byte("before"), PartitionKey: aws.String("k")}); err != nil {
		t.Fatal(err)
	}

	it := e.iterator(t, "live", shard, types.ShardIteratorTypeLatest)
	empty, err := e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: it})
	if err != nil || len(empty.Records) != 0 || empty.NextShardIterator == nil || *empty.MillisBehindLatest != 0 {
		t.Fatalf("first poll = %+v, %v", empty, err)
	}
	for _, d := range []string{"one", "two"} {
		if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("live"), Data: []byte(d), PartitionKey: aws.String("k")}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: empty.NextShardIterator})
	if err != nil || len(got.Records) != 2 || string(got.Records[0].Data) != "one" || string(got.Records[1].Data) != "two" {
		t.Fatalf("second poll = %+v, %v", got, err)
	}
}

func TestSDKIteratorTypes(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "typed", 1)
	shard := aws.String("shardId-000000000000")
	var seqs []*string
	for _, d := range []string{"a", "b", "c"} {
		out, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("typed"), Data: []byte(d), PartitionKey: aws.String("k")})
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, out.SequenceNumber)
		e.clock.Advance(time.Minute)
	}
	tests := []struct {
		name string
		in   sdk.GetShardIteratorInput
		want string
	}{
		{"at sequence", sdk.GetShardIteratorInput{ShardIteratorType: types.ShardIteratorTypeAtSequenceNumber, StartingSequenceNumber: seqs[1]}, "bc"},
		{"after sequence", sdk.GetShardIteratorInput{ShardIteratorType: types.ShardIteratorTypeAfterSequenceNumber, StartingSequenceNumber: seqs[1]}, "c"},
		{"at timestamp", sdk.GetShardIteratorInput{ShardIteratorType: types.ShardIteratorTypeAtTimestamp, Timestamp: aws.Time(start.Add(30 * time.Second))}, "bc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.StreamName, tc.in.ShardId = aws.String("typed"), shard
			it, err := e.client.GetShardIterator(ctx, &tc.in)
			if err != nil {
				t.Fatal(err)
			}
			out, err := e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: it.ShardIterator})
			if err != nil {
				t.Fatal(err)
			}
			var got string
			for _, r := range out.Records {
				got += string(r.Data)
			}
			if got != tc.want {
				t.Fatalf("read %q, want %q", got, tc.want)
			}
		})
	}
	// A minute passed between the puts: the first record is 2 minutes behind the last.
	out, _ := e.client.GetRecords(ctx, &sdk.GetRecordsInput{
		ShardIterator: e.iterator(t, "typed", *shard, types.ShardIteratorTypeTrimHorizon), Limit: aws.Int32(1),
	})
	if *out.MillisBehindLatest != 120000 {
		t.Fatalf("MillisBehindLatest = %d, want 120000", *out.MillisBehindLatest)
	}
}

func TestSDKBinaryPayloadsRoundTrip(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "blobs", 1)

	every := make([]byte, 256)
	for i := range every {
		every[i] = byte(i)
	}
	gzipMagic := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xfe}
	large := bytes.Repeat([]byte{0xAB, 0x00, 0xFF}, 100000)
	payloads := [][]byte{every, gzipMagic, large, {}, {0}}
	for _, p := range payloads {
		if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("blobs"), Data: p, PartitionKey: aws.String("k")}); err != nil {
			t.Fatalf("PutRecord(%d bytes): %v", len(p), err)
		}
	}
	got := e.drain(t, "blobs", "shardId-000000000000")
	if len(got) != len(payloads) {
		t.Fatalf("read %d records, want %d", len(got), len(payloads))
	}
	for i, p := range payloads {
		if !bytes.Equal(got[i].Data, p) {
			t.Fatalf("payload %d changed in transit (%d bytes in, %d out)", i, len(p), len(got[i].Data))
		}
	}
}

func TestSDKTypedErrors(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "orders", 1)

	var notFound *types.ResourceNotFoundException
	if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("nope"), Data: []byte("x"), PartitionKey: aws.String("k")}); !errors.As(err, &notFound) {
		t.Errorf("PutRecord to a missing stream: %v", err)
	}
	if _, err := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{
		StreamARN: aws.String("arn:aws:kinesis:eu-central-1:000000000000:stream/nope"),
	}); !errors.As(err, &notFound) {
		t.Errorf("describe by missing ARN: %v", err)
	}
	if _, err := e.client.GetShardIterator(ctx, &sdk.GetShardIteratorInput{
		StreamName: aws.String("orders"), ShardId: aws.String("shardId-000000000009"), ShardIteratorType: types.ShardIteratorTypeLatest,
	}); !errors.As(err, &notFound) {
		t.Errorf("iterator of an unknown shard: %v", err)
	}

	var inUse *types.ResourceInUseException
	if _, err := e.client.CreateStream(ctx, &sdk.CreateStreamInput{StreamName: aws.String("orders"), ShardCount: aws.Int32(1)}); !errors.As(err, &inUse) {
		t.Errorf("duplicate CreateStream: %v", err)
	}

	var invalid *types.InvalidArgumentException
	if _, err := e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: aws.String("garbage")}); !errors.As(err, &invalid) {
		t.Errorf("malformed iterator: %v", err)
	}
	if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{
		StreamName: aws.String("orders"), Data: []byte("x"), PartitionKey: aws.String("k"), ExplicitHashKey: aws.String("-1"),
	}); !errors.As(err, &invalid) {
		t.Errorf("bad ExplicitHashKey: %v", err)
	}

	var validation *types.ValidationException
	tooMany := make([]types.PutRecordsRequestEntry, 501)
	for i := range tooMany {
		tooMany[i] = types.PutRecordsRequestEntry{Data: []byte("x"), PartitionKey: aws.String("k")}
	}
	if _, err := e.client.PutRecords(ctx, &sdk.PutRecordsInput{StreamName: aws.String("orders"), Records: tooMany}); !errors.As(err, &validation) {
		t.Errorf("PutRecords with 501 records: %v", err)
	}

	var limit *types.LimitExceededException
	tags := map[string]string{}
	for i := 0; i < 51; i++ {
		tags[fmt.Sprintf("k%02d", i)] = "v"
	}
	if _, err := e.client.AddTagsToStream(ctx, &sdk.AddTagsToStreamInput{StreamName: aws.String("orders"), Tags: tags}); !errors.As(err, &limit) {
		t.Errorf("51 tags: %v", err)
	}
}

func TestSDKExpiredIterator(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "orders", 1)
	it := e.iterator(t, "orders", "shardId-000000000000", types.ShardIteratorTypeTrimHorizon)

	e.clock.Advance(5*time.Minute + time.Second)
	var expired *types.ExpiredIteratorException
	if _, err := e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: it}); !errors.As(err, &expired) {
		t.Fatalf("GetRecords with a 5-minute-old iterator: %v", err)
	}
}

func TestSDKTrimExpired(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "orders", 1)
	put := func(d string) {
		if _, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{StreamName: aws.String("orders"), Data: []byte(d), PartitionKey: aws.String("k")}); err != nil {
			t.Fatal(err)
		}
	}
	put("old")
	e.clock.Advance(20 * time.Hour)
	put("new")
	e.clock.Advance(5 * time.Hour)
	if n := e.svc.TrimExpired(); n != 1 {
		t.Fatalf("TrimExpired = %d, want 1", n)
	}
	got := e.drain(t, "orders", "shardId-000000000000")
	if len(got) != 1 || string(got[0].Data) != "new" {
		t.Fatalf("after trimming: %+v", got)
	}
}

func TestSDKConcurrentProducers(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "busy", 4)
	const producers, perProducer = 8, 50

	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				_, err := e.client.PutRecord(ctx, &sdk.PutRecordInput{
					StreamName:   aws.String("busy"),
					Data:         []byte(fmt.Sprintf("%d/%d", p, i)),
					PartitionKey: aws.String(fmt.Sprintf("p%d", p)),
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	// A consumer reads while the producers write; it must never see an error.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			it, err := e.client.GetShardIterator(ctx, &sdk.GetShardIteratorInput{
				StreamName: aws.String("busy"), ShardId: aws.String("shardId-000000000000"), ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			})
			if err == nil {
				_, err = e.client.GetRecords(ctx, &sdk.GetRecordsInput{ShardIterator: it.ShardIterator})
			}
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	perKey := map[string][]string{}
	total := 0
	for i := 0; i < 4; i++ {
		for _, r := range e.drain(t, "busy", fmt.Sprintf("shardId-%012d", i)) {
			perKey[*r.PartitionKey] = append(perKey[*r.PartitionKey], string(r.Data))
			total++
		}
	}
	if total != producers*perProducer {
		t.Fatalf("stored %d records, want %d", total, producers*perProducer)
	}
	for p := 0; p < producers; p++ {
		got := perKey[fmt.Sprintf("p%d", p)]
		for i, d := range got {
			if want := fmt.Sprintf("%d/%d", p, i); d != want {
				t.Fatalf("producer %d: record %d is %q, want %q", p, i, d, want)
			}
		}
	}
}

func TestRawPutRecordWithPlaceholderAuthorization(t *testing.T) {
	e := newEnv(t)
	e.createStream(t, "raw", 1)

	data := base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 0xff})
	body := `{"StreamName":"raw","PartitionKey":"k","Data":"` + data + `"}`
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, e.url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.PutRecord")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/kinesis/aws4_request, SignedHeaders=host, Signature=e2e")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/x-amz-json-1.1" {
		t.Fatalf("PutRecord = %d %s: %s", resp.StatusCode, resp.Header.Get("Content-Type"), raw)
	}
	var out struct{ ShardId, SequenceNumber string }
	if err := json.Unmarshal(raw, &out); err != nil || out.ShardId != "shardId-000000000000" || len(out.SequenceNumber) != 56 {
		t.Fatalf("response %s (%v)", raw, err)
	}
	if got := e.drain(t, "raw", out.ShardId); len(got) != 1 || !bytes.Equal(got[0].Data, []byte{0, 1, 2, 0xff}) {
		t.Fatalf("stored %+v", got)
	}
}

// post sends one raw request and returns the status and decoded error body.
func post(t *testing.T, url, contentType, target, body string) (int, map[string]string) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Amz-Target", target)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestUnsupportedAndCBOR(t *testing.T) {
	e := newEnv(t)
	const json11 = "application/x-amz-json-1.1"

	for _, op := range []string{"SubscribeToShard", "UpdateShardCount", "RegisterStreamConsumer"} {
		status, out := post(t, e.url, json11, "Kinesis_20131202."+op, "{}")
		if status != http.StatusBadRequest || !strings.Contains(out["message"], op) || out["__type"] != "UnsupportedOperation" {
			t.Errorf("%s: %d %v", op, status, out)
		}
	}

	status, out := post(t, e.url, "application/x-amz-cbor-1.1", "Kinesis_20131202.ListStreams", "")
	if status != http.StatusBadRequest || !strings.Contains(out["message"], "CBOR") {
		t.Errorf("CBOR request: %d %v", status, out)
	}

	// The error shape is the bare name, with no namespace.
	status, out = post(t, e.url, json11, "Kinesis_20131202.DescribeStreamSummary", `{"StreamName":"missing"}`)
	if status != http.StatusBadRequest || out["__type"] != "ResourceNotFoundException" {
		t.Errorf("error body: %d %v", status, out)
	}
}

func TestSDKCreateStreamOnDemand(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	if _, err := e.client.CreateStream(ctx, &sdk.CreateStreamInput{
		StreamName: aws.String("auto"), StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand},
	}); err != nil {
		t.Fatal(err)
	}
	sum, err := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: aws.String("auto")})
	if err != nil || *sum.StreamDescriptionSummary.OpenShardCount != 4 ||
		sum.StreamDescriptionSummary.StreamModeDetails.StreamMode != types.StreamModeOnDemand {
		t.Fatalf("on-demand stream = %+v, %v", sum, err)
	}
	// The typed API a manifest uses creates the same kind of stream.
	if err := e.svc.CreateStream("from-manifest", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("from-manifest")}); err != nil {
		t.Fatal(err)
	}
}
