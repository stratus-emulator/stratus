package s3_test

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/s3"
)

func TestSDKBucketTagging(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()

	_, err := e.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("missing")})
	wantAPIError(t, err, "NoSuchBucket")
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("missing"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
	})
	wantAPIError(t, err, "NoSuchBucket")
	_, err = e.client.DeleteBucketTagging(ctx, &s3sdk.DeleteBucketTaggingInput{Bucket: aws.String("missing")})
	wantAPIError(t, err, "NoSuchBucket")

	e.mustCreateBucket(t, "tagged", s3.BucketOptions{})

	_, err = e.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("tagged")})
	wantAPIError(t, err, "NoSuchTagSet")
	wantStatus(t, err, 404)

	if _, err := e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket: aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("dev")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		}},
	}); err != nil {
		t.Fatalf("PutBucketTagging: %v", err)
	}

	got, err := e.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("tagged")})
	if err != nil {
		t.Fatalf("GetBucketTagging: %v", err)
	}
	want := map[string]string{"env": "dev", "team": "platform"}
	have := make(map[string]string, len(got.TagSet))
	for _, tag := range got.TagSet {
		have[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if !maps.Equal(have, want) {
		t.Fatalf("tags = %v, want %v", have, want)
	}

	// Replacement drops old keys; 50 tags is the limit.
	fifty := make([]types.Tag, 50)
	for i := range fifty {
		fifty[i] = types.Tag{Key: aws.String(fmt.Sprintf("k%02d", i)), Value: aws.String("v")}
	}
	if _, err := e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: fifty},
	}); err != nil {
		t.Fatalf("PutBucketTagging 50 tags: %v", err)
	}
	got50, err := e.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("tagged")})
	if err != nil || len(got50.TagSet) != 50 {
		t.Fatalf("GetBucketTagging 50 = %d, %v", len(got50.TagSet), err)
	}

	// Over 50 tags is rejected.
	fiftyOne := append(append([]types.Tag(nil), fifty...), types.Tag{Key: aws.String("extra"), Value: aws.String("v")})
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: fiftyOne},
	})
	wantAPIError(t, err, "InvalidTag")

	// Empty key, duplicate key, and oversized value are rejected.
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String(""), Value: aws.String("v")}}},
	})
	wantAPIError(t, err, "InvalidTag")
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket: aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String("dup"), Value: aws.String("a")},
			{Key: aws.String("dup"), Value: aws.String("b")},
		}},
	})
	wantAPIError(t, err, "InvalidTag")
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String(strings.Repeat("x", 257))}}},
	})
	wantAPIError(t, err, "InvalidTag")

	// Multi-byte UTF-8 characters are counted in Unicode characters (runes), not bytes.
	if _, err := e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket: aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String(strings.Repeat("ł", 128)), Value: aws.String(strings.Repeat("ó", 256))},
		}},
	}); err != nil {
		t.Fatalf("PutBucketTagging 128/256 Unicode chars: %v", err)
	}
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String(strings.Repeat("ł", 129)), Value: aws.String("v")}}},
	})
	wantAPIError(t, err, "InvalidTag")

	// Reserved aws: prefix and disallowed characters are rejected.
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("aws:cloudformation:stack"), Value: aws.String("v")}}},
	})
	wantAPIError(t, err, "InvalidTag")
	_, err = e.client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  aws.String("tagged"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("bad!key"), Value: aws.String("v")}}},
	})
	wantAPIError(t, err, "InvalidTag")

	if _, err := e.client.DeleteBucketTagging(ctx, &s3sdk.DeleteBucketTaggingInput{Bucket: aws.String("tagged")}); err != nil {
		t.Fatalf("DeleteBucketTagging: %v", err)
	}
	_, err = e.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("tagged")})
	wantAPIError(t, err, "NoSuchTagSet")
}
