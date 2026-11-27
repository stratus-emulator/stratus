package kinesis_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestSDKStreamEncryption(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "secure", 1)

	desc, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeNone || desc.StreamDescription.KeyId != nil {
		t.Fatalf("initial DescribeStream encryption = (%v, %v)", desc.StreamDescription.EncryptionType, desc.StreamDescription.KeyId)
	}

	// Validation errors.
	var invalid *types.InvalidArgumentException
	var validation *types.ValidationException
	var notFound *types.ResourceNotFoundException

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeNone,
		KeyId:          aws.String("alias/test"),
	})
	if !errors.As(err, &invalid) {
		t.Fatalf("NONE EncryptionType err = %v, want InvalidArgumentException", err)
	}

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionType("FOO"),
		KeyId:          aws.String("alias/test"),
	})
	if !errors.As(err, &validation) {
		t.Fatalf("unknown EncryptionType err = %v, want ValidationException", err)
	}

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(""),
	})
	if !errors.As(err, &validation) {
		t.Fatalf("missing KeyId err = %v, want ValidationException", err)
	}

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(strings.Repeat("a", 2049)),
	})
	if !errors.As(err, &validation) {
		t.Fatalf("oversized KeyId err = %v, want ValidationException", err)
	}

	_, err = e.client.StopStreamEncryption(ctx, &sdk.StopStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeNone,
		KeyId:          aws.String("alias/test"),
	})
	if !errors.As(err, &invalid) {
		t.Fatalf("Stop NONE EncryptionType err = %v, want InvalidArgumentException", err)
	}

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("missing"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String("alias/test"),
	})
	if !errors.As(err, &notFound) {
		t.Fatalf("missing stream err = %v, want ResourceNotFoundException", err)
	}

	const keyID = "arn:aws:kms:eu-central-1:000000000000:key/1234abcd"
	if _, err := e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(keyID),
	}); err != nil {
		t.Fatalf("StartStreamEncryption: %v", err)
	}

	desc, err = e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStream after Start: %v", err)
	}
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeKms || aws.ToString(desc.StreamDescription.KeyId) != keyID {
		t.Fatalf("DescribeStream after Start = (%v, %v)", desc.StreamDescription.EncryptionType, aws.ToString(desc.StreamDescription.KeyId))
	}

	sum, err := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStreamSummary after Start: %v", err)
	}
	if sum.StreamDescriptionSummary.EncryptionType != types.EncryptionTypeKms || aws.ToString(sum.StreamDescriptionSummary.KeyId) != keyID {
		t.Fatalf("DescribeStreamSummary after Start = (%v, %v)", sum.StreamDescriptionSummary.EncryptionType, aws.ToString(sum.StreamDescriptionSummary.KeyId))
	}

	if _, err := e.client.StopStreamEncryption(ctx, &sdk.StopStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(keyID),
	}); err != nil {
		t.Fatalf("StopStreamEncryption: %v", err)
	}

	desc, err = e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStream after Stop: %v", err)
	}
	sum, err = e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStreamSummary after Stop: %v", err)
	}
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeNone || desc.StreamDescription.KeyId != nil {
		t.Fatalf("DescribeStream after Stop = (%v, %v)", desc.StreamDescription.EncryptionType, desc.StreamDescription.KeyId)
	}
	if sum.StreamDescriptionSummary.EncryptionType != types.EncryptionTypeNone || sum.StreamDescriptionSummary.KeyId != nil {
		t.Fatalf("DescribeStreamSummary after Stop = (%v, %v)", sum.StreamDescriptionSummary.EncryptionType, sum.StreamDescriptionSummary.KeyId)
	}
}
