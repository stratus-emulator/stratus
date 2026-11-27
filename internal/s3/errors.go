package s3

import (
	"errors"
	"net/http"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

func s3Error(status int, code, message string) *awsapi.Error {
	return &awsapi.Error{Status: status, Code: code, Message: message}
}

// errorCode is the S3 code of an error; one that is not an awsapi.Error is
// an InternalError.
func errorCode(err error) string {
	var apiErr *awsapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return "InternalError"
}

func errNoSuchBucket() *awsapi.Error {
	return s3Error(http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
}

func errNoSuchKey() *awsapi.Error {
	return s3Error(http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
}

func errNoSuchVersion() *awsapi.Error {
	return s3Error(http.StatusNotFound, "NoSuchVersion", "The specified version does not exist.")
}

func errNoSuchUpload() *awsapi.Error {
	return s3Error(http.StatusNotFound, "NoSuchUpload",
		"The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")
}

func errNoSuchLifecycle() *awsapi.Error {
	return s3Error(http.StatusNotFound, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist")
}

func errBucketExists() *awsapi.Error {
	return s3Error(http.StatusConflict, "BucketAlreadyOwnedByYou",
		"Your previous request to create the named bucket succeeded and you already own it.")
}

func errBucketNotEmpty() *awsapi.Error {
	return s3Error(http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty")
}

func errInvalidBucketName() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.")
}

func errPreconditionFailed() *awsapi.Error {
	return s3Error(http.StatusPreconditionFailed, "PreconditionFailed",
		"At least one of the pre-conditions you specified did not hold")
}

func errInvalidRange() *awsapi.Error {
	return s3Error(http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
}

func errInvalidPartNumber() *awsapi.Error {
	return s3Error(http.StatusRequestedRangeNotSatisfiable, "InvalidPartNumber", "The requested partnumber is not satisfiable")
}

func errInvalidPart(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidPart", message)
}

func errInvalidPartOrder() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidPartOrder",
		"The list of parts was not in ascending order. Parts must be ordered by part number.")
}

func errEntityTooSmall() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "EntityTooSmall",
		"Your proposed upload is smaller than the minimum allowed object size.")
}

func errEntityTooLarge() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "EntityTooLarge",
		"Your proposed upload exceeds the maximum allowed object size.")
}

func errBadDigest(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "BadDigest", message)
}

func errInvalidDigest() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidDigest", "The Content-MD5 you specified was invalid.")
}

func errIncompleteBody(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "IncompleteBody", message)
}

func errInvalidRequest(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidRequest", message)
}

func errInvalidTag(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidTag", message)
}

func errInvalidArgument(message string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "InvalidArgument", message)
}

func errMalformedXML() *awsapi.Error {
	return s3Error(http.StatusBadRequest, "MalformedXML",
		"The XML you provided was not well-formed or did not validate against our published schema")
}

func errMethodNotAllowed() *awsapi.Error {
	return s3Error(http.StatusMethodNotAllowed, "MethodNotAllowed",
		"The specified method is not allowed against this resource.")
}

func errNotImplemented(operation string) *awsapi.Error {
	return s3Error(http.StatusNotImplemented, "NotImplemented",
		"stratus does not implement the S3 operation "+operation+".")
}

func errAccessDenied(message string) *awsapi.Error {
	return s3Error(http.StatusForbidden, "AccessDenied", message)
}

func errIllegalLocation(location string) *awsapi.Error {
	return s3Error(http.StatusBadRequest, "IllegalLocationConstraintException",
		"The "+location+" location constraint is incompatible for the region specific endpoint this request was sent to.")
}
