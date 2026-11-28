package sqs

import (
	"net/http"
	"strconv"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

func errQueueNotFound() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "QueueDoesNotExist",
		QueryCode: "AWS.SimpleQueueService.NonExistentQueue",
		Message:   "The specified queue does not exist.",
	}
}

func errQueueNameExists() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "QueueNameExists",
		QueryCode: "QueueAlreadyExists",
		Message:   "A queue already exists with the same name and a different value for one of its attributes.",
	}
}

func errInvalidAttributeName(name string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "InvalidAttributeName",
		Message: "Unknown or unsupported attribute " + name + ".",
	}
}

func errInvalidAttributeValue(name, reason string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "InvalidAttributeValue",
		Message: "Invalid value for the parameter " + name + ". Reason: " + reason,
	}
}

func errInvalidParameter(message string) *awsapi.Error {
	return &awsapi.Error{Status: http.StatusBadRequest, Code: "InvalidParameterValue", Message: message}
}

func errMissingParameter(name string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "MissingParameter",
		Message: "The request must contain the parameter " + name + ".",
	}
}

func errReceiptHandleInvalid() *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "ReceiptHandleIsInvalid",
		Message: "The input receipt handle is invalid.",
	}
}

func errMessageNotInflight() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "MessageNotInflight",
		QueryCode: "AWS.SimpleQueueService.MessageNotInflight",
		Message:   "The message referred to isn't in flight.",
	}
}

// batchError builds one of the errors that reject a whole batch request.
func batchError(code, message string) *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      code,
		QueryCode: "AWS.SimpleQueueService." + code,
		Message:   message,
	}
}

func errEmptyBatch(entryType string) *awsapi.Error {
	return batchError("EmptyBatchRequest", "There should be at least one "+entryType+" in the request.")
}

func errTooManyEntries(n int) *awsapi.Error {
	return batchError("TooManyEntriesInBatchRequest",
		"Maximum number of entries per request are 10. You have sent "+strconv.Itoa(n)+".")
}

func errBatchIDsNotDistinct(id string) *awsapi.Error {
	return batchError("BatchEntryIdsNotDistinct", "Id "+id+" repeated.")
}

func errInvalidBatchEntryID() *awsapi.Error {
	return batchError("InvalidBatchEntryId", "A batch entry id can only contain alphanumeric "+
		"characters, hyphens and underscores. It can be at most 80 letters long.")
}

func errBatchTooLong(size int) *awsapi.Error {
	return batchError("BatchRequestTooLong", "Batch requests cannot be longer than "+
		strconv.Itoa(maxBatchBytes)+" bytes. You have sent "+strconv.Itoa(size)+" bytes.")
}

func errResourceNotFound(message string) *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusNotFound,
		Code:      "ResourceNotFoundException",
		QueryCode: "ResourceNotFoundException",
		Message:   message,
	}
}

func errInvalidAddress(message string) *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusNotFound,
		Code:      "InvalidAddress",
		QueryCode: "InvalidAddress",
		Message:   message,
	}
}

func errUnsupportedOperation(message string) *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "UnsupportedOperation",
		QueryCode: "AWS.SimpleQueueService.UnsupportedOperation",
		Message:   message,
	}
}
