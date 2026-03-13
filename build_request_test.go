package sqserv

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/net/context"
)

func nopLogf(string, ...interface{}) {}

func strPtr(s string) *string { return aws.String(s) }

func TestBuildRequest_BasicPath(t *testing.T) {
	m := types.Message{
		Body:          strPtr("hello"),
		MessageId:     strPtr("msg-1"),
		ReceiptHandle: strPtr("rh-1"),
		MD5OfBody:     strPtr("abc123"),
	}
	req := buildRequest(context.Background(), "my-queue", m, nopLogf)
	if req.URL.Path != "/my-queue" {
		t.Errorf("expected path /my-queue, got %s", req.URL.Path)
	}
	if req.Header.Get("X-Amzn-MessageID") != "msg-1" {
		t.Error("missing X-Amzn-MessageID header")
	}
	if req.Header.Get("X-Amzn-Receipt-Handle") != "rh-1" {
		t.Error("missing X-Amzn-Receipt-Handle header")
	}
	if req.Header.Get("Content-MD5") != "abc123" {
		t.Error("missing Content-MD5 header")
	}
	b, _ := io.ReadAll(req.Body)
	if string(b) != "hello" {
		t.Errorf("expected body 'hello', got %q", string(b))
	}
}

func TestBuildRequest_NilBody(t *testing.T) {
	m := types.Message{Body: nil}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	b, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "" {
		t.Errorf("expected empty body, got %q", string(b))
	}
}

func TestBuildRequest_NilMessageId(t *testing.T) {
	m := types.Message{Body: strPtr("x"), MessageId: nil}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Amzn-MessageID") != "" {
		t.Error("expected no X-Amzn-MessageID header when MessageId is nil")
	}
}

func TestBuildRequest_NilReceiptHandle(t *testing.T) {
	m := types.Message{Body: strPtr("x"), ReceiptHandle: nil}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Amzn-Receipt-Handle") != "" {
		t.Error("expected no X-Amzn-Receipt-Handle header when ReceiptHandle is nil")
	}
}

func TestBuildRequest_NilMD5OfBody(t *testing.T) {
	m := types.Message{Body: strPtr("x"), MD5OfBody: nil}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("Content-MD5") != "" {
		t.Error("expected no Content-MD5 header when MD5OfBody is nil")
	}
}

func TestBuildRequest_PathAttribute_Valid(t *testing.T) {
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Path": {StringValue: strPtr("subpath"), DataType: strPtr("String")},
		},
	}
	req := buildRequest(context.Background(), "my-queue", m, nopLogf)
	if req.URL.Path != "/my-queue/subpath" {
		t.Errorf("expected /my-queue/subpath, got %s", req.URL.Path)
	}
}

func TestBuildRequest_PathAttribute_Traversal(t *testing.T) {
	rejected := false
	logf := func(msg string, args ...interface{}) { rejected = true }

	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Path": {StringValue: strPtr("../other-queue"), DataType: strPtr("String")},
		},
	}
	req := buildRequest(context.Background(), "my-queue", m, logf)
	if req.URL.Path != "/my-queue" {
		t.Errorf("expected path to stay at /my-queue, got %s", req.URL.Path)
	}
	if !rejected {
		t.Error("expected traversal attempt to be logged")
	}
}

func TestBuildRequest_PathAttribute_NilStringValue(t *testing.T) {
	// Should not panic; attribute with nil StringValue is skipped.
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Path": {StringValue: nil, DataType: strPtr("String")},
		},
	}
	req := buildRequest(context.Background(), "my-queue", m, nopLogf)
	if req.URL.Path != "/my-queue" {
		t.Errorf("expected base path /my-queue, got %s", req.URL.Path)
	}
}

func TestBuildRequest_MessageAttribute_String(t *testing.T) {
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"X-Custom": {StringValue: strPtr("myvalue"), DataType: strPtr("String")},
		},
	}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Custom") != "myvalue" {
		t.Errorf("expected header X-Custom=myvalue, got %q", req.Header.Get("X-Custom"))
	}
}

func TestBuildRequest_MessageAttribute_Binary_Excluded(t *testing.T) {
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Blob": {StringValue: strPtr("should-not-appear"), DataType: strPtr("Binary")},
		},
	}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("Blob") != "" {
		t.Error("Binary attributes should not be set as headers")
	}
}

func TestBuildRequest_MessageAttribute_NilDataType_Included(t *testing.T) {
	// A nil DataType is treated as non-binary; the attribute should be set as a header.
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"X-Thing": {StringValue: strPtr("val"), DataType: nil},
		},
	}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Thing") != "val" {
		t.Errorf("expected header X-Thing=val when DataType is nil, got %q", req.Header.Get("X-Thing"))
	}
}

func TestBuildRequest_MessageAttribute_NilStringValue_Skipped(t *testing.T) {
	// An attribute with nil StringValue must not panic and must not be added as a header.
	m := types.Message{
		Body: strPtr("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"X-Thing": {StringValue: nil, DataType: strPtr("String")},
		},
	}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Thing") != "" {
		t.Error("expected no header for attribute with nil StringValue")
	}
}

func TestBuildRequest_SQSAttributes_MappedToHeaders(t *testing.T) {
	m := types.Message{
		Body: strPtr("x"),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "3",
		},
	}
	req := buildRequest(context.Background(), "q", m, nopLogf)
	if req.Header.Get("X-Amzn-ApproximateReceiveCount") != "3" {
		t.Errorf("expected X-Amzn-ApproximateReceiveCount=3, got %q", req.Header.Get("X-Amzn-ApproximateReceiveCount"))
	}
}
