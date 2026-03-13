package sqserv

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/net/context"
)

func TestZeroQueues(t *testing.T) {
	conf := aws.Config{
		Region: "us-west-2",
	}
	s, err := New(conf, nil)
	if err != nil {
		t.Error(err)
	}

	err = s.ListenAndServe()
	if err == nil {
		t.Error("Expected an error to be generated for not passing any queues")
	}
}

// captureTransport records the last request body and returns a canned response.
type captureTransport struct {
	captured map[string]string // decoded form body of last request
	response string
}

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	// SQS SDK v2 uses query protocol (application/x-www-form-urlencoded)
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		c.captured = map[string]string{"_raw": string(body)}
	} else {
		c.captured = make(map[string]string, len(vals))
		for k, v := range vals {
			if len(v) > 0 {
				c.captured[k] = v[0]
			}
		}
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(c.response)),
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
	}, nil
}

func newServerWithTransport(t *testing.T, rt http.RoundTripper) *SQSServer {
	t.Helper()
	conf := aws.Config{
		Region:     "us-east-1",
		HTTPClient: &http.Client{Transport: rt},
	}
	s, err := New(conf, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const sqsGetQueueURLResponse = `<?xml version="1.0"?><GetQueueUrlResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><GetQueueUrlResult><QueueUrl>https://sqs.us-east-1.amazonaws.com/123456789012/my-queue</QueueUrl></GetQueueUrlResult><ResponseMetadata><RequestId>test-request-id</RequestId></ResponseMetadata></GetQueueUrlResponse>`

func TestGetQueue_AccountID_Set(t *testing.T) {
	ct := &captureTransport{response: sqsGetQueueURLResponse}
	s := newServerWithTransport(t, ct)

	q := QueueConf{Name: "my-queue", Region: "us-east-1", AccountID: "123456789012"}
	_, err := s.getQueue(context.Background(), q)
	if err != nil {
		t.Fatalf("getQueue returned error: %v", err)
	}
	if ct.captured["QueueOwnerAWSAccountId"] != "123456789012" {
		t.Errorf("expected QueueOwnerAWSAccountId=123456789012 in request, got params: %v", ct.captured)
	}
}

func TestGetQueue_AccountID_Empty(t *testing.T) {
	ct := &captureTransport{response: sqsGetQueueURLResponse}
	s := newServerWithTransport(t, ct)

	q := QueueConf{Name: "my-queue", Region: "us-east-1"}
	_, err := s.getQueue(context.Background(), q)
	if err != nil {
		t.Fatalf("getQueue returned error: %v", err)
	}
	if _, ok := ct.captured["QueueOwnerAWSAccountId"]; ok {
		t.Errorf("expected QueueOwnerAWSAccountId to be absent when AccountID is empty, got: %v", ct.captured)
	}
}
