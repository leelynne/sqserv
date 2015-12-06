package sqserv

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

// SQSServer handles SQS messages in a similar fashion to http.Server
type SQSServer struct {
	// ErrorLog specifies an optional logger for message related
	// errors. If nil, logging goes to os.Stderr
	ErrorLog            *log.Logger
	Handler             http.Handler
	serv                *sqs.SQS
	stopPolling         func()
	stopTasks           func()
	tasks               sync.WaitGroup
	queuePollers        sync.WaitGroup
	mu                  sync.Mutex
	currentPollRequests map[string]*request.Request
}

type writer struct {
	status int
}

type queue struct {
	name               string
	url                string
	batchSize          int64
	attributesToReturn []*string
}

// New creates a new SQSServer.
// If Handler is nil http.DefaultServeMux is used
func New(conf *aws.Config, h http.Handler) (*SQSServer, error) {
	if h == nil {
		h = http.DefaultServeMux
	}
	conf.HTTPClient = &http.Client{
		Transport: http.DefaultTransport,
	}
	return &SQSServer{
			Handler:             h,
			serv:                sqs.New(session.New(), conf),
			queuePollers:        sync.WaitGroup{},
			currentPollRequests: map[string]*request.Request{},
		},
		nil
}

// ListenAndServe begins polling SQS without blocking.
func (s *SQSServer) ListenAndServe(queues ...string) error {
	if len(queues) == 0 {
		return fmt.Errorf("Must specify at least one SQS queue to poll")
	}
	pollctx, pollcancel := context.WithCancel(context.Background())
	taskctx, taskcancel := context.WithCancel(context.Background())
	s.stopPolling = pollcancel
	s.stopTasks = taskcancel
	return s.pollQueues(pollctx, taskctx, queues)
}

// Shutdown gracefully stops queue listeners and gives running
// tasks a change to finish.
// Shutdown will wait up maxWait duration before returning if any tasks are still running
func (s *SQSServer) Shutdown(maxWait time.Duration) {
	// Run the context cancel function
	s.stopPolling()
	// The queue pollers will most likely be in the middle of a long poll
	// Need to cancel their requests
	s.mu.Lock()
	for _, req := range s.currentPollRequests {
		if t, ok := req.Config.HTTPClient.Transport.(*http.Transport); ok {
			t.CancelRequest(req.HTTPRequest)
		}
	}
	s.mu.Unlock()
	s.queuePollers.Wait()
	kill := time.After(maxWait)
	done := make(chan struct{})
	go func() {
		s.tasks.Wait()
		done <- struct{}{}
	}()
	for {
		select {
		case <-kill:
			s.stopTasks()
		case <-done:
			close(done)
			return
		}
	}
}

// pollQueues kicks off a goroutines for polling the specified queues
func (s *SQSServer) pollQueues(pollctx, taskctx context.Context, queueNames []string) error {
	for _, name := range queueNames {
		q, err := s.getQueue(name)
		if err != nil {
			return err
		}
		req := &sqs.GetQueueAttributesInput{
			AttributeNames: []*string{aws.String("VisibilityTimeout")},
			QueueUrl:       &q.url,
		}
		resp, err := s.serv.GetQueueAttributes(req)
		if err != nil {
			return fmt.Errorf("Failed to get queue attributes for '%s' - %s", name, err.Error())
		}
		to := resp.Attributes["VisibilityTimeout"]
		if to == nil {
			return fmt.Errorf("No visibility timeout returned by SQS for queue '%s'", name)
		}
		visTimeout, err := strconv.Atoi(*to)
		if err != nil {
			return fmt.Errorf("Failed to convert visibility timeout from '%s' to int - '%s'", *to, err.Error())
		}
		// Each queue runs in a dedicated go routine.
		go func(vt int64) {
			s.queuePollers.Add(1)
			defer s.queuePollers.Done()
			s.run(pollctx, taskctx, q, vt)
		}(int64(visTimeout))
	}

	return nil
}

// run will poll a single queue and handle arriving messages
func (s *SQSServer) run(pollctx, taskctx context.Context, q queue, visibilityTimeout int64) {
	failAttempts := 0
	backoff := time.Duration(0)
	for {
		select {
		case <-pollctx.Done():
			return
		default:
			if backoff.Seconds() > 0 {
				time.Sleep(backoff)
			}
			// Receive only one message at a time to ensure job load is spread over all machines
			reqInput := &sqs.ReceiveMessageInput{
				QueueUrl:              &q.url,
				MaxNumberOfMessages:   aws.Int64(q.batchSize),
				WaitTimeSeconds:       aws.Int64(20),
				AttributeNames:        q.attributesToReturn,
				MessageAttributeNames: q.attributesToReturn,
			}
			req, resp := s.serv.ReceiveMessageRequest(reqInput)
			// Mark current polling request
			s.mu.Lock()
			s.currentPollRequests[q.name] = req
			s.mu.Unlock()
			err := req.Send()
			if err != nil {
				failAttempts++
				if failAttempts > 20 {
					panic(fmt.Sprintf(" %s - Failed to poll for too long. Panicing (not picnicing).", q.name))
				}
				backoff = time.Duration(math.Min(float64(failAttempts*20), 120))
				continue
			} else {
				backoff = time.Duration(0)
				failAttempts = 0
			}
			for i := range resp.Messages {
				msg := resp.Messages[i]
				// Run each request/message in its own goroutine
				go s.serveMessage(taskctx, q, msg, visibilityTimeout)
			}
		}
	}
}

func (s *SQSServer) serveMessage(ctx context.Context, q queue, m *sqs.Message, visibilityTimeout int64) {
	s.tasks.Add(1)
	defer s.tasks.Done()
	headers := http.Header{}

	// SQS Specific attributes are mapped as X-Amzn-*
	for k, attr := range m.Attributes {
		headers.Set(fmt.Sprintf("X-Amzn-%s", k), *attr)
	}
	path := fmt.Sprintf("/%s", q.name)

	for k, attr := range m.MessageAttributes {
		if k == "Path" {
			path = fmt.Sprintf("%s/%s", path, *attr.StringValue)
			continue
		}
		if *(attr.DataType) != "Binary" {
			headers.Set(k, *attr.StringValue)
		}
	}
	headers.Set("Content-MD5", *m.MD5OfBody)
	headers.Set("X-Amzn-MessageID", *m.MessageId)

	url, _ := url.Parse(path)
	s.logf("Serving for path %s", url)
	req := &http.Request{
		URL:    url,
		Method: "POST",
		Header: headers,
		Body:   ioutil.NopCloser(strings.NewReader(*m.Body)),
	}

	done := make(chan struct{})
	w := &writer{}
	go func() {
		s.Handler.ServeHTTP(w, req)
		done <- struct{}{}
	}()
	hbeat := time.Second * time.Duration(float64(visibilityTimeout)*0.9)
	for {
		select {
		case <-ctx.Done():
			close(done)
			return
		case <-time.After(hbeat):
			err := s.heartbeat(q, m, visibilityTimeout)
			if err != nil {
				s.logf("Heartbeat failed - %s:%s - Cause '%s'", q.name, *m.MessageId, err.Error())
			}
		case <-done:
			if w.status >= 200 && w.status < 300 {
				err := s.ack(q, m)
				if err != nil {
					s.logf("ACK Failed %s:%s - Cause '%s'", q.name, *m.MessageId, err.Error())
				}
			}
			close(done)
			return
		}
	}
}

func (s *SQSServer) getQueue(queueName string) (queue, error) {
	req := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	url, err := s.serv.GetQueueUrl(req)
	if err != nil {
		return queue{}, fmt.Errorf("Failed to get queue %s - '%s'", queueName, err.Error())
	}
	return queue{
		name:               queueName,
		url:                *url.QueueUrl,
		attributesToReturn: []*string{aws.String("All")},
		batchSize:          1,
	}, nil
}

func (s *SQSServer) ack(q queue, m *sqs.Message) error {
	req := &sqs.DeleteMessageInput{
		QueueUrl:      &q.url,
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err := s.serv.DeleteMessage(req)
	return err
}

func (s *SQSServer) heartbeat(q queue, m *sqs.Message, visibilityTimeout int64) error {
	req := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &q.url,
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: aws.Int64(visibilityTimeout),
	}
	_, err := s.serv.ChangeMessageVisibility(req)
	return err
}

func (s *SQSServer) logf(msg string, args ...interface{}) {
	if s.ErrorLog != nil {
		s.ErrorLog.Printf(msg, args...)
	} else {
		log.Printf(msg, args)
	}
}
func (w *writer) Header() http.Header {
	return http.Header{}
}

func (w *writer) Write(b []byte) (int, error) {
	return len(b), nil
}

func (w *writer) WriteHeader(status int) {
	w.status = status
}
