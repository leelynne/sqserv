package sqserv

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"golang.org/x/net/context"
)

// MetricType lists each action that generates a MetricCallback
type MetricType int

const (
	defaultReadBatchSize            = 1    // Size one ensures best load balancing between multiple SQS listeners.
	MetricAck            MetricType = iota // Called after message processing success response. val is always 1.
	MetricNack                             // Called after message processing failure response. val is always 1. Note that SQS does not have nacks.
	MetricHeartBeat                        // Called when a heartbeat/extend vibisility timeout was triggered. val is always 1.
	MetricPollFailure                      // Called after failure of an SQS long poll request. val is always 1.
	MetricReceive                          // Called after a successful batch message receive. val is the number of messages received in the batch.
	MetricShutdown                         // Called when the the Shutdown method is invoked. val is always 1.
)

// SQSServer handles SQS messages in a similar fashion to http.Server
type SQSServer struct {
	// ErrorLog specifies an optional logger for message related
	// errors. If nil, logging goes to os.Stderr
	ErrorLog       *log.Logger
	Handler        http.Handler
	defaultAWSConf aws.Config
	defaultRegion  string

	stopPolling  func()
	stopTasks    func()
	tasks        sync.WaitGroup
	queuePollers sync.WaitGroup

	mu          sync.Mutex
	srvByRegion map[string]*sqs.Client
}

// MetricCallback provides telemetry for library users. 'val' is specific to the MetricType. 'inflight' is the current count of all messages received but not yet processed.
type MetricCallback func(m MetricType, val float64, inflight int)

// QueueConf allows for details queue configuration
type QueueConf struct {
	Name      string
	Region    string // Region will override the region in the aws.Config passed to New()
	ReadBatch uint   // Size of read batch. Defaults to maximum allows by SQS.
	Metrics   MetricCallback
}

type writer struct {
	status int
}

type queue struct {
	QueueConf
	url                string
	attributesToReturn []string
	inprocess          int32
}

// New creates a new SQSServer. If Handler is nil http.DefaultServeMux is used.
// Must specify a region in conf that will be the default queue region.
func New(conf aws.Config, h http.Handler) (*SQSServer, error) {
	if conf.Region == "" {
		return nil, fmt.Errorf("Must specify default region in aws.Config")
	}
	if h == nil {
		h = http.DefaultServeMux
	}
	if conf.HTTPClient == nil {
		// For backwards compatibility, set this http client if one is not already set.
		conf.HTTPClient = &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 3 * time.Second,
				}).DialContext,
			},
		}
	}

	return &SQSServer{
			Handler:        h,
			defaultAWSConf: conf,
			defaultRegion:  conf.Region,
			srvByRegion:    map[string]*sqs.Client{},
			queuePollers:   sync.WaitGroup{},
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
	qconfs := make([]QueueConf, len(queues))
	for i := range queues {
		qconfs[i].Name = queues[i]
		qconfs[i].Region = s.defaultRegion
		qconfs[i].ReadBatch = defaultReadBatchSize
		qconfs[i].Metrics = func(MetricType, float64, int) {}
	}

	return s.pollQueues(pollctx, taskctx, qconfs)
}

// ListenAndServeQueues begins polling SQS without blocking.
func (s *SQSServer) ListenAndServeQueues(queues ...QueueConf) error {
	if len(queues) == 0 {
		return fmt.Errorf("Must specify at least one SQS queue to poll")
	}
	pollctx, pollcancel := context.WithCancel(context.Background())
	taskctx, taskcancel := context.WithCancel(context.Background())
	s.stopPolling = pollcancel
	s.stopTasks = taskcancel
	for i := range queues {
		if queues[i].Name == "" {
			return fmt.Errorf("Queue configuration must have a Name")
		}
		if queues[i].Region == "" {
			queues[i].Region = s.defaultRegion
		}
		if queues[i].ReadBatch == 0 {
			queues[i].ReadBatch = defaultReadBatchSize
		}
		if queues[i].Metrics == nil {
			queues[i].Metrics = func(MetricType, float64, int) {}
		}
	}
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
func (s *SQSServer) pollQueues(pollctx, taskctx context.Context, queues []QueueConf) error {
	for _, qconf := range queues {
		q, err := s.getQueue(pollctx, qconf)
		if err != nil {
			return err
		}
		req := &sqs.GetQueueAttributesInput{
			AttributeNames: []types.QueueAttributeName{("VisibilityTimeout")},
			QueueUrl:       &q.url,
		}
		resp, err := s.sqsSrv(q.QueueConf).GetQueueAttributes(pollctx, req)
		if err != nil {
			return fmt.Errorf("Failed to get queue attributes for '%s' - %s", q.Name, err.Error())
		}
		to := resp.Attributes["VisibilityTimeout"]
		if to == "" {
			return fmt.Errorf("No visibility timeout returned by SQS for queue '%s'", q.Name)
		}
		visTimeout, err := strconv.Atoi(to)
		if err != nil {
			return fmt.Errorf("Failed to convert visibility timeout from '%s' to int - '%s'", to, err.Error())
		}
		// Each queue runs in a dedicated go routine.
		go func(vt int32) {
			s.queuePollers.Add(1)
			defer s.queuePollers.Done()
			s.run(pollctx, taskctx, q, vt)
		}(int32(visTimeout))
	}

	return nil
}

// run will poll a single queue and handle arriving messages
func (s *SQSServer) run(pollctx, taskctx context.Context, q *queue, visibilityTimeout int32) {
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

			attributeNames := []types.QueueAttributeName{}
			for _, attr := range q.attributesToReturn {
				attributeNames = append(attributeNames, types.QueueAttributeName(attr))
			}
			// Receive only one message at a time to ensure job load is spread over all machines
			reqInput := &sqs.ReceiveMessageInput{
				QueueUrl:              &q.url,
				MaxNumberOfMessages:   int32(q.ReadBatch),
				WaitTimeSeconds:       20,
				AttributeNames:        attributeNames,
				MessageAttributeNames: q.attributesToReturn,
			}
			resp, err := s.sqsSrv(q.QueueConf).ReceiveMessage(pollctx, reqInput)
			if err != nil {
				q.Metrics(MetricPollFailure, 1, int(atomic.LoadInt32(&q.inprocess)))
				failAttempts++
				if failAttempts > 20 {
					panic(fmt.Sprintf(" %s - Failed to poll for too long. Panicing (not picnicing).", q.Name))
				}
				backoff = time.Duration(math.Min(float64(failAttempts*20), 120))
				continue
			} else {
				backoff = time.Duration(0)
				failAttempts = 0
			}
			start := time.Now()
			inflight := atomic.AddInt32(&q.inprocess, int32(len(resp.Messages)))
			q.Metrics(MetricReceive, float64(len(resp.Messages)), int(inflight))
			ctxWithTime := context.WithValue(taskctx, "start", start)
			for i := range resp.Messages {
				msg := resp.Messages[i]
				// Run each request/message in its own goroutine
				go s.serveMessage(ctxWithTime, q, msg, visibilityTimeout)
			}
		}
	}
}

func (s *SQSServer) serveMessage(ctx context.Context, q *queue, m types.Message, visibilityTimeout int32) {
	s.tasks.Add(1)
	defer s.tasks.Done()
	defer atomic.AddInt32(&q.inprocess, -1)
	start := ctx.Value("start").(time.Time)
	headers := http.Header{}

	// SQS Specific attributes are mapped as X-Amzn-*
	for k, attr := range m.Attributes {
		headers.Set(fmt.Sprintf("X-Amzn-%s", k), attr)
	}
	path := fmt.Sprintf("/%s", q.Name)

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

	req := &http.Request{
		URL:    url,
		Method: "POST",
		Header: headers,
		Body:   ioutil.NopCloser(strings.NewReader(*m.Body)),
	}
	req = req.WithContext(ctx)

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
			q.Metrics(MetricAck, 1, int(atomic.LoadInt32(&q.inprocess)))
			close(done)
			return
		case <-time.After(hbeat):
			err := s.heartbeat(ctx, q, m, visibilityTimeout)
			if err != nil {
				s.logf("Heartbeat failed - %s:%s - Cause '%s'", q.Name, *m.MessageId, err.Error())
			} else {
				q.Metrics(MetricHeartBeat, durationMillis(start), int(atomic.LoadInt32(&q.inprocess)))
			}
		case <-done:
			if w.status >= 200 && w.status < 300 {
				err := s.ack(ctx, q, m)
				if err != nil {
					s.logf("ACK Failed %s:%s - Cause '%s'", q.Name, *m.MessageId, err.Error())
					q.Metrics(MetricNack, durationMillis(start), int(atomic.LoadInt32(&q.inprocess)))
				} else {
					q.Metrics(MetricAck, durationMillis(start), int(atomic.LoadInt32(&q.inprocess)))
				}
			} else {
				q.Metrics(MetricNack, durationMillis(start), int(atomic.LoadInt32(&q.inprocess)))
			}
			close(done)
			return
		}
	}
}

func (s *SQSServer) getQueue(ctx context.Context, q QueueConf) (*queue, error) {
	req := &sqs.GetQueueUrlInput{
		QueueName: &q.Name,
	}
	url, err := s.sqsSrv(q).GetQueueUrl(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Failed to get queue %s - '%s'", q.Name, err.Error())
	}
	return &queue{
		QueueConf:          q,
		url:                *url.QueueUrl,
		attributesToReturn: []string{"All"},
	}, nil
}

func (s *SQSServer) ack(ctx context.Context, q *queue, m types.Message) error {
	req := &sqs.DeleteMessageInput{
		QueueUrl:      &q.url,
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err := s.sqsSrv(q.QueueConf).DeleteMessage(ctx, req)
	return err
}

func (s *SQSServer) heartbeat(ctx context.Context, q *queue, m types.Message, visibilityTimeout int32) error {
	req := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &q.url,
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: visibilityTimeout,
	}
	_, err := s.sqsSrv(q.QueueConf).ChangeMessageVisibility(ctx, req)
	return err
}

func (s *SQSServer) sqsSrv(q QueueConf) *sqs.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srvByRegion[q.Region] == nil {
		aconf := aws.Config{
			Region: q.Region,
		}
		s.srvByRegion[q.Region] = sqs.NewFromConfig(aconf)
	}
	return s.srvByRegion[q.Region]
}

func (s *SQSServer) logf(msg string, args ...interface{}) {
	if s.ErrorLog != nil {
		s.ErrorLog.Printf(msg, args...)
	} else {
		log.Printf(msg, args...)
	}
}

func durationMillis(t time.Time) float64 {
	return float64((time.Since(t) / time.Millisecond))
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
