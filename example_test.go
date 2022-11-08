package sqserv_test

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/leelynne/sqserv/v2"
)

func Example_basic() {
	// For SQS queue name 'message-queue'
	queueHandler := func(w http.ResponseWriter, req *http.Request) {
		// Handle as a you would an http request

		// Ack the message by returning a success code
		w.WriteHeader(http.StatusNoContent)
	}

	// Use any http.Handler compatible router/middleware
	mux := http.NewServeMux()
	mux.Handle("/message-queue", http.HandlerFunc(queueHandler))

	conf := aws.Config{}
	qsrv, err := sqserv.New(conf, mux)
	if err != nil {
		log.Fatal(err)
	}

	// non-blocking call to start polling for SQS messages
	err = qsrv.ListenAndServe("message-queue")
	if err == nil {
		log.Fatal(err)
	}

	// Block until shutdown
	ch := make(chan os.Signal)
	// Blocks until a signal is received
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Shutting down queue listener.")
	qsrv.Shutdown(2 * time.Second)
}

func Example_withConf() {
	// For SQS queue name 'message-queue'
	queueHandler := func(w http.ResponseWriter, req *http.Request) {
		// Handle as a you would an http request

		// Ack the message by returning a success code
		w.WriteHeader(http.StatusNoContent)
	}

	// Use any http.Handler compatible router/middleware
	mux := http.NewServeMux()
	mux.Handle("/worker-virgina", http.HandlerFunc(queueHandler))
	mux.Handle("/worker-oregon", http.HandlerFunc(queueHandler))

	conf := aws.Config{}
	qsrv, err := sqserv.New(conf, mux)
	if err != nil {
		log.Fatal(err)
	}

	// Listen to queues from two region
	queues := []sqserv.QueueConf{
		{
			Name:   "workers-virginia",
			Region: "us-east-1",
		},
		{
			Name:   "workers-oregon",
			Region: "us-west-2",
		},
	}

	// non-blocking call to start polling for SQS messages
	err = qsrv.ListenAndServeQueues(queues...)
	if err == nil {
		log.Fatal(err)
	}

	// Block until shutdown
	ch := make(chan os.Signal)
	// Blocks until a signal is received
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Shutting down queue listener.")
	qsrv.Shutdown(2 * time.Second)
}

func Example_metrics() {
	metricHandler := func(mtype sqserv.MetricType, val float64, inflight int) {
		switch mtype {
		case sqserv.MetricAck:
			log.Printf("ACK: Inflight %d", inflight)
		case sqserv.MetricNack:
			log.Printf("NACK: Inflight %d", inflight)
		case sqserv.MetricHeartBeat:
			log.Printf("Heartbeat: Inflight %d", inflight)
		case sqserv.MetricPollFailure:
			log.Printf("Poll Failure: Inflight %d", inflight)
		case sqserv.MetricReceive:
			log.Printf("Received %f: Inflight %d", val, inflight)
		case sqserv.MetricShutdown:
			log.Printf("Shutdown: Inflight %d", inflight)
		}
	}

	// For SQS queue name 'message-queue'
	queueHandler := func(w http.ResponseWriter, req *http.Request) {
		// Handle as a you would an http request

		// Ack the message by returning a success code
		w.WriteHeader(http.StatusNoContent)
	}

	// Use any http.Handler compatible router/middleware
	mux := http.NewServeMux()
	mux.Handle("/worker", http.HandlerFunc(queueHandler))

	conf := aws.Config{}
	qsrv, err := sqserv.New(conf, mux)
	if err != nil {
		log.Fatal(err)
	}

	// Listen to queues from two region
	qconf := sqserv.QueueConf{
		Name:    "workers",
		Metrics: metricHandler,
	}

	// non-blocking call to start polling for SQS messages
	err = qsrv.ListenAndServeQueues(qconf)
	if err == nil {
		log.Fatal(err)
	}

	// Block until shutdown
	ch := make(chan os.Signal)
	// Blocks until a signal is received
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Shutting down queue listener.")
	qsrv.Shutdown(2 * time.Second)
}

func ExampleNew() {
	conf := aws.Config{}
	qsrv, err := sqserv.New(conf, nil)
	if err != nil {
		log.Fatal(err)
	}
	qsrv.ListenAndServe("myqueue")
}

func ExampleSQSServer_Shutdown() {
	conf := aws.Config{}
	qsrv, err := sqserv.New(conf, nil)
	if err != nil {
		log.Fatal(err)
	}

	// non-blocking call to start polling for SQS messages
	err = qsrv.ListenAndServe("message-queue")
	if err == nil {
		log.Fatal(err)
	}

	// Block until shutdown
	ch := make(chan os.Signal)
	// Blocks until a signal is received
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Shutting down queue listener.")
	qsrv.Shutdown(2 * time.Second)
}
