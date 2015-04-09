// +build integ

package sqserv

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"sync"
	"testing"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
	"github.com/talio/itt"
)

// TestSQS is a generic test of happy case processing.
func TestSQS(t *testing.T) {
	c := itt.WithContainerCfgs(t, itt.Container{
		Name:        "talio/docker-fakesqs",
		Delay:       time.Duration(time.Millisecond * 3000),
		RandomPorts: false,
	})

	defer c.Close()
	port := c.PortMappings["9324"]
	aws.Regions["testregion"] = aws.Region{SQSEndpoint: fmt.Sprintf("http://localhost:%s", port)}
	s := sqs.New(aws.Auth{AccessKey: "blah", SecretKey: "blah"}, aws.Regions["testregion"])
	createQueue(c, "testqueue1", s)
	createQueue(c, "testqueue2", s)

	mu := sync.Mutex{}
	found := make(map[string]string)

	updateFound := func(r *Request, status string) error {
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			c.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		found[string(msg)] = status
		return nil
	}
	foundHandler := func(r *Request) error {
		return updateFound(r, "found")
	}
	notfoundHandler := func(r *Request) error {
		return updateFound(r, "notfound")
	}

	mux := QueueMux{
		NotFoundHandler: HandlerFunc(notfoundHandler),
		patterns:        make(map[string]Handler),
	}
	mux.HandleFunc("testqueue1", foundHandler)
	srv, err := NewSQSServer(&mux, aws.Auth{AccessKey: "blah", SecretKey: "blah"}, "testregion", log.New(twriter{t}, "UNIT-", 0))
	if err != nil {
		c.Fatal(err)
	}

	err = srv.ListenAndServe("testqueue1", "testqueue2")
	if err != nil {
		c.Fatal(err)
	}
	sendMsg(c, s, "testqueue1", "1")
	sendMsg(c, s, "testqueue1", "2")
	sendMsg(c, s, "testqueue2", "3")
	time.Sleep(time.Second * 2)
	mu.Lock()
	defer mu.Unlock()
	if found["1"] != "found" {
		c.Fatalf("Message with body 1 not processed")
	}
	if found["2"] != "found" {
		c.Fatalf("Message with body 2 not processed")
	}

	if found["3"] != "notfound" {
		c.Fatalf("Message with body 3 not processed with notfound handler")
	}
	srv.Shutdown()
}

func createQueue(c *itt.Manager, queue string, s *sqs.SQS) {
	q, err := s.CreateQueue(queue)
	if err != nil {
		c.Fatal(err)
	}
	q.SetQueueAttributes(map[string]string{"VisibilityTimeout": "1"})
	if err != nil {
		c.Fatal(err)
	}
}

func sendMsg(c *itt.Manager, s *sqs.SQS, queue string, msg string) {
	q, err := s.GetQueue(queue)
	if err != nil {
		c.Fatal(err)
	}

	_, err = q.SendMessage(msg)
	if err != nil {
		c.Fatal(err)
	}
}

type twriter struct {
	t *testing.T
}

func (tw twriter) Write(p []byte) (int, error) {
	d := string(p)
	tw.t.Log(d)
	return len(d), nil
}

type httpLogger struct {
	impl http.RoundTripper
}

func (h *httpLogger) RoundTrip(r *http.Request) (*http.Response, error) {
	reqout, err := httputil.DumpRequestOut(r, true)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(reqout))
	resp, err := h.impl.RoundTrip(r)
	respout, err := httputil.DumpResponse(resp, true)
	if err != nil {
		return resp, err
	}
	fmt.Println(string(respout))
	return resp, err
}
