package sqserv

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
