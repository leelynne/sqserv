package sqserv

import (
	"testing"

	"github.com/awslabs/aws-sdk-go/aws"
)

func TestZeroQueues(t *testing.T) {
	conf := &aws.Config{}
	s, err := New(conf, nil)
	if err != nil {
		t.Error(err)
	}

	err = s.ListenAndServe()
	if err == nil {
		t.Error("Expected an error to be generated for not passing any queues")
	}
}
