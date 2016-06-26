package sqserv

import "testing"
import "github.com/aws/aws-sdk-go/aws"

func TestZeroQueues(t *testing.T) {
	conf := &aws.Config{}
	s, err := New(conf.WithRegion("us-west-2"), nil)
	if err != nil {
		t.Error(err)
	}

	err = s.ListenAndServe()
	if err == nil {
		t.Error("Expected an error to be generated for not passing any queues")
	}
}
