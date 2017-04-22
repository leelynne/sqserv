# sqserv

[![GoDoc](https://godoc.org/github.com/leelynne/sqserv?status.svg)](https://godoc.org/github.com/leelynne/sqserv)


Package sqserv provides a net/http style interface for handling AWS SQS messages.  http.Request objects are generated from SQS messages then handled with standard http.Handler methods.

### Why?
Using the net/http interface allows for handy reuse of http middleware and other related utilities.

### Basic Example

``` go
    queueHandler := func(w http.ResponseWriter, req *http.Request) {
		// Handle as a you would an http request

		// Ack the message by returning a success code
		w.WriteHeader(http.StatusNoContent)
	}
        // For SQS queue name 'message-queue'
	// Use any http.Handler compatible router/middleware
	mux := http.NewServeMux()
	mux.Handle("/message-queue", http.HandlerFunc(queueHandler))

	conf := &aws.Config{}
	qsrv, err := sqserv.New(conf.WithRegion("us-west-2"), mux)
	if err != nil {
		log.Fatal(err)
	}

	// non-blocking call to start polling for SQS messages
	err = qsrv.ListenAndServe("message-queue")
	if err == nil {
		log.Fatal(err)
	}
```

[The package doc](https://godoc.org/github.com/leelynne/sqserv) contains more examples and implementation details.


## Documentation
[godoc](https://godoc.org/github.com/leelynne/sqserv) contains more examples and implementation details.
