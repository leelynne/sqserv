/*
Package sqserv provides a net/http style interface for handling AWS SQS messages.  http.Request objects are generated from SQS messages.  Using the net/http interface allows for reuse of http middleware and other related utilities.

Path Matching

The url of the generated http.Request is set to the name of the SQS queue. For example, with a SQS queue named 'worker' the url will be '/worker'.  Therefore your http.Handler needs to handle '/worker' requests as follows.

    mux.Handle("/worker", workerHandler)

To allow more than one handler per queue you can add the 'Path' Message Attribute to your SQS messages.  The Path attribute is appended to the queue name to form the url.  For example, by appending Path = 'highpri'  or Path = 'lowpri' to your SQS messages allows multiple handlers as follows:

    mux.Handle("/worker", genericHandler)
    mux.Handle("/worker/highpri", highHandler)
    mux.Handle("/worker/lowpri", lowHandler)

SQS and Message Attributes

SQS Attributes (MD5 digest, message id, etc) are converted into http headers prefixed with 'X-Amzn-'. For example 'ApproximateFirstReceiveTimestamp' is converted to the header 'X-Amzn-ApproximateFirstReceiveTimestamp'.

Additionally, the Content-MD5 header is set as the SQS body MD5

All non-binary message attributes are converted directly into HTTP Headers. Note that the net/http header standardizes
header names. For example an attribute named 'log-path' will be standardized to 'Log-Path'. See net/http docuement for standardization details.

Polling and goroutines

This package will long poll SQS with a configurable read batch size.  Each SQS message is invoked in its own goroutine.

Heartbeats

If message processing time starts to get close to the SQS visibility timeout, this package will automatically extend the visibility timeout (aka heartbeat) to allow work to continue. This allows SQS queues to have tight timeouts.


Leaky Abstraction

Forcing SQS messages into http.Request provides a lot of benefits but does have some caveats.

You must always provide an http response code or else the processing goroutine runs forever and the SQS message is never acked.

SQS doesn't support 'nacks'.  So when processing results in a failure, the SQS message doesn't get redriven until it times out.

*/
package sqserv
