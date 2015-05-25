package amqputil

import (
	"fmt"
	"github.com/jbwarren/syncutil"
	"github.com/streadway/amqp"
	"time"
)

/////////////////////////////////////////////////
// management
/////////////////////////////////////////////////

// used for reliable amqp publishing (i.e., channel in Confirm mode, handling acks/nacks from server)
// NOTE:  this object is not intended to be safe for use across goroutines
type ReliablePublisher struct {
	conn    *Connection // nil if not connected
	running *rpRunning  // nil if not running
	result  *rpResult   // set by Wait() after publisher exits
}

// connects to the amqp server at <url> and returns a new ReliablePublisher object
func NewReliablePublisher(url string) (rp *ReliablePublisher, err error) {
	options := &ChannelOptions{GetClosings: true, ConfirmPubs: true, ConfirmCapacity: 1000} // only what we need: closings & acks/nacks
	conn, err := Dial(url, nil, options)
	if err != nil {
		return
	}
	rp = &ReliablePublisher{conn: conn}
	return
}

// returns a new channel on the Publisher's connection
// can be used to configure exchanges/queues/... prior to running the Publisher
// caller must call Close() on the returned channel
func (me *ReliablePublisher) NewChannel(options *ChannelOptions) (ch *Channel, err error) {
	if me.conn == nil {
		err = fmt.Errorf("ReliablePublisher.NewChannel():  connection not initialized")
		return
	}
	return me.conn.NewChannel(options)
}

// returns a set of (golang) channels for out-of-band events on the publisher's connection/channel
// can be used to watch for and respond to detailed events/errors the publisher doesn't handle (e.g., returns)
// beware registering for publish confirm events (ack/nack), as the message id is not reset
func (me *ReliablePublisher) RegisterForEvents(connopts *ConnectionOptions, chanopts *ChannelOptions) (connevents *ConnectionEvents, chanevents *ChannelEvents, err error) {
	if me.conn == nil {
		err = fmt.Errorf("ReliablePublisher.RegisterForEvents():  connection not initialized")
		return
	}
	return me.conn.RegisterForEvents(connopts, chanopts)
}

// signals the publisher to quit
func (me *ReliablePublisher) Quit() (err error) {
	if me.conn == nil {
		return fmt.Errorf("ReliablePublisher.Quit():  connection not initialized")
	}
	if me.running == nil {
		return fmt.Errorf("ReliablePublisher.Quit():  not running")
	}

	// close quit channel (signal publisher to quit), if haven't already
	if me.running.quit_ch != nil { // first time?
		close(me.running.quit_ch)
		me.running.quit_ch = nil // not safe for concurrency
	}
	return nil
}

// waits for the publisher to exit (return a result)
// waits forever if timeout < 0
// the publisher will exit once its input closes or after Quit() is called
// returns ok=true if publisher has exited; also sets publisher result and calls Close()
// returns ok=false on timeout
// not safe to call concurrently - use WaitSignal() for concurrent waiting
//		afterwards, Wait() will return immediately (still do not call concurrently)
func (me *ReliablePublisher) Wait(timeout time.Duration) (ok bool) {
	if me.running == nil { // not running?
		return true
	}

	// wait, with timeout
	var (
		result *rpResult
		more   bool
	)
	switch {
	case timeout >= 0: // wait with timeout
		select {
		case <-time.After(timeout): // timeout
			return false
		case result, more = <-me.running.result_ch: // exited (continue below)
		}
	case timeout < 0: // wait forever
		result, more = <-me.running.result_ch
	}

	// got a result?
	if more { // otherwise, already got one (shouldn't happen)
		me.result = result
	}
	me.running = nil // not safe for concurrency
	me.Close(0)
	return true
}

// returns a channel that will be closed when the publisher exits
// client must still call Wait() afterwards to collect publisher results (will return immediately)
// returns nil if the publisher is known to not be running
func (me *ReliablePublisher) WaitSignal() (done <-chan int) {
	if me.running == nil {
		return nil
	}
	return me.running.wait_ch
}

// gets error result after exit
// otherwise returns nil
// <outstanding> contains messages not acked/nacked by the server prior to exit, keyed by message id
//		should only happen if exiting with error
func (me *ReliablePublisher) GetError() (err *amqp.Error, outstanding map[uint64]*PublishRequest) {
	if me.result == nil {
		return
	}
	return me.result.Error, me.result.Outstanding
}

// cleans up resources, including closing the connection
// any secondary channels created with NewChannel() will be closed as well
// must be called if Run() is never called
// otherwise calls Quit() and waits for publisher to exit, with <timeout>
// returns ok=false on timeout
func (me *ReliablePublisher) Close(timeout time.Duration) (ok bool) {
	if me.conn == nil {
		return true // already closed (or never initialized)
	}
	switch me.running != nil { // running?
	case true:
		me.Quit()
		return me.Wait(timeout) // calls Close() if successful
	case false:
		me.conn.Close()
		me.conn = nil
	}
	return true
}

///////////////////////////////////////////////////
// running
///////////////////////////////////////////////////

const QUIT_TIMEOUT time.Duration = 15 * time.Second

// stores stuff needed for running, by both RP struct & publisher goroutine
// two copies of the struct are maintained - one for each
// initialize with .initialize()
type rpRunning struct {
	// signaling
	quit_ch   chan int       // signals publisher to quit; nil in RP struct if already signaled to quit (so we don't close twice)
	wait_ch   chan int       // closed when publisher has exited (syncutil.Waiter.WaitChannel()); can be passed to client for use in select
	result_ch chan *rpResult // publisher pushes a single result upon exit, then closes

	// inputs
	requests <-chan *PublishRequest

	// outputs
	acked  chan *PublishResult
	nacked chan *PublishResult // also includes Publish() errors
}

// stores stuff returned from a publisher upon completion
// initialize with .initialize()
type rpResult struct {
	Error       *amqp.Error                // nil if exits without error
	Outstanding map[uint64]*PublishRequest // messages not acked/nacked prior to exit (on error);  key=msgid
}

// encapsulates a call to amqp.Publish()
type PublishRequest struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Message   *amqp.Publishing
}

// encapsulates an ack/nack or error from amqp.Publish()
type PublishResult struct {
	RequestNum uint64
	Error      error
	Request    *PublishRequest
}

func (me *ReliablePublisher) Run(requests <-chan *PublishRequest) (acked, nacked <-chan *PublishResult, err error) {
	if me.conn == nil {
		return nil, nil, fmt.Errorf("ReliablePublisher.Run():  connection not initialized")
	}
	if me.running != nil {
		return nil, nil, fmt.Errorf("ReliablePublisher.Run():  already running")
	}

	// initialize
	running := &rpRunning{}
	running.initialize(requests)
	wait := &syncutil.Waiter{} // we'll initialize running.wait_ch with wait.WaitChannel() after starting the goroutine

	// start goroutine
	wait.Add(1)                                                 // he who goes, adds (first)
	go reliablePublisher(me.conn.channel, running.copy(), wait) // give routine its own copy of stuff
	running.wait_ch = wait.WaitChannel()                        // must come after the Add()

	// initialize RP object
	me.running = running
	return running.acked, running.nacked, nil
}

// publishes msgs read from params.requests, handling acks/nacks
// outputs msg results to params.acked/nacked
// quits when params.requests is closed, or when params.quit_ch is signaled
// closes params.acked/nacked on exit
// pushes overall result on params.result_ch and signals <done> on exit
func reliablePublisher(ch *Channel, params *rpRunning, done *syncutil.Waiter) {
	defer done.Done()             // signal on exit
	defer close(params.result_ch) // result will be pushed first (see below)
	defer close(params.acked)
	defer close(params.nacked)

	// set up result structure; push on exit
	result := &rpResult{}
	result.initialize()
	defer func() { params.result_ch <- result }() // will be called before done.Done()

	// for managing actual publisher routine (makes blocking Publish() calls)
	// lock-step signaling (req/rep pattern)
	topublish := make(chan *PublishRequest) // capacity=0 so we're never queuing requests internally
	pubresults := make(chan error, 1)       // capacity=1 so we can queue one response (so publisher can always exit)
	defer close(topublish)                  // will signal routine to stop

	// define raw publisher routine (because Publish() may block)
	// reads input from <topublish>, writes results to <pubresults>
	// reads until <topublish> closed, closes <pubresults> on exit
	publisher := func() {
		defer close(pubresults) // close when done
		for p := range topublish {
			// Publish(exchange, key, mandatory, immediate, msg)
			pubresults <- ch.Publish(p.Exchange, p.Key, p.Mandatory, p.Immediate, *p.Message)
		}
	}
	go publisher()

	// for managing event loop
	lastseqnum := uint64(0)           // first is 1
	outstanding := result.Outstanding // keyed on sequence number (automatically update result)
	ready := true                     // publish routine ready?
	quitting := false                 // not yet
	var quit_timer <-chan time.Time   // will be set when quitting

	// helper functions
	handleResult := func(num uint64, output chan *PublishResult, err error, label string) { // got an ack/nack/error back from publisher
		req, ok := outstanding[num] // get request
		if !ok {                    // not found
			fmt.Printf("ReliablePublisher():  ERROR:  received unexpected %s:  %v\n", label, num)
		}
		delete(outstanding, num)                // remove it from the map
		output <- &PublishResult{num, err, req} // send it to output channel
	}
	startToQuit := func() {
		quitting = true                       // continue looping to handle remaining acks/nacks
		quit_timer = time.After(QUIT_TIMEOUT) // set quit timer
		if len(outstanding) > 0 {
			fmt.Printf("ReliablePublisher():  handling acks for %v outstanding msgs before exiting\n", len(outstanding))
		}
	}

	// loop, publishing messages & handling events
loop:
	for {
		if quitting && len(outstanding) == 0 { // no more acks/nacks to process?
			break loop
		}

		select { // check for events

		// quit timer expired?
		case <-quit_timer:
			break loop

		// quit signaled?
		case <-params.quit_ch:
			fmt.Println("ReliablePublisher():  got quit signal")
			startToQuit()

		// connection or channel exception?
		case result.Error = <-ch.CloseEvents(): // if so, set result error code
			fmt.Printf("ReliablePublisher():  got connection/channel exception: %v\n", result.Error)
			break loop

		// got an ack (or ack channel closed)?
		case acknum, ok := <-ch.AckEvents():
			if !ok { // ack channel closed (probably resulting from channel close exception)
				fmt.Println("ReliablePublisher():  ack channel closed unexpectedly")
				break loop
			}
			handleResult(acknum, params.acked, nil, "ack") // handle ack

		// got a nack (or nack channel closed)?
		case nacknum, ok := <-ch.NackEvents():
			if !ok { // nack channel closed (probably resulting from channel close exception)
				fmt.Println("ReliablePublisher():  ERROR:  nack channel closed unexpectedly")
				break loop
			}
			handleResult(nacknum, params.nacked, nil, "nack") // handle nack

		// got a result from publisher routine?
		case err := <-pubresults:
			ready = true    // publisher free
			if err != nil { // error
				handleResult(lastseqnum, params.nacked, err, "Publish() error") // handle as nack w/ error
			}

		// no channels ready
		default:
			switch {

			// quitting?  still have to handle acks/nacks in flight
			// not ready?  waiting for a response from publisher routine
			case quitting || !ready:
				time.Sleep(time.Microsecond) // and continue

			// running & ready:  try get next request
			default:
				select { // don't block
				case req, ok := <-params.requests: // got one (or channel closed)
					switch ok {

					case true: // got a request
						topublish <- req              // send it to publisher
						ready = false                 // publisher busy
						lastseqnum++                  // update seqnum
						outstanding[lastseqnum] = req // put it in the map

					case false: // input closed
						fmt.Println("ReliablePublisher():  input closed")
						startToQuit()
					}
				}
			}
		}
	}

	if len(outstanding) > 0 {
		fmt.Printf("ReliablePublisher():  WARNING:  exiting with %v msgs not acked/nacked\n", len(outstanding))
	}
}

///////////////////////////////////////////////////
// helper functions
///////////////////////////////////////////////////

// initializes a rpRunning struct
// except for wait_ch, which must be manually set to syncutil.Waiter.WaitChannel() after the goroutine starts
func (me *rpRunning) initialize(requests <-chan *PublishRequest) {
	me.quit_ch = make(chan int)
	me.result_ch = make(chan *rpResult, 1) // capacity=1 so publisher can push a result and exit
	me.requests = requests
	me.acked = make(chan *PublishResult, 100)
	me.nacked = make(chan *PublishResult, 100)
}

// copies a rpRunning struct (one for RP, one for publisher goroutine)
func (me *rpRunning) copy() *rpRunning {
	r := &rpRunning{}
	r.quit_ch = me.quit_ch
	r.wait_ch = me.wait_ch
	r.result_ch = me.result_ch
	r.requests = me.requests
	r.acked = me.acked
	r.nacked = me.nacked
	return r
}

// initializes an rpResult struct
func (me *rpResult) initialize() {
	me.Outstanding = map[uint64]*PublishRequest{}
}
