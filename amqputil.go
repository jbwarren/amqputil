package amqputil

import (
	"github.com/streadway/amqp"
)

////////////////////////////////////////////
// structs & accessors
////////////////////////////////////////////

// the Connection struct wraps amqp.Connection
// it provides storage/management of other relevant stuff
// it embeds the amqp.Connection object for syntactic simplicity in accessing wrapped methods
type Connection struct {
	*amqp.Connection // embedded

	events  *ConnectionEvents
	channel *Channel
}

// the Channel struct wraps amqp.Channel
// it provides storage/management of other relevant stuff
// it embeds the amqp.Channel object for syntactic simplicity in accessing wrapped methods
type Channel struct {
	*amqp.Channel // embedded

	events    *ChannelEvents
	inConfirm bool
}

// channels for events associated with connections
type ConnectionEvents struct {
	blockings chan amqp.Blocking
	closings  chan *amqp.Error
}

// channels for events associated with amqp channels
type ChannelEvents struct {
	cancels  chan string
	closings chan *amqp.Error
	flows    chan bool
	returns  chan amqp.Return
	acks     chan uint64
	nacks    chan uint64
}

// the ConnectionOptions struct encapsulates options for an amqp connection
type ConnectionOptions struct {
	// general options
	GetClosings bool // initialize Connection.Closings via NotifyClose()

	// publisher options
	GetBlockings bool // initialize Connection.Blockings via NotifyBlocked()
}

type ChannelOptions struct {
	// general options
	GetClosings bool // initialize Channel.Closings via NotifyClose()

	// publisher options
	GetFlows        bool // initialize Channel.Flows via NotifyFlow()
	GetReturns      bool // initialize Channel.Returns via NotifyReturn()
	ConfirmPubs     bool // put Channel in Confirm mode and initialize Channel.Acks/Nacks via NotifyConfirm()
	ConfirmCapacity int  // capacity of Channel.Acks/Nacks (ignored unless ConfirmPubs is true)

	// consumer options
	GetCancels bool // initialize Channel.Cancels via NotifyCancel()
}

///////////////////////////////////////////////////////////
// major methods
///////////////////////////////////////////////////////////

// Dial() initializes a Connection object, including its contained Channel object
// if any error occurs, it returns a nil Connection
// options structs controls the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// if an options struct is nil, then default options are used (all disabled)
func Dial(url string, connopts *ConnectionOptions, chanopts *ChannelOptions) (conn *Connection, err error) {
	if connopts == nil {
		connopts = &ConnectionOptions{} // use defaults
	}

	// connect to server
	c, err := amqp.Dial(url)
	if err != nil {
		return
	}

	// initialize object
	conn = &Connection{Connection: c} // create Connection struct
	conn.events = conn.registerForEvents(connopts)

	// create channel
	conn.channel, err = conn.NewChannel(chanopts)
	if err != nil { // unwind on error
		c.Close()
		conn = nil
	}
	return
}

// DialToConsume() wraps Dial(), hiding options that are only relevant to publishers
// NOTE:  the returned Connection/Channel is still ok for publishing - this is mainly a convenience function
func DialToConsume(url string, cancels, closings bool) (conn *Connection, err error) {
	return Dial(url, nil, &ChannelOptions{GetCancels: cancels, GetClosings: closings})
}

// closes the associated Channel and the underlying amqp.Connection object
// any other Channels created on this Connection should be closed first
func (me *Connection) Close() {
	if me.channel != nil {
		me.channel.Close()
		me.channel = nil
	}
	me.Connection.Close()
	me.events = nil
}

// creates a Channel object that may be used separately from this Connection object
// if any error occurs, returns a nil Channel
// <options> control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// if <options> is nil, then default options are used (all disabled)
// NOTE:  this function is also called by Dial() to initialize the Connection's own Channel object
func (me *Connection) NewChannel(options *ChannelOptions) (channel *Channel, err error) {
	if options == nil {
		options = &ChannelOptions{} // use defaults
	}

	// get channel
	ch, err := me.Connection.Channel()
	if err != nil {
		return
	}

	// initialize object
	channel = &Channel{Channel: ch}
	channel.events, err = channel.RegisterForEvents(options)
	if err != nil {
		channel.Close()
		channel = nil
	}
	return
}

// detaches the primary Channel object from this Connection, so that it may be closed manually
// NOTE:  the channel MUST then be closed manually
// useful when an application creates many Channels on a Connection and wants to treat them all similarly
// returns nil if there is no primary Channel
func (me *Connection) DetachChannel() (channel *Channel) {
	channel, me.channel = me.channel, nil
	return
}

// sets up and returns (golang) channels for various amqp connection/channel events
// this can be used to get a duplicate set of event channels for the same connection/channel, in case there are multiple consumers
// <options> control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// NOTE:  if ack/nack events (confirm mode) have been registered before, the message id counter will NOT be reset (know what you're doing)
func (me *Connection) RegisterForEvents(connopts *ConnectionOptions, chanopts *ChannelOptions) (connevents *ConnectionEvents, chanevents *ChannelEvents, err error) {
	connevents = me.registerForEvents(connopts)
	if me.channel != nil {
		chanevents, err = me.channel.RegisterForEvents(chanopts)
	}
	return
}

// sets up and returns (golang) channels for various Connection events
// <options> control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// NOTE:  this function is also called by Dial() to initialize the Connection object
func (me *Connection) registerForEvents(connopts *ConnectionOptions) (connevents *ConnectionEvents) {
	if connopts == nil {
		connopts = &ConnectionOptions{} // use defaults
	}

	// register for connection events
	if connopts.GetBlockings || connopts.GetClosings {
		connevents = &ConnectionEvents{} // create events structure
		if connopts.GetBlockings {
			connevents.blockings = me.NotifyBlocked(make(chan amqp.Blocking)) // register to get blocking events
		}
		if connopts.GetClosings {
			connevents.closings = me.NotifyClose(make(chan *amqp.Error)) // register to get closing events
		}
	}
	return
}

// sets up and returns (golang) channels for various amqp channel events
// this can be used to get a duplicate set of event channels for the same channel, in case there are multiple consumers
// <options> control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// NOTE:  if ack/nack events (confirm mode) have been registered before, the message id counter will NOT be reset (know what you're doing)
// NOTE:  this function is also called by Connection.NewChannel() to initialize a Channel object
func (me *Channel) RegisterForEvents(options *ChannelOptions) (events *ChannelEvents, err error) {
	if options == nil {
		return
	}

	// register for events
	if options.GetCancels || options.GetClosings || options.GetFlows || options.GetReturns {
		events = &ChannelEvents{} // create events struct
		if options.GetCancels {
			events.cancels = me.NotifyCancel(make(chan string))
		}
		if options.GetClosings {
			events.closings = me.NotifyClose(make(chan *amqp.Error))
		}
		if options.GetFlows {
			events.flows = me.NotifyFlow(make(chan bool))
		}
		if options.GetReturns {
			events.returns = me.NotifyReturn(make(chan amqp.Return))
		}
		if options.ConfirmPubs {
			events.acks, events.nacks = me.NotifyConfirm(make(chan uint64, options.ConfirmCapacity), make(chan uint64, options.ConfirmCapacity))
			if !me.inConfirm { // go into confirm mode if not already
				err = me.Confirm(false) // put into confirm mode (synchronous call)
				// let things play out on err - not sure what to do - let app handle
			}
		}
	}
	return
}

// closes the underlying amqp.Channel object
// called automatically by Connection.Close() if this Channel object is its primary Channel
//		call Connection.DetachChannel() if the Channel is to be closed manually
func (me *Channel) Close() {
	me.Channel.Close()
	me.events = nil
}

////////////////////////////////////////////////
// accessors & other simple methods
////////////////////////////////////////////////

func (me *Connection) Channel() *Channel {
	return me.channel
}

func (me *Connection) Events() *ConnectionEvents {
	return me.events
}

func (me *Connection) BlockingEvents() <-chan amqp.Blocking {
	if me.events == nil {
		return nil
	}
	return me.events.blockings
}

func (me *Connection) ClosingEvents() <-chan *amqp.Error {
	if me.events == nil {
		return nil
	}
	return me.events.closings
}

func (me *ConnectionEvents) Blockings() <-chan amqp.Blocking {
	return me.blockings
}

func (me *ConnectionEvents) Closings() <-chan *amqp.Error {
	return me.closings
}

func (me *Channel) Events() *ChannelEvents {
	return me.events
}

func (me *Channel) CancelEvents() <-chan string {
	if me.events == nil {
		return nil
	}
	return me.events.cancels
}

func (me *Channel) CloseEvents() <-chan *amqp.Error {
	if me.events == nil {
		return nil
	}
	return me.events.closings
}

func (me *Channel) FlowEvents() <-chan bool {
	if me.events == nil {
		return nil
	}
	return me.events.flows
}

func (me *Channel) ReturnEvents() <-chan amqp.Return {
	if me.events == nil {
		return nil
	}
	return me.events.returns
}

func (me *Channel) AckEvents() <-chan uint64 {
	if me.events == nil {
		return nil
	}
	return me.events.acks
}

func (me *Channel) NackEvents() <-chan uint64 {
	if me.events == nil {
		return nil
	}
	return me.events.nacks
}

func (me *ChannelEvents) Cancels() <-chan string {
	return me.cancels
}

func (me *ChannelEvents) Closings() <-chan *amqp.Error {
	return me.closings
}

func (me *ChannelEvents) Flows() <-chan bool {
	return me.flows
}

func (me *ChannelEvents) Returns() <-chan amqp.Return {
	return me.returns
}

func (me *ChannelEvents) Acks() <-chan uint64 {
	return me.acks
}

func (me *ChannelEvents) Nacks() <-chan uint64 {
	return me.nacks
}

// sets a ConnectionOptions struct to have all options turned on
func (me *ConnectionOptions) AllEnabled() {
	me.GetBlockings = true
	me.GetClosings = true
}

// sets a ChannelOptions structure to have all producer options turned on
func (me *ChannelOptions) AllProducerEnabled(confirmCapacity int) {
	me.GetClosings = true
	me.GetFlows = true
	me.GetReturns = true
	me.ConfirmPubs = true
	me.ConfirmCapacity = confirmCapacity
}

// sets a ChannelOptions structure to have all consumer options turned on
func (me *ChannelOptions) AllConsumerEnabled() {
	me.GetClosings = true
	me.GetCancels = true
}

// sets a ChannelOptions structure to have all options turned on
func (me *ChannelOptions) AllEnabled(confirmCapacity int) {
	me.AllProducerEnabled(confirmCapacity)
	me.AllConsumerEnabled()
}
