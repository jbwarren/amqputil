package amqputil

import (
	"github.com/streadway/amqp"
)

// the Connection struct wraps amqp.Connection
// it provides storage/management of other relevant stuff
// it embeds the amqp.Connection object for syntactic simplicity in accessing wrapped methods
type Connection struct {
	*amqp.Connection // embedded

	Blockings chan amqp.Blocking
	Channel   *Channel
}

// the Channel struct wraps amqp.Channel
// it provides storage/management of other relevant stuff
// it embeds the amqp.Channel object for syntactic simplicity in accessing wrapped methods
type Channel struct {
	*amqp.Channel // embedded

	Cancels  chan string
	Closings chan *amqp.Error
	Flows    chan bool
	Returns  chan amqp.Return
	Acks     chan uint64
	Nacks    chan uint64
}

// Dial() initializes a Connection object, including its contained Channel object
// if any error occurs, it returns a nil Connection
// flags control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// if <confirms> is >= 0, then the channel will be put into confirm mode, and <confirms> specifies the capacity of channels for receiving acks/nacks
// if <confirms> is < 0, then reliable publishing will not be used
func Dial(url string, blockings, cancels, closings, flows, returns bool, confirms int) (conn *Connection, dialErr, channelErr error) {
	c, dialErr := amqp.Dial(url) // connect to server
	if dialErr == nil {
		conn = &Connection{Connection: c} // create Connection struct
		if blockings {
			conn.Blockings = conn.NotifyBlocked(make(chan amqp.Blocking))
		}
		conn.Channel, channelErr = conn.NewChannel(cancels, closings, flows, returns, confirms) // create Channel
		if channelErr != nil {                                                                  // unwind on error
			c.Close()
			conn = nil
		}
	}
	return
}

// DialToConsume() wraps Dial(), hiding flags that are only relevant to publishers
// NOTE:  the returned Connection/Channel is still ok for publishing - this is mainly a convenience function
func DialToConsume(url string, closings bool) (conn *Connection, dialErr, channelErr error) {
	return Dial(url, false, false, closings, false, false, -1)
}

// closes the associated Channel and the underlying amqp.Connection object
// any other Channels created on this Connection should be closed first
func (conn *Connection) Close() {
	if conn.Channel != nil {
		conn.Channel.Close()
		conn.Channel = nil
	}
	conn.Connection.Close()
}

// creates a Channel object that may be used separately from this Connection object
// if any error occurs, returns a nil Channel
// flags control the initializtion of various (golang) channels (via Notify_() methods) for monitoring out-of-band events
// if <confirms> is >= 0, then the channel will be put into confirm mode, and <confirms> specifies the capacity of channels for receiving acks/nacks
// if <confirms> is < 0, then reliable publishing will not be used
// NOTE:  this function is also called by Dial() to initialize the Connection's own Channel object
func (conn *Connection) NewChannel(cancels, closings, flows, returns bool, confirms int) (channel *Channel, err error) {
	ch, err := conn.Connection.Channel() // get channel
	if err == nil {
		channel = &Channel{Channel: ch} // create Channel struct
		if cancels {
			channel.Cancels = channel.NotifyCancel(make(chan string))
		}
		if closings {
			channel.Closings = channel.NotifyClose(make(chan *amqp.Error))
		}
		if flows {
			channel.Flows = channel.NotifyFlow(make(chan bool))
		}
		if returns {
			channel.Returns = channel.NotifyReturn(make(chan amqp.Return))
		}
		if confirms >= 0 {
			channel.Acks, channel.Nacks = channel.NotifyConfirm(make(chan uint64, confirms), make(chan uint64, confirms))
			err = channel.Confirm(false) // put into confirm mode (synchronous call)
			if err != nil {              // unwind on error
				channel.Close()
				channel = nil
			}
		}
	}
	return
}

// detaches the primary Channel object from this Connection, so that it may be closed manually
// NOTE:  the channel MUST then be closed manually
// useful when an application creates many Channels on a Connection and wants to treat them all similarly
// returns nil if there is no primary Channel
func (conn *Connection) DetachChannel() (channel *Channel) {
	channel, conn.Channel = conn.Channel, nil
	return
}

// closes the underlying amqp.Channel object
// called automatically by Connection.Close() if this Channel object is its primary Channel
//		call Connection.DetachChannel() if the Channel is to be closed manually
func (channel *Channel) Close() {
	channel.Channel.Close()
}
