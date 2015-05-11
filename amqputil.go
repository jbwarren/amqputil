package amqputil

import (
	"github.com/streadway/amqp"
)

type Connection struct {
	C         *amqp.Connection
	Blockings chan amqp.Blocking
	Channel   *Channel
}

type Channel struct {
	Ch       *amqp.Channel
	Cancels  chan string
	Closings chan *amqp.Error
	Flows    chan bool
	Returns  chan amqp.Return
}

func Dial(url string, blockings, cancels, closings, flows, returns bool) (conn *Connection, dialErr, channelErr error) {
	c, dialErr := amqp.Dial(url) // connect to server
	if dialErr == nil {
		conn = &Connection{C: c} // create Connection struct
		if blockings {
			conn.Blockings = c.NotifyBlocked(make(chan amqp.Blocking))
		}
		conn.Channel, channelErr = conn.NewChannel(cancels, closings, flows, returns) // create Channel
		if channelErr != nil {                                                        // unwind
			c.Close()
			conn = nil
		}
	}
	return
}

func (conn *Connection) Close() {
	if conn.Channel != nil {
		conn.Channel.Close()
		conn.Channel = nil
	}
	conn.C.Close()
}

func (conn *Connection) NewChannel(cancels, closings, flows, returns bool) (channel *Channel, err error) {
	ch, err := conn.C.Channel() // get channel
	if err == nil {
		channel = &Channel{Ch: ch} // create Channel struct
		if cancels {
			channel.Cancels = ch.NotifyCancel(make(chan string))
		}
		if closings {
			channel.Closings = ch.NotifyClose(make(chan *amqp.Error))
		}
		if flows {
			channel.Flows = ch.NotifyFlow(make(chan bool))
		}
		if returns {
			channel.Returns = ch.NotifyReturn(make(chan amqp.Return))
		}
	}
	return
}

func (channel *Channel) Close() {
	channel.Ch.Close()
}
