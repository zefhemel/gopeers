package gopeers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Frame connection only operates on []byte

type frameKind uint8

const (
	notificationFrame      frameKind = 0
	requestFrame           frameKind = 1
	requestStreamFrame     frameKind = 2
	responseFrame          frameKind = 3
	openReadChannelFrame   frameKind = 4
	openWriteChannelFrame  frameKind = 5
	channelDataFrame       frameKind = 6
	channelCloseFrame      frameKind = 7
	channelClosedFrame     frameKind = 8
)

type frame struct {
    kind       frameKind
    reqId      uint32
    data       []byte
}

type pendingRequest struct {
    stream  bool
    channel chan []byte
    doneChannel chan bool
}

type FrameConnection struct {
        // The actual socket connection
	conn                net.Conn

	// Current request Id (starts at 0)
	reqId               uint32

	// Whether the connection is in currently being closed
	closing             bool

	// Channel to push frames to be sent on
	writeChannel        chan frame

	// Pending requests
	requests            map[uint32]pendingRequest

	// Handlers for various events
	OnNotification      func([]byte)
	OnRequest           func([]byte) []byte
	OnOpenReadChannel   func([]byte, chan []byte)
	OnOpenWriteChannel  func([]byte, chan []byte)
	OnDisconnect        func()
}



func NewFrameConnection(conn net.Conn) *FrameConnection {
	c := &FrameConnection{}
	c.initFrameConnection(conn)
	return c
}

func (c *FrameConnection) initFrameConnection(conn net.Conn) {
        c.reqId = 0
        c.conn = conn
        c.writeChannel = make(chan frame)
        c.requests = make(map[uint32]pendingRequest)
}

func (c *FrameConnection) Loop() {
	defer c.conn.Close()
	go c.writeLoop()
	c.readLoop()
}

func (c *FrameConnection) Notify(data []byte) {
	c.reqId++
	f := frame{
		kind:  notificationFrame,
		reqId: c.reqId,
		data:  data,
	}
	c.writeChannel <- f
}

func (c *FrameConnection) Request(data []byte) ([]byte, error) {
	c.reqId++
	reqId := c.reqId
	f := frame{
		kind: requestFrame,
		reqId:       reqId,
		data:        data,
	}
	pr := pendingRequest{false, make(chan []byte), nil}
	c.requests[reqId] = pr
	c.writeChannel <- f
	result, ok := <-pr.channel
	if !ok {
		return nil, errors.New("Channel closed")
	}
	close(pr.channel)
	delete(c.requests, reqId)
	return result, nil
}

func (c *FrameConnection) OpenReadChannel(data []byte) (chan []byte, error) {
	c.reqId++
	f := frame{
		kind: openReadChannelFrame,
		reqId:       c.reqId,
		data:        data,
	}
	pr := pendingRequest{true, make(chan []byte), nil}
	c.requests[c.reqId] = pr
	c.writeChannel <- f
	return pr.channel, nil
}

func (c *FrameConnection) OpenWriteChannel(data []byte) (chan []byte, chan bool, error) {
	c.reqId++
	f := frame{
		kind: openWriteChannelFrame,
		reqId:       c.reqId,
		data:        data,
	}
	pr := pendingRequest{true, make(chan []byte), make(chan bool, 1)}
	c.requests[c.reqId] = pr
	c.writeChannel <- f
	go c.streamResponse(c.reqId, pr.channel)
	return pr.channel, pr.doneChannel, nil
}

func (c *FrameConnection) sendResponse(reqId uint32, data []byte) {
	f := frame{
		kind: responseFrame,
		reqId:       reqId,
		data:        data,
	}
	c.writeChannel <- f
}
func (c *FrameConnection) streamResponse(reqId uint32, channel chan []byte) {
	for data := range channel {
		c.writeChannel <- frame{
			kind: channelDataFrame,
			reqId:       reqId,
			data:        data,
		}
	}
	c.writeChannel <- frame{
		kind:  channelCloseFrame,
		reqId: reqId,
		data:  []byte{},
	}
}

func (c *FrameConnection) writeLoop() {
	for {
		f, ok := <-c.writeChannel
		if !ok {
			c.Close()
			return
		}
		err := binary.Write(c.conn, binary.LittleEndian, f.kind)
		if err != nil {
			fmt.Println("Write error 1", err)
			c.Close()
			return
		}
		err = binary.Write(c.conn, binary.LittleEndian, f.reqId)
		if err != nil {
			fmt.Println("Write error 2", err)
			c.Close()
			return
		}
		l := uint32(len(f.data))
		err = binary.Write(c.conn, binary.LittleEndian, l)
		if err != nil {
			fmt.Println("Write error 3", err)
			c.Close()
			return
		}
		err = binary.Write(c.conn, binary.LittleEndian, f.data)
		if err != nil {
			fmt.Println("Write error 4", err)
			c.Close()
			return
		}
	}
}

func (c *FrameConnection) readLoop() {
	var kind frameKind
	var reqId uint32
	var length uint32
	for {
		// read frameKind
		err := binary.Read(c.conn, binary.LittleEndian, &kind)
		if err != nil {
			// fmt.Println("Read error", err.Error())
			c.Close()
			return
		}
		// Read reqId
		err = binary.Read(c.conn, binary.LittleEndian, &reqId)
		if err != nil {
			// fmt.Println("Read error", err.Error())
			c.Close()
			return
		}
		// Read length
		err = binary.Read(c.conn, binary.LittleEndian, &length)
		if err != nil {
			// fmt.Println("Read error", err.Error())
			c.Close()
			return
		}
		buffer := make([]byte, length)
		_, err = io.ReadFull(c.conn, buffer)
		if err != nil {
			fmt.Println("Read error", err.Error())
			c.Close()
			return
		}

		switch kind {
		case notificationFrame:
			c.OnNotification(buffer)
		case requestFrame:
			// TODO: run in goroutine
			resp := c.OnRequest(buffer)
			c.sendResponse(reqId, resp)
		case openReadChannelFrame:
			pr := pendingRequest{true, make(chan []byte), nil}
			c.requests[reqId] = pr
			go c.OnOpenReadChannel(buffer, pr.channel)
			go c.streamResponse(reqId, pr.channel)
		case openWriteChannelFrame:
			pr := pendingRequest{true, make(chan []byte), nil}
			c.requests[reqId] = pr
			go c.OnOpenWriteChannel(buffer, pr.channel)
		case responseFrame, channelDataFrame:
			if pr, ok := c.requests[reqId]; ok {
				pr.channel <- buffer
				// NOTE: Request method closes channel and cleans up
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		case channelCloseFrame:
			if pr, ok := c.requests[reqId]; ok {
				close(pr.channel)
				delete(c.requests, reqId)
				f := frame {
                                    kind: channelClosedFrame,
                                    reqId:       reqId,
                                    data:        []byte{},
                                }
                                c.writeChannel <- f
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		case channelClosedFrame:
			if pr, ok := c.requests[reqId]; ok {
			        pr.doneChannel <- true
				delete(c.requests, reqId)
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		default:
			fmt.Println("Unkown message type", kind)

		}

	}
}

func (c *FrameConnection) Close() {
	if !c.closing {
		c.closing = true
		close(c.writeChannel)
		c.conn.Close()
		c.OnDisconnect()
	}
}
