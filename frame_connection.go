package gopeers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Frame connection only operates on []byte

type FrameType uint8

const (
	NotificationFrame         FrameType = 0
	RequestFrame              FrameType = 1
	RequestStreamFrame        FrameType = 2
	ResponseFrame             FrameType = 3
	RequestReceiveStreamFrame FrameType = 4
	RequestSendStreamFrame    FrameType = 5
	StreamDataFrame           FrameType = 6
	StreamCloseFrame          FrameType = 7
	StreamClosedFrame         FrameType = 8
)

type FrameConnection struct {
	info    NodeInfo
	conn    net.Conn
	reqId   uint32
	closing bool
	// readChannel chan Frame
	writeChannel chan Frame
	requests     map[uint32]PendingRequest

	// handlers
	onNotification         func([]byte)
	onRequest              func([]byte) []byte
	onRequestReceiveStream func([]byte, chan []byte)
	onRequestSendStream    func([]byte, chan []byte)
	onDisconnect           func()
}

type Frame struct {
	frameType FrameType
	reqId       uint32
	data        []byte
}

type PendingRequest struct {
	stream  bool
	channel chan []byte
	doneChannel chan bool
}

func NewFrameConnection(conn net.Conn) *FrameConnection {
	c := &FrameConnection{}
	c.initFrameConnection(conn)
	return c
}

func (c *FrameConnection) initFrameConnection(conn net.Conn) {
        c.reqId = 0
        c.conn = conn
        c.writeChannel = make(chan Frame)
        c.requests = make(map[uint32]PendingRequest)
}

func (c *FrameConnection) Loop() {
	defer c.conn.Close()
	go c.writeLoop()
	c.readLoop()
}

func (c *FrameConnection) Notify(data []byte) {
	c.reqId++
	f := Frame{
		frameType: NotificationFrame,
		reqId:     c.reqId,
		data:      data,
	}
	c.writeChannel <- f
}

func (c *FrameConnection) Request(data []byte) ([]byte, error) {
	c.reqId++
	reqId := c.reqId
	f := Frame{
		frameType: RequestFrame,
		reqId:       reqId,
		data:        data,
	}
	pr := PendingRequest{false, make(chan []byte), nil}
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

func (c *FrameConnection) OpenReceiveStream(data []byte) (chan []byte, error) {
	c.reqId++
	f := Frame{
		frameType: RequestReceiveStreamFrame,
		reqId:       c.reqId,
		data:        data,
	}
	pr := PendingRequest{true, make(chan []byte), nil}
	c.requests[c.reqId] = pr
	c.writeChannel <- f
	return pr.channel, nil
}

func (c *FrameConnection) OpenSendStream(data []byte) (chan []byte, chan bool, error) {
	c.reqId++
	f := Frame{
		frameType: RequestSendStreamFrame,
		reqId:       c.reqId,
		data:        data,
	}
	pr := PendingRequest{true, make(chan []byte), make(chan bool, 1)}
	c.requests[c.reqId] = pr
	c.writeChannel <- f
	go c.streamResponse(c.reqId, pr.channel)
	return pr.channel, pr.doneChannel, nil
}

func (c *FrameConnection) sendResponse(reqId uint32, data []byte) {
	f := Frame{
		frameType: ResponseFrame,
		reqId:       reqId,
		data:        data,
	}
	c.writeChannel <- f
}
func (c *FrameConnection) streamResponse(reqId uint32, channel chan []byte) {
	for data := range channel {
		c.writeChannel <- Frame{
			frameType: StreamDataFrame,
			reqId:       reqId,
			data:        data,
		}
	}
	c.writeChannel <- Frame{
		frameType: StreamCloseFrame,
		reqId:       reqId,
		data:        []byte{},
	}
}

func (c *FrameConnection) writeLoop() {
	for {
		f, ok := <-c.writeChannel
		if !ok {
			c.Close()
			return
		}
		err := binary.Write(c.conn, binary.LittleEndian, f.frameType)
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
	var frameType FrameType
	var reqId uint32
	var length uint32
	for {
		// read FrameType
		err := binary.Read(c.conn, binary.LittleEndian, &frameType)
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

		switch frameType {
		case NotificationFrame:
			c.onNotification(buffer)
		case RequestFrame:
			// TODO: run in goroutine
			resp := c.onRequest(buffer)
			c.sendResponse(reqId, resp)
		case RequestReceiveStreamFrame:
			pr := PendingRequest{true, make(chan []byte), nil}
			c.requests[reqId] = pr
			go c.onRequestReceiveStream(buffer, pr.channel)
			go c.streamResponse(reqId, pr.channel)
		case RequestSendStreamFrame:
			pr := PendingRequest{true, make(chan []byte), nil}
			c.requests[reqId] = pr
			go c.onRequestSendStream(buffer, pr.channel)
		case ResponseFrame, StreamDataFrame:
			if pr, ok := c.requests[reqId]; ok {
				pr.channel <- buffer
				// NOTE: Request method closes channel and cleans up
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		case StreamCloseFrame:
			if pr, ok := c.requests[reqId]; ok {
				close(pr.channel)
				delete(c.requests, reqId)
				f := Frame {
                                    frameType: StreamClosedFrame,
                                    reqId:       reqId,
                                    data:        []byte{},
                                }
                                c.writeChannel <- f
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		case StreamClosedFrame:
			if pr, ok := c.requests[reqId]; ok {
			        pr.doneChannel <- true
// 				close(pr.channel)
				delete(c.requests, reqId)
			} else {
				fmt.Println("Response to non-existent request id", reqId)
			}
		default:
			fmt.Println("Unkown message type", frameType)

		}

	}
}

func (c *FrameConnection) Close() {
	if !c.closing {
		c.closing = true
		close(c.writeChannel)
		c.conn.Close()
		c.onDisconnect()
	}
}
