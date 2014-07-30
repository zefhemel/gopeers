package gopeers

import (
	"fmt"
	"testing"
	"time"
	"errors"
)

type setKeyMessage struct {
        Key string
        Value interface{}
}


type getKeyMessage struct {
        Key string
}

type getKeyValueMessage struct {
        Value interface{}
}

type personAnnounce struct {
        Name string
        Age int
}

type sendSomeBytes struct {
}

type receiveSomeBytes struct {
}

var testMessages = []interface{} { personAnnounce{}, setKeyMessage{}, getKeyMessage{}, getKeyValueMessage{}, true, sendSomeBytes{}, receiveSomeBytes{} }

type testDelegate struct {
        database map[string]interface{}
        t *testing.T
}

func newTestDelegate(t *testing.T) *testDelegate {
        return &testDelegate {
                database: make(map[string]interface{}),
                t: t,
        }
}

func (td *testDelegate) Request(mc *MessageConnection, message interface{}) (interface{}, error) {
        // fmt.Println("Got a request", message)
        switch m := message.(type) {
        case *setKeyMessage:
                td.database[m.Key] = m.Value
                return true, nil
        case *getKeyMessage:
                if v, ok := td.database[m.Key]; ok {
                        return getKeyValueMessage{v}, nil
                } else {
                        return nil, errors.New("No such key")
                }
        default:
                fmt.Println("Unkown message", message)
                return nil, errors.New("No response")
        }
}

func (td *testDelegate) Notification(mc *MessageConnection, message interface{}) {
        fmt.Println("Got a notification", message)
}

func (td *testDelegate) OpenReadChannel(vmc *MessageConnection, message interface{}, channel chan []byte) error {
        fmt.Println("Got a request receive stream", message)
        switch message.(type) {
        case *sendSomeBytes:
                channel <- []byte { 0, 1, 2 }
                channel <- []byte { 2, 3, 4 }
                channel <- []byte { 4, 5, 6 }
                close(channel)
                return nil
        }
        return nil
}

func (td *testDelegate) OpenWriteChannel(mc *MessageConnection, message interface{}, channel chan []byte) error {
        fmt.Println("Got a request send stream", message)
        switch message.(type) {
        case *receiveSomeBytes:
                data := <-channel
                if len(data) != 3 {
                        td.t.Error("Expected 3 bytes")
                }
                data = <-channel
                if len(data) != 3 {
                        td.t.Error("Expected 3 bytes")
                }
                data = <-channel
                if len(data) != 3 {
                        td.t.Error("Expected 3 bytes")
                }
                _, ok := <-channel
                if ok {
                        td.t.Error("Channel should be closed now")
                }
                return nil
        default:
                fmt.Println("Not sure what to do with this:", message)
        }
        return nil
}

func (td *testDelegate) Disconnect(mc *MessageConnection) {
        // fmt.Println("Got a disconnect")
}


func TestConnect(t *testing.T) {
	node1 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node1.Connect("", 0)
	node2 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node2.Connect("127.0.0.1", node1.listenPort)
	node3 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node3.Connect("127.0.0.1", node1.listenPort)
	node4 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node4.Connect("127.0.0.1", node3.listenPort)

	countConnections := func(n *Node) int {
		c := 0
		for _ = range n.connections {
			c++
		}
		return c
	}

	if countConnections(node1) != 4 {
		t.Error("Node 1 has wrong number of connections")
	}
	if countConnections(node2) != 4 {
		t.Error("Node 2 has wrong number of connections")
	}
	if countConnections(node3) != 4 {
		t.Error("Node 3 has wrong number of connections")
	}
	if countConnections(node4) != 4 {
		t.Error("Node 4 has wrong number of connections")
	}
}

func TestConnectDisconnect(t *testing.T) {
	node1 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node1.Connect("", 0)
	node2 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node2.Connect("127.0.0.1", node1.listenPort)
	node3 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node3.Connect("127.0.0.1", node1.listenPort)
	node4 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node4.Connect("127.0.0.1", node1.listenPort)
	fmt.Println("Now shutting down node 3 and 1", node3.guid)
	node3.Close()
	node1.Close()

	countConnections := func(n *Node) int {
		return len(n.connections)
	}

	time.Sleep(100 * time.Millisecond)

	if len(node1.connections) != 0 {
		t.Error("Node 1 has wrong number of connections", countConnections(node1))
	}
	if countConnections(node2) != 2 {
		t.Error("Node 2 has wrong number of connections")
	}
	if countConnections(node3) != 0 {
		t.Error("Node 3 has wrong number of connections")
	}
	if countConnections(node4) != 2 {
		t.Error("Node 4 has wrong number of connections")
	}
}

func TestKeyValue(t *testing.T) {
	node1 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node1.Connect("", 0)
	node2 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node2.Connect("127.0.0.1", node1.listenPort)
	node3 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node3.Connect("127.0.0.1", node1.listenPort)

	setChannel := node1.BroadcastRequest(setKeyMessage{
	        Key: "name",
	        Value: "Zef Hemel",
        })

	for msg := range setChannel {
		if !*msg.message.(*bool) {
		        t.Error("Invalid response during set")
		}
	}

	peerMessage := <-node3.BroadcastRequest(getKeyMessage{
		Key: "name",
	})

	if peerMessage.message.(*getKeyValueMessage).Value != "Zef Hemel" {
		t.Error("Invalid response")
	}

	peerMessage = <-node2.BroadcastRequest(getKeyMessage{ Key: "age" })
	if peerMessage.err.Error() != "No such key" {
	        t.Error("No error message returned")
	}
}

func TestReceiveStream(t *testing.T) {
	node1 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node1.Connect("", 0)
	node2 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
	node2.Connect("127.0.0.1", node1.listenPort)
	channel, err := node1.connections[node2.guid].OpenReadChannelMessage(sendSomeBytes{})
	if err != nil {
		t.Error("Got error requesting some bytes", err)
		return
	}
	for data := range channel {
		fmt.Println("Bytes I got", data)
	}
}

func TestSendStream(t *testing.T) {
	node1 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
        node1.Connect("", 0)
        node2 := NewNode("127.0.0.1", newTestDelegate(t), testMessages)
        node2.Connect("127.0.0.1", node1.listenPort)
	channel, doneChannel, err := node1.connections[node2.guid].OpenWriteChannelMessage(receiveSomeBytes{})
	if err != nil {
		t.Error("Got error sending block", err)
		return
	}
	channel <- []byte { 0, 1, 2 }
        channel <- []byte { 2, 3, 4 }
        channel <- []byte { 4, 5, 6 }
	close(channel)
	<-doneChannel
}
