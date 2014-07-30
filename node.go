package gopeers

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"log"
	"net"
	"os"
)

type NodeInfo struct {
	Guid        string
	Ip          string
	Port        int
	StorageNode bool
}

type PeerMessage struct {
	conn    *MessageConnection
	message interface{}
	err     error
}

/*
The handler interface that an application should implement, handling
incoming messages of various kinds.
*/
type NodeHandlerDelegate interface {
        // A RPC-like request has been received, respond with either an error
        // or return a new message. Both incoming and outgoing messages must be registered
        // protocol messages.
        Request(mc *MessageConnection, message interface{}) (interface{}, error)

        // A notification message was received.
        Notification(mc *MessageConnection, message interface{})

        // A request to open a read channel was received. The channel that's
        // passed in should be written to from this end (so that the receiver reads).
        OpenReadChannel(vmc *MessageConnection, message interface{}, channel chan []byte) error

        // A request to open a write channel was received. The channel that's
        // passed in should be read from (the other end will be writing to it).
        OpenWriteChannel(mc *MessageConnection, message interface{}, channel chan []byte) error

        // A peer has disconnected
        Disconnect(mc *MessageConnection)
}

// Built in protocol messages

type helloMessage struct {
        Guid string
        Port int
}

type helloResponseMessage struct {
        Guid string
        Nodes []NodeInfo
}

// The representation of a peer in the network. You usually instantiate
// one of these and connect it to another peer somewhere in the network.
type Node struct {
	guid        string
	listenIp    string
	listenPort  int
	listener    net.Listener
	connections map[string]*MessageConnection

	initializedChan chan bool

	log     *log.Logger
	closing bool

	handlerDelegate NodeHandlerDelegate
	protocolMessages []interface{}
}

func NewNode(listenIp string, handlerDelegate NodeHandlerDelegate, protocolMessages []interface{}) *Node {
        node := &Node {}
        node.initNode(listenIp)
        node.handlerDelegate = handlerDelegate
        node.protocolMessages = append([]interface{} { helloMessage{}, helloResponseMessage{} }, protocolMessages...)
	return node
}

func (n *Node) initNode(listenIp string) {
        n.guid = uuid.New()
        n.listenIp  = listenIp
        n.connections = make(map[string]*MessageConnection)
        n.initializedChan = make(chan bool, 1)
        n.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", n.guid), log.Lshortfile)
}

// Returns the (automatically generated) GUID for this node
func (n *Node) Guid() string {
        return n.guid
}

// Returns a list of all currently open connections to other peers
func (n *Node) Connections() []*MessageConnection {
        conns := make([]*MessageConnection, len(n.connections))
        i := 0
        for _, conn := range n.connections {
                conns[i] = conn
                i++
        }
        return conns
}

// Returns a connection given a particular GUId, or nil if not connected
func (n *Node) Connection(guid string) *MessageConnection {
        return n.connections[guid]
}


func (n *Node) initMessageConnection(c *MessageConnection) {
	c.OnNotificationMessage = func(message interface{}) {
		n.handlerDelegate.Notification(c, message)
	}
	c.OnRequestMessage = func(message interface{}) (interface{}, error) {
	        // Check if it's a built-in message I should respond to
	        switch m := message.(type) {
                case *helloMessage:
                        nodes := make([]NodeInfo, len(n.connections))
                        idx := 0
                        for _, conn := range n.connections {
                            nodes[idx] = conn.info
                            idx++
                        }
                        c.info.Port = m.Port
                        n.connections[m.Guid] = c
                        c.info.Guid = m.Guid
                        return helloResponseMessage {
                            Guid:  n.guid,
                            Nodes: nodes,
                        }, nil
                default:
		        return n.handlerDelegate.Request(c, message)
	        }
	}
	c.OnOpenReadChannelMessage = func(message interface{}, channel chan []byte) error {
		return n.handlerDelegate.OpenReadChannel(c, message, channel)
	}
	c.OnOpenWriteChannelMessage = func(message interface{}, channel chan []byte) error {
		return n.handlerDelegate.OpenWriteChannel(c, message, channel)
	}
	c.OnDisconnect = func() {
		if c.info.Guid != "" {
			n.log.Println("Node", c.info.Guid, "disconnected")
			n.handlerDelegate.Disconnect(c)
			delete(n.connections, c.info.Guid)
		} else {
			n.log.Println("SOmebody who I don't know yet disconnected", c.info)
		}
	}
}

func (n *Node) handleMessageConnection(conn net.Conn) {
	c := NewMessageConnection(conn, n.protocolMessages)
	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	c.info.Ip = host
	n.log.Println("Connected", host)
	n.initMessageConnection(c)
	c.Loop()
}

// Broadcast a request (RPC) to all connected peers and return responses
// as a (buffered) channel of PeerMessages
// Note that if you're only interested in one response, just get one and
// ignore the rest.
func (n *Node) BroadcastRequest(message interface{}) chan PeerMessage {
	channel := make(chan PeerMessage, len(n.connections))
	waitingForResponses := 0

	sendToMessageConnection := func(conn *MessageConnection) {
		waitingForResponses++
		response, err := conn.RequestMessage(message)
		if err != nil {
			channel <- PeerMessage{conn, nil, err}
		} else {
		        channel <- PeerMessage{conn, response, nil}
		}
		waitingForResponses--
		if waitingForResponses == 0 {
			close(channel)
		}
	}

	for _, conn := range n.connections {
		// 		n.log.Println("Sending to", conn.info.guid)
		go sendToMessageConnection(conn)
	}
	return channel
}

func (n *Node) mainLoop() {
	listening := false
	n.listenPort = 7337
	for !listening {
		var err error
		n.listener, err = net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", n.listenPort))
		if err != nil {
			n.listenPort++
		} else {
			listening = true
		}
	}
	n.log.Println("Listening on port ", n.listenPort)
	n.initializedChan <- true
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			if n.closing {
				n.log.Println("Stopping to listen")
				return
			} else {
				n.log.Println("Accept connection error", err)
				continue
			}
		}
		go n.handleMessageConnection(conn)
	}
}

func (n *Node) connect(info NodeInfo) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", info.Ip, info.Port))
	if err != nil {
		return err
	}
	c := NewMessageConnection(conn, n.protocolMessages)
	c.info = info
	n.initMessageConnection(c)
	go c.Loop()
	resp, err := c.RequestMessage(helloMessage{
		Guid: n.guid,
		Port: n.listenPort,
	})
	if err != nil {
		return err
	}
	switch r := resp.(type) {
        case *helloResponseMessage:
        	guid := r.Guid
        	n.connections[guid] = c
        	c.info.Guid = guid
        	for _, nodeInfo := range r.Nodes {
        		if nodeInfo.Guid == n.guid {
        			continue
        		}
        		if _, ok := n.connections[nodeInfo.Guid]; ok {
        			continue
        		}
        		err = n.connect(nodeInfo)
        		if err != nil {
        			n.log.Println("Could not connect to", nodeInfo)
        		}
        	}
        default:
                n.log.Println("Got response message I didn't get", resp)
	}
	return nil
}

// Boot up the node in the background, returns as soon as the node
// is listening for incoming connections.
func (n *Node) Start() error {
	go n.mainLoop()
	// Wait until fully initialized
	<-n.initializedChan
	return n.connect(NodeInfo{Ip: "127.0.0.1", Port: n.listenPort})
}

// Connect to another node in the network, which in turn will result in
// further connects to other nodes that that node knows about.
func (n *Node) Connect(ip string, port int) error {
	return n.connect(NodeInfo{Ip: ip, Port: port})
}

// Shut down the node, stop listening to incomming connections and
// close all existing connections to other nodes.
func (n *Node) Close() error {
	n.closing = true
	n.listener.Close()
	for _, conn := range n.connections {
		conn.Close()
	}
	return nil
}
