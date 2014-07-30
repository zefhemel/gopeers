package main

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"log"
	"net"
	"os"
)

const (
        BUFFER_SIZE = 128 * 1024
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

type NodeHandlerDelegate interface {
        Request(mc *MessageConnection, message interface{}) (interface{}, error)
        Notification(mc *MessageConnection, message interface{})
        RequestReceiveStream(vmc *MessageConnection, message interface{}, channel chan []byte) error
        RequestSendStream(mc *MessageConnection, message interface{}, channel chan []byte) error
        Disconnect(mc *MessageConnection)
}

// Built in protocol messages

type HelloMessage struct {
        Guid string
        Port int
}

type HelloResponseMessage struct {
        Guid string
        Nodes []NodeInfo
}

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
        node.protocolMessages = append([]interface{} { HelloMessage{}, HelloResponseMessage{} }, protocolMessages...)
	return node
}

func (n *Node) initNode(listenIp string) {
        n.guid = uuid.New()
        n.listenIp  = listenIp
        n.connections = make(map[string]*MessageConnection)
        n.initializedChan = make(chan bool, 1)
        n.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", n.guid), log.Lshortfile)
}


func (n *Node) initMessageConnection(c *MessageConnection) {
	c.onNotificationMessage = func(message interface{}) {
		n.handlerDelegate.Notification(c, message)
	}
	c.onRequestMessage = func(message interface{}) (interface{}, error) {
	        // Check if it's a built-in message I should respond to
	        switch m := message.(type) {
                case *HelloMessage:
                        nodes := make([]NodeInfo, len(n.connections))
                        idx := 0
                        for _, conn := range n.connections {
                            nodes[idx] = conn.info
                            idx++
                        }
                        c.info.Port = m.Port
                        n.connections[m.Guid] = c
                        c.info.Guid = m.Guid
                        return HelloResponseMessage {
                            Guid:  n.guid,
                            Nodes: nodes,
                        }, nil
                default:
		        return n.handlerDelegate.Request(c, message)
	        }
	}
	c.onRequestReceiveStreamMessage = func(message interface{}, channel chan []byte) error {
		return n.handlerDelegate.RequestReceiveStream(c, message, channel)
	}
	c.onRequestSendStreamMessage = func(message interface{}, channel chan []byte) error {
		return n.handlerDelegate.RequestSendStream(c, message, channel)
	}
	c.onDisconnect = func() {
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
	resp, err := c.RequestMessage(HelloMessage{
		Guid: n.guid,
		Port: n.listenPort,
	})
	if err != nil {
		return err
	}
	switch r := resp.(type) {
        case *HelloResponseMessage:
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

func (n *Node) Connect(ip string, port int) error {
	go n.mainLoop()
	// Wait until fully initialized
	<-n.initializedChan
	err := n.connect(NodeInfo{Ip: "127.0.0.1", Port: n.listenPort})
	if err != nil {
		n.log.Println("Couldn't connect to myself")
		return err
	}
	if ip != "" {
		return n.connect(NodeInfo{Ip: ip, Port: port})
	} else {
		return nil
	}
}

func (n *Node) Close() error {
	n.closing = true
	n.listener.Close()
	for _, conn := range n.connections {
		conn.Close()
	}
	return nil
}
