/*
Go package that implements simple means to create a peer-to-peer network
using TCP sockets. The network will be a full network, that is every node
will be connected to every other node.

Every node will self-assign a GUID that it will advertise itself with on
the network.

Using gopeer consists of three parts: define the protocol messages you will need,
create a struct that implements the NodeHandlerDelegate and boot up a node

As an example, let's consider a simple peer2peer key-value store:

We define the following messages:

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

And create a list of their values to later register with the node:

        var dbMessages = []interface{} {
                setKeyMessage{},
                getKeyMessage{},
                getKeyValueMessage{},
                true
        }

Note the last value, which is an instance of the bool type, which we'll
use as a result message while setting keys. So: you cannot only use
custom structs, any JSON serializable structs are acceptable.

Next, we create the delegate:

        type dbDelegate struct {
                database map[string]interface{}
        }

        func newDbDelegate() *dbDelegate {
                return &dbDelegate {
                        database: make(map[string]interface{}),
                }
        }

        // We handle two types of requests: set key and get key
        func (td *dbDelegate) Request(mc *gopeers.MessageConnection, message interface{}) (interface{}, error) {
                switch m := message.(type) {
                case *setKeyMessage:
                        td.database[m.Key] = m.Value
                        // Here we return a bool value back
                        return true, nil
                case *getKeyMessage:
                        if v, ok := td.database[m.Key]; ok {
                                return getKeyValueMessage{v}, nil
                        } else {
                                return nil, errors.New("No such key")
                        }
                }
        }

        // And we'll ignore the rest
        func (td *dbDelegate) Notification(mc *gopeers.MessageConnection, message interface{}) {
        }

        func (td *dbDelegate) OpenReadChannel(vmc *gopeers.MessageConnection, message interface{}, channel chan []byte) error {
        }

        func (td *dbDelegate) OpenWriteChannel(mc *gopeers.MessageConnection, message interface{}, channel chan []byte) error {
        }

        func (td *dbDelegate) Disconnect(mc *gopeers.MessageConnection) {
        }

And finally, we boot up a node:

        node := gopeers.NewNode("127.0.0.1", newDbDelegate(), dbMessages)
        node.Start()

This starts a node in the background, to connect to another node:

        node.Connect("someip", somePort)
*/
package gopeers
