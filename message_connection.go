package gopeers

import (
    "errors"
    "fmt"
    "net"
    "reflect"
    "encoding/json"
)


type messageKind uint16

const errorMessageKind = 65535

// Extends FrameConnection with a Message layer + error handling

type MessageConnection struct {
        FrameConnection

        info    NodeInfo

        protocolMessages []reflect.Type
        inverseProtocolMessages map[reflect.Type]messageKind

        // handlers
        OnNotificationMessage  func(interface{})
        OnRequestMessage       func(interface{}) (interface{}, error)
        OnOpenReadChannelMessage func(interface{}, chan []byte) error
        OnOpenWriteChannelMessage func(interface{}, chan []byte) error
}

// messageKind = ErrormessageKind
type errorMessage struct {
        Message string
}

func NewMessageConnection(conn net.Conn, protocolMessages []interface{}) *MessageConnection {
        mc := &MessageConnection {}
        mc.initFrameConnection(conn)
        mc.setProtocolMessages(protocolMessages)
        mc.OnNotification = func(data []byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                mc.OnNotificationMessage(val)
        }
        mc.OnRequest = func(data []byte) []byte {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return nil
                }
                result, err := mc.OnRequestMessage(val)
                if err != nil {
                        result = errorMessage { err.Error() }
                }
                resultBytes, err := mc.encodeMessage(result)
                if err != nil {
                        fmt.Println("Couldn't encode message", err)
                        return nil
                }
                return resultBytes
        }
        mc.OnOpenReadChannel = func(data []byte, channel chan []byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                err = mc.OnOpenReadChannelMessage(val, channel)
                if err != nil {
                        fmt.Println("Got error from request receive stream", err)
                }
        }
        mc.OnOpenWriteChannel = func(data []byte, channel chan[] byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                err = mc.OnOpenWriteChannelMessage(val, channel)
                if err != nil {
                        fmt.Println("Got error from request send stream", err)
                }
        }

        return mc
}


func (mc *MessageConnection) setProtocolMessages(messages []interface{}) {
        mc.protocolMessages = make([]reflect.Type, len(messages))
        mc.inverseProtocolMessages = make(map[reflect.Type]messageKind)
        for i, m := range messages {
                t := reflect.TypeOf(m)
                mc.protocolMessages[i] = t
                mc.inverseProtocolMessages[t] = messageKind(i)
        }
}

func (mc *MessageConnection) encodeMessage(message interface{}) ([]byte, error) {
        var messageKind messageKind
        switch message.(type) {
        case errorMessage:
                messageKind = errorMessageKind
        case *errorMessage:
                messageKind = errorMessageKind
        default:
                t := reflect.TypeOf(message)
                var ok bool
                messageKind, ok = mc.inverseProtocolMessages[t]
                if !ok {
                        return nil, errors.New(fmt.Sprintf("Not a known message type: %s", t))
                }
        }
        data, err := json.Marshal(message)
        if err != nil {
                return nil, err
        }
        dataWithmessageKind := make([]byte, len(data)+2)
        writeMessageKindToBytes(messageKind, dataWithmessageKind)
        copy(dataWithmessageKind[2:], data)
        return dataWithmessageKind, nil
}

func (mc *MessageConnection) decodeMessage(data []byte) (interface{}, error) {
        kind := readMessageKindFromBytes(data)
        if kind == errorMessageKind {
                value := errorMessage{}
                err := json.Unmarshal(data[2:], &value)
                if err != nil {
                        return nil, err
                }
                return nil, errors.New(value.Message)
        } else if kind < messageKind(len(mc.protocolMessages)) {
                value := reflect.New(mc.protocolMessages[kind]).Interface()
                err := json.Unmarshal(data[2:], value)
                return value, err
        } else {
                return nil, errors.New("Invalid message type")
        }
}

func (mc *MessageConnection) RequestMessage(message interface{}) (interface{}, error) {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return nil, err
        }
        resultBytes, err := mc.Request(data)
        if err != nil {
                return nil, err
        }
        return mc.decodeMessage(resultBytes)
}

func (mc *MessageConnection) NotifyMessage(message interface{}) error {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return err
        }
        mc.Notify(data)
        return nil
}

func (mc *MessageConnection) OpenReadChannelMessage(message interface{}) (chan []byte, error) {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return nil, err
        }
        return mc.OpenReadChannel(data)
}

func (mc *MessageConnection) OpenWriteChannelMessage(message interface{}) (chan []byte, chan bool, error) {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return nil, nil, err
        }
        return mc.OpenWriteChannel(data)
}

func writeMessageKindToBytes(t messageKind, data []byte) {
    data[0] = byte(t / 256)
    data[1] = byte(t % 256)
}

func readMessageKindFromBytes(data []byte) messageKind {
    return messageKind(data[0]) * 256 + messageKind(data[1])
}
