package gopeers

import (
    "errors"
    "fmt"
    "net"
    "reflect"
    "encoding/json"
)


type MessageType uint16

const ErrorMessageType = 65535

// Extends FrameConnection with a Message layer + error handling

type MessageConnection struct {
        FrameConnection

        protocolMessages []reflect.Type
        inverseProtocolMessages map[reflect.Type]MessageType

        // handlers
        onNotificationMessage  func(interface{})
        onRequestMessage       func(interface{}) (interface{}, error)
        onRequestReceiveStreamMessage func(interface{}, chan []byte) error
        onRequestSendStreamMessage func(interface{}, chan []byte) error
}

// messageType = ErrorMessageType
type ErrorMessage struct {
        Message string
}

func NewMessageConnection(conn net.Conn, protocolMessages []interface{}) *MessageConnection {
        mc := &MessageConnection {}
        mc.initFrameConnection(conn)
        mc.setProtocolMessages(protocolMessages)
        mc.onNotification = func(data []byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                mc.onNotificationMessage(val)
        }
        mc.onRequest = func(data []byte) []byte {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return nil
                }
                result, err := mc.onRequestMessage(val)
                if err != nil {
                        result = ErrorMessage { err.Error() }
                }
                resultBytes, err := mc.encodeMessage(result)
                if err != nil {
                        fmt.Println("Couldn't encode message", err)
                        return nil
                }
                return resultBytes
        }
        mc.onRequestReceiveStream = func(data []byte, channel chan []byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                err = mc.onRequestReceiveStreamMessage(val, channel)
                if err != nil {
                        fmt.Println("Got error from request receive stream", err)
                }
        }
        mc.onRequestSendStream = func(data []byte, channel chan[] byte) {
                val, err := mc.decodeMessage(data)
                if err != nil {
                        fmt.Println("Couldn't decode message", err)
                        return
                }
                err = mc.onRequestSendStreamMessage(val, channel)
                if err != nil {
                        fmt.Println("Got error from request send stream", err)
                }
        }

        return mc
}


func (mc *MessageConnection) setProtocolMessages(messages []interface{}) {
        mc.protocolMessages = make([]reflect.Type, len(messages))
        mc.inverseProtocolMessages = make(map[reflect.Type]MessageType)
        for i, m := range messages {
                t := reflect.TypeOf(m)
                mc.protocolMessages[i] = t
                mc.inverseProtocolMessages[t] = MessageType(i)
        }
}

func (mc *MessageConnection) encodeMessage(message interface{}) ([]byte, error) {
        var messageType MessageType
        switch message.(type) {
        case ErrorMessage:
                messageType = ErrorMessageType
        case *ErrorMessage:
                messageType = ErrorMessageType
        default:
                t := reflect.TypeOf(message)
                var ok bool
                messageType, ok = mc.inverseProtocolMessages[t]
                if !ok {
                        return nil, errors.New(fmt.Sprintf("Not a known message type: %s", t))
                }
        }
        data, err := json.Marshal(message)
        if err != nil {
                return nil, err
        }
        dataWithMessageType := make([]byte, len(data)+2)
        writeMesssageTypeToBytes(messageType, dataWithMessageType)
        copy(dataWithMessageType[2:], data)
        return dataWithMessageType, nil
}

func (mc *MessageConnection) decodeMessage(data []byte) (interface{}, error) {
        messageType := readMessageTypeFromBytes(data)
        if messageType == ErrorMessageType {
                value := ErrorMessage{}
                err := json.Unmarshal(data[2:], &value)
                if err != nil {
                        return nil, err
                }
                return nil, errors.New(value.Message)
        } else if messageType < MessageType(len(mc.protocolMessages)) {
                value := reflect.New(mc.protocolMessages[messageType]).Interface()
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

func (mc *MessageConnection) OpenReceiveStreamMessage(message interface{}) (chan []byte, error) {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return nil, err
        }
        return mc.OpenReceiveStream(data)
}

func (mc *MessageConnection) OpenSendStreamMessage(message interface{}) (chan []byte, chan bool, error) {
        data, err := mc.encodeMessage(message)
        if err != nil {
                return nil, nil, err
        }
        return mc.OpenSendStream(data)
}

func writeMesssageTypeToBytes(t MessageType, data []byte) {
    data[0] = byte(t / 256)
    data[1] = byte(t % 256)
}

func readMessageTypeFromBytes(data []byte) MessageType {
    return MessageType(data[0]) * 256 + MessageType(data[1])
}
