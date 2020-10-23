package pex

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/sirupsen/logrus"
)

type streamWrapper struct {
	pex      *PEXDiscovery
	ctx      context.Context
	stream   network.Stream
	rw       *bufio.ReadWriter
	requests map[string]chan message
}

type message struct {
	ID      string
	Type    string
	Payload map[string]interface{}
}

var swLog = logrus.WithFields(logrus.Fields{
	"subsystem": "pex/stream_wrapper",
})

func newStreamWrapper(ctx context.Context, stream network.Stream) *streamWrapper {
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	sw := &streamWrapper{ctx: ctx, stream: stream, rw: rw, requests: map[string]chan message{}}
	return sw
}

func (sw *streamWrapper) readData() {
	for {
		select {
		case <-sw.ctx.Done():
			return
		default:
			{
				data, err := sw.rw.ReadString('\n')
				if err != nil {
					swLog.Warn("unable to read message: ", err.Error())
					return
				}
				if data == "" {
					swLog.Debug("received message with empty content")
					break
				}
				if data != "\n" {
					var msg message
					err = json.Unmarshal([]byte(data), &msg)
					if err != nil {
						swLog.Warn("unable to unmarshal message: ", err.Error())
						break
					}
					if ch, ok := sw.requests[msg.ID]; ok {
						ch <- msg
					}
				} else {
					swLog.Debug("incorrect data received")
					break
				}
			}
		}

	}
}

func (sw *streamWrapper) GetPeers(ns string, count int) ([]peerInfo, error) {
	msg := message{}
	msg.Type = "get_peers"
	msg.Payload = map[string]interface{}{}
	msg.Payload["ns"] = ns
	msg.Payload["count"] = count
	resp, err := sw.makeNewRequest(msg)
	if err != nil {
		return nil, err
	}
	var peers []peerInfo
	err = json.Unmarshal([]byte(resp.Payload["peers"].(string)), &peers)
	if err != nil {
		return nil, err
	}
	return peers, nil
}

func (sw *streamWrapper) Advertise(ns string, pInfo peerInfo) error {
	msg := message{}
	msg.Type = "advertise"
	msg.Payload = map[string]interface{}{}
	msg.Payload["ns"] = ns
	mPInfo, err := json.Marshal(pInfo)
	if err != nil {
		return err
	}
	msg.Payload["peerInfo"] = string(mPInfo)
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = sw.rw.Write(msgJson)
	if err != nil {
		return err
	}
	return nil
}

func (sw *streamWrapper) makeNewRequest(msg message) (message, error) {
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return message{}, err
	}
	reqID := uuid.New().String()
	respChan := make(chan message)
	sw.requests[reqID] = respChan
	_, err = sw.rw.Write(msgJson)
	if err != nil {
		return message{}, err
	}
	resp := <-respChan
	return resp, nil
}
