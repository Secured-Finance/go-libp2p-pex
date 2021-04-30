package pex

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	ctxio "github.com/jbenet/go-context/io"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/sirupsen/logrus"
)

const (
	MessageTypeGetPeers = iota
	MessageTypeAdvertise
	MessageTypePing
	MessageTypePong

	readMessageTimeout = time.Minute
)

type DiscoveryNetworkManager struct {
	streamMapMutex sync.Mutex
	streamMap      map[peer.ID]*StreamWrapper
	ctx            context.Context
	pex            *PEXDiscovery
}

func NewPEXDiscoveryNetwork(ctx context.Context, pex *PEXDiscovery) *DiscoveryNetworkManager {
	return &DiscoveryNetworkManager{
		pex:       pex,
		streamMap: map[peer.ID]*StreamWrapper{},
		ctx:       ctx,
	}
}

var dnLog = logrus.WithFields(logrus.Fields{
	"subsystem": "pex/discovery_network",
})

type PEXMessage struct {
	Type          uint8       `json:"type"`
	PeerInfo      *peerInfo   `json:"peerInfo"`
	Peers         []*peerInfo `json:"peers"`
	GetPeersCount uint        `json:"getPeersCount"`
	NetworkID     string      `json:"networkID"`
}

func (pdn *DiscoveryNetworkManager) handleNewStream(s network.Stream) {

	ctx := pdn.ctx
	cr := ctxio.NewReader(ctx, s)
	cw := ctxio.NewWriter(ctx, s)
	r := msgio.NewReaderSize(cr, network.MessageSizeMax)
	w := msgio.NewWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		pmes := &PEXMessage{}
		bMsg, err := r.ReadMsg()
		switch err {
		case io.EOF:
			s.Close()
			return
		case nil:
		default:
			s.Reset()
			return
		}

		err = cbor.Unmarshal(bMsg, pmes)
		if err != nil {
			s.Reset()
			dnLog.Warnf("Cannot unmarshal received message: %s", err.Error())
			return
		}

		// get handler for this msg type.
		handler := pdn.pex.getHandlerForType(pmes.Type)
		if handler == nil {
			s.Reset()
			dnLog.Debug("Unknown message type")
			return
		}

		// dispatch handler.
		resp, err := handler(ctx, mPeer, pmes)
		if err != nil {
			s.Reset()
			dnLog.Errorf("Handle message error: %s", err)
			return
		}

		// if nil response, return it before serializing
		if resp == nil {
			continue
		}

		// send out response msg
		bResp, err := cbor.Marshal(resp)
		if err != nil {
			s.Reset()
			dnLog.Errorf("Response marshalling error: %s", err)
			return
		}
		if err := w.WriteMsg(bResp); err != nil {
			s.Reset()
			dnLog.Errorf("Send response error: %s", err)
			return
		}
	}
}

// sendRequest sends out a request
func (pdn *DiscoveryNetworkManager) sendRequest(ctx context.Context, p peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	ms, err := pdn.createStreamWrapper(p)
	if err != nil {
		return nil, err
	}

	resp, err := ms.SendRequest(ctx, msg)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// sendMessage sends out a message without waiting for response
func (pdn *DiscoveryNetworkManager) sendMessage(ctx context.Context, p peer.ID, pmes *PEXMessage) error {
	sw, err := pdn.createStreamWrapper(p)
	if err != nil {
		return err
	}

	if err := sw.SendMessage(ctx, pmes); err != nil {
		return err
	}
	return nil
}

func (pdn *DiscoveryNetworkManager) createStreamWrapper(p peer.ID) (*StreamWrapper, error) {
	pdn.streamMapMutex.Lock()
	sw, ok := pdn.streamMap[p]
	if ok {
		pdn.streamMapMutex.Unlock()
		return sw, nil
	}
	sw = &StreamWrapper{peerID: p, host: pdn.pex.host}
	pdn.streamMap[p] = sw
	pdn.streamMapMutex.Unlock()

	if err := sw.initOrInvalidate(); err != nil {
		pdn.streamMapMutex.Lock()
		defer pdn.streamMapMutex.Unlock()

		swCur := pdn.streamMap[p]
		// Changed. Use the new one, old one is invalid and
		// not in the map so we can just throw it away.
		if sw != swCur {
			return swCur, nil
		}
		// Not changed, remove the now invalid stream from the
		// map.
		delete(pdn.streamMap, p)
		return nil, err
	}
	// Invalid but not in map. Must have been removed by a disconnect.

	// All ready to go.
	return sw, nil
}

type StreamWrapper struct {
	stream       network.Stream
	streamReader msgio.ReadCloser
	streamWriter msgio.WriteCloser
	mutex        sync.Mutex
	peerID       peer.ID
	host         host.Host

	invalid   bool
	singleMes int
}

// invalidate is called before this StreamWrapper is removed from the strmap.
// It prevents the StreamWrapper from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (sw *StreamWrapper) invalidate() {
	sw.invalid = true
	if sw.stream != nil {
		sw.stream.Reset()
		sw.stream = nil
	}
}

func (sw *StreamWrapper) initOrInvalidate() error {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()
	if err := sw.init(); err != nil {
		sw.invalidate()
		return err
	}
	return nil
}

func (sw *StreamWrapper) init() error {
	if sw.invalid {
		return fmt.Errorf("stream wrapper has been invalidated")
	}
	if sw.stream != nil {
		return nil
	}

	nstr, err := sw.host.NewStream(context.TODO(), sw.peerID, PEXProtocolID)
	if err != nil {
		return err
	}

	sw.streamReader = msgio.NewReaderSize(nstr, network.MessageSizeMax)
	sw.streamWriter = msgio.NewWriter(nstr)
	sw.stream = nstr

	return nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (sw *StreamWrapper) SendMessage(ctx context.Context, msg *PEXMessage) error {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()
	didRetry := false
	for {
		if err := sw.init(); err != nil {
			return err
		}

		bMsg, err := cbor.Marshal(msg)
		if err != nil {
			return err
		}

		if err := sw.streamWriter.WriteMsg(bMsg); err != nil {
			sw.stream.Reset()
			sw.stream = nil

			if didRetry {
				return err
			} else {
				didRetry = true
				continue
			}
		}

		//log.Event(ctx, "cNode SentMessage", sw.cNode .self, sw.peerID, msg)

		if sw.singleMes > streamReuseTries {
			sw.stream.Close()
			sw.stream = nil
		} else if didRetry {
			sw.singleMes++
		}

		return nil
	}
}

func (sw *StreamWrapper) SendRequest(ctx context.Context, msg *PEXMessage) (*PEXMessage, error) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()
	didRetry := false
	for {
		if err := sw.init(); err != nil {
			return nil, err
		}

		bMsg, err := cbor.Marshal(msg)
		if err != nil {
			return nil, err
		}

		if err := sw.streamWriter.WriteMsg(bMsg); err != nil {
			sw.stream.Reset()
			sw.stream = nil

			if didRetry {
				return nil, err
			} else {
				didRetry = true
				continue
			}
		}

		mes := &PEXMessage{}
		if err := sw.ctxReadMsg(ctx, mes); err != nil {
			sw.stream.Reset()
			sw.stream = nil

			if didRetry {
				dnLog.Warnf("Error reading message, bailing stream: %s", err.Error())
				return nil, err
			} else {
				dnLog.Warnf("Error reading message, trying again: %s", err)
				didRetry = true
				continue
			}
		}

		if sw.singleMes > streamReuseTries {
			sw.stream.Close()
			sw.stream = nil
		} else if didRetry {
			sw.singleMes++
		}

		return mes, nil
	}
}

func (sw *StreamWrapper) ctxReadMsg(ctx context.Context, msg *PEXMessage) error {
	errc := make(chan error, 1)
	go func(r msgio.ReadCloser) {
		bMsg, err := r.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		err = cbor.Unmarshal(bMsg, msg)
		errc <- err
	}(sw.streamReader)

	t := time.NewTimer(readMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return fmt.Errorf("read message timeout")
	}
}
