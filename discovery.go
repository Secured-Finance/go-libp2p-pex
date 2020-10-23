package pex

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

const (
	PEXProtocolID = "/pex/1.0.0"
)

var pdLog = logrus.WithFields(logrus.Fields{
	"subsystem": "pex",
})

type PEXDiscovery struct {
	bootstrapNodes []ma.Multiaddr
	host           host.Host
	peersMutex     sync.RWMutex
	newPeers       map[string]*newPeersChannelWrapper
	peers          map[string][]*peerInfo
}

type newPeersChannelWrapper struct {
	NewPeersChannel chan peer.AddrInfo
	count           int
	maxCount        int
}

func NewPEXDiscovery(h host.Host, bootstrapNodes []ma.Multiaddr) (*PEXDiscovery, error) {
	pd := &PEXDiscovery{host: h, peers: map[string][]*peerInfo{}, newPeers: map[string]*newPeersChannelWrapper{}}
	for _, addr := range bootstrapNodes {
		peerAddr, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, err
		}
		err = h.Connect(context.TODO(), *peerAddr)
		if err != nil {
			return nil, err
		}
	}
	h.SetStreamHandler(PEXProtocolID, pd.handleStream)
	pd.bootstrapNodes = bootstrapNodes
	return pd, nil
}

func (pd *PEXDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	dOpts := discovery.Options{}
	_ = dOpts.Apply(opts...)

	pInfo := peerInfo{
		peer.AddrInfo{
			ID:    pd.host.ID(),
			Addrs: pd.host.Addrs(),
		}, dOpts.Ttl,
	}
	for _, addr := range pd.bootstrapNodes {
		bAddrInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return 0, err
		}
		stream, err := pd.host.NewStream(ctx, bAddrInfo.ID, PEXProtocolID)
		if err != nil {
			return 0, err
		}

		sw := newStreamWrapper(context.TODO(), stream)
		err = sw.Advertise(ns, pInfo)
		if err != nil {
			return 0, err
		}
	}

	return dOpts.Ttl, nil
}

func (pd *PEXDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	dOpts := discovery.Options{}
	_ = dOpts.Apply(opts...)

	pd.newPeers[ns] = &newPeersChannelWrapper{
		NewPeersChannel: make(chan peer.AddrInfo),
		maxCount:        dOpts.Limit,
	}
	return pd.newPeers[ns].NewPeersChannel, nil
}

func (pd *PEXDiscovery) getPeers(ns string, count int) []peerInfo {
	pd.peersMutex.RLock()
	defer pd.peersMutex.RUnlock()

	rand.Seed(time.Now().UnixNano())
	var peers []peerInfo

	if len(pd.peers) == 0 {
		return peers
	}

	// randomly select peers which we have
	for i := 0; i < count; i++ {
		if p, ok := pd.peers[ns]; ok {
			randPeerNumber := rand.Intn(len(pd.peers[ns]) - 1)
			pinfo := p[randPeerNumber]
			var isAlreadyExistInResList bool
			for _, v := range peers {
				if v.ID.String() == pinfo.ID.String() {
					isAlreadyExistInResList = true
				}
			}
			if isAlreadyExistInResList {
				continue
			}
			peers = append(peers, *pinfo)
		}
	}
	return peers
}

func (pd *PEXDiscovery) handleStream(stream network.Stream) {
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	go pd.readData(context.TODO(), rw)
}

func (pd *PEXDiscovery) readData(ctx context.Context, rw *bufio.ReadWriter) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			{
				data, err := rw.ReadString('\n')
				if err != nil {
					swLog.Warn("unable to read message: ", err.Error())
					return // we do it because probably stream was closed
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
					go pd.handleMessage(rw, &msg)
				} else {
					swLog.Warn("incorrect data received")
					break
				}
			}
		}
	}
}

func (pd *PEXDiscovery) handleMessage(rw *bufio.ReadWriter, msg *message) {
	switch msg.Type {
	case "request_peers":
		{
			peers := pd.getPeers(msg.Payload["ns"].(string), msg.Payload["count"].(int))
			var resp message
			resp.Payload = map[string]interface{}{}
			resp.ID = msg.ID
			resp.Type = "request_peers"
			peersJson, err := json.Marshal(peers)
			if err != nil {
				pdLog.Warn("cannot marshal peer list: " + err.Error())
				return
			}
			resp.Payload["peers"] = peersJson
			mResp, err := json.Marshal(resp)
			if err != nil {
				pdLog.Warn("cannot marshal response: " + err.Error())
				return
			}
			_, err = rw.Write(mResp)
			if err != nil {
				pdLog.Warn("cannot send response: " + err.Error())
				return
			}
		}
	case "advertise":
		{
			ns := msg.Payload["ns"].(string)
			peerInfoStr := msg.Payload["peerInfo"].(string)
			var peerInfo peerInfo
			err := json.Unmarshal([]byte(peerInfoStr), &peerInfo)
			if err != nil {
				pdLog.Warn("cannot unmarshal peerInfo: " + err.Error())
				return
			}
			pd.peersMutex.RLock()
			for i, v := range pd.peers[ns] {
				if v.ID.String() == peerInfo.ID.String() {
					pd.peersMutex.Lock()
					peerArr := pd.peers[ns]

					// delete old info and update peer
					peerArr[i] = peerArr[len(peerArr)-1]
					peerArr[len(peerArr)-1] = nil
					peerArr = peerArr[:len(peerArr)-1]
					peerArr = append(peerArr, &peerInfo)
					pd.peers[ns] = peerArr
					pd.peersMutex.Unlock()
				} else {
					// add new peer
					pd.peersMutex.Lock()
					peerArr := pd.peers[ns]
					peerArr = append(peerArr, &peerInfo)
					pd.peers[ns] = peerArr
					pd.peersMutex.Unlock()
					if v, ok := pd.newPeers[ns]; ok {
						v.count++
						if v.count > v.maxCount {
							close(v.NewPeersChannel)
							delete(pd.newPeers, ns)
							continue
						}
						v.NewPeersChannel <- peerInfo.AddrInfo
					}
				}
			}
			pd.peersMutex.RUnlock()
		}
	}
}
