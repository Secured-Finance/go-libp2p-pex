package pex

import (
	"context"
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
	bootstrapNodes          []ma.Multiaddr
	host                    host.Host
	peersMutex              sync.RWMutex
	newPeers                map[string]*newPeersChannelWrapper
	peers                   map[string][]*peerInfo
	ticker                  *time.Ticker
	updateStopper           chan bool
	discoveryNetworkManager *DiscoveryNetworkManager
}

type newPeersChannelWrapper struct {
	NewPeersChannel chan peer.AddrInfo
	count           int
	maxCount        int
}

func NewPEXDiscovery(h host.Host, bootstrapNodes []ma.Multiaddr, updateInterval time.Duration) (*PEXDiscovery, error) {
	pd := &PEXDiscovery{host: h, peers: map[string][]*peerInfo{}, newPeers: map[string]*newPeersChannelWrapper{}}
	pd.discoveryNetworkManager = NewPEXDiscoveryNetwork(context.TODO(), pd)
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
	pd.updateStopper = pd.startAsyncPeerListUpdater(updateInterval)
	return pd, nil
}

func (pd *PEXDiscovery) startAsyncPeerListUpdater(updateInterval time.Duration) chan bool {
	stopper := make(chan bool, 1)
	go func() {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopper:
				{
					pdLog.Debug("peer list updater has stopped")
					return
				}
			case <-ticker.C:
				{
					pd.updatePeerList()
				}
			}
		}
	}()
	return stopper
}

func (pd *PEXDiscovery) updatePeerList() {
	// TODO
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

	// TODO advertise not only to bootstrap nodes, but also to random network neighbours (by network id)
	for _, addr := range pd.bootstrapNodes {
		bAddrInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return 0, err
		}

		msg := &PEXMessage{
			Type:      MessageTypeAdvertise,
			NetworkID: ns,
			PeerInfo:  pInfo,
		}

		err = pd.discoveryNetworkManager.sendMessage(context.TODO(), bAddrInfo.ID, msg)
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
	go pd.discoveryNetworkManager.handleNewStream(stream)
}

type Handler func(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error)

func (pd *PEXDiscovery) getHandlerForType(msgType uint8) Handler {
	switch msgType {
	case MessageTypeAdvertise:
		return pd.handleAdvertise
	case MessageTypeGetPeers:
		return pd.handleGetPeers
	case MessageTypePing:
		return pd.handleGetPeers
	default:
		return nil
	}
}

func (pd *PEXDiscovery) handleAdvertise(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	pd.peersMutex.RLock()
	for i, v := range pd.peers[msg.NetworkID] {
		peerArr := pd.peers[msg.NetworkID]
		pd.peersMutex.Lock()
		if v.ID.String() == msg.PeerInfo.ID.String() {
			// delete old peer info
			peerArr[i] = peerArr[len(peerArr)-1]
			peerArr[len(peerArr)-1] = nil
			peerArr = peerArr[:len(peerArr)-1]
		}
		peerArr = append(peerArr, &msg.PeerInfo)
		pd.peers[msg.NetworkID] = peerArr
		pd.peersMutex.Unlock()
		if v, ok := pd.newPeers[msg.NetworkID]; ok {
			v.count++
			if v.count > v.maxCount {
				close(v.NewPeersChannel)
				delete(pd.newPeers, msg.NetworkID)
				continue
			}
			v.NewPeersChannel <- msg.PeerInfo.AddrInfo
		}
	}
	pd.peersMutex.RUnlock()
	return nil, nil
}

func (pd *PEXDiscovery) handleGetPeers(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	peers := pd.getPeers(msg.NetworkID, int(msg.GetPeersCount))
	return &PEXMessage{
		Type:  MessageTypeGetPeers,
		Peers: peers,
	}, nil
}

func (pd *PEXDiscovery) handlePing(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	return &PEXMessage{
		Type: MessageTypePong,
	}, nil
}
