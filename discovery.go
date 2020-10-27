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

	DefaultGetPeersMaxCount = 30
)

var pdLog = logrus.WithFields(logrus.Fields{
	"subsystem": "pex",
})

type PEXDiscovery struct {
	bootstrapNodes          []ma.Multiaddr
	host                    host.Host
	peersMutex              sync.RWMutex
	newPeers                map[string]*newPeersChannelWrapper
	newPeersMutex           sync.RWMutex
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
	pd.peersMutex.RLock()
	defer pd.peersMutex.RUnlock()
	for networkID, peers := range pd.peers {
		for i, p := range peers {
			// invalidate peers which have expired ttl
			if time.Since(p.AddedTs) > p.TTL {
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				peers = pd.deletePeer(peers, i)
				pd.peers[networkID] = peers
				pd.peersMutex.Unlock()
				pd.peersMutex.RLock()
			}
		}

		go func(peers []*peerInfo, networkID string) {
			pd.peersMutex.RLock()
			peerListSize := len(peers)
			rand.Seed(time.Now().Unix())
			// select random peer from our list
			randomPeerIndex := rand.Intn(peerListSize - 1)
			pingMessage := &PEXMessage{
				Type: MessageTypePing,
			}

			// ping him
			resp, err := pd.discoveryNetworkManager.sendRequest(context.TODO(), peers[randomPeerIndex].ID, pingMessage)
			if err != nil {
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
				pd.peersMutex.Unlock()
				return
			}
			if resp.Type != MessageTypePong {
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
				pd.peersMutex.Unlock()
				return
			}

			// get new peers from him
			getPeersMessage := &PEXMessage{
				Type:          MessageTypeGetPeers,
				GetPeersCount: DefaultGetPeersMaxCount,
				NetworkID:     networkID,
			}
			resp, err = pd.discoveryNetworkManager.sendRequest(context.TODO(), peers[randomPeerIndex].ID, getPeersMessage)
			if err != nil {
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
				pd.peersMutex.Unlock()
				return
			}
			for _, newPeer := range resp.Peers {
				newPeer.AddedTs = time.Now().UTC()
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				pd.peers[networkID] = append(pd.peers[networkID], &newPeer)
				pd.peersMutex.Unlock()
			}
		}(peers, networkID)
	}
}

func (pd *PEXDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	dOpts := discovery.Options{}
	_ = dOpts.Apply(opts...)

	pInfo := peerInfo{
		peer.AddrInfo{
			ID:    pd.host.ID(),
			Addrs: pd.host.Addrs(),
		}, dOpts.Ttl, time.Time{},
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

func (pd *PEXDiscovery) getPeers(ns string, maxCount int) []peerInfo {
	pd.peersMutex.RLock()
	defer pd.peersMutex.RUnlock()

	rand.Seed(time.Now().UnixNano())
	var peers []peerInfo

	if len(pd.peers) == 0 {
		return peers
	}

	curCount := 0

	// randomly select peers which we have
	for _, _ = range pd.peers[ns] {
		curCount++
		if curCount > maxCount {
			break
		}
		randPeerNumber := rand.Intn(len(pd.peers[ns]) - 1)
		peerInfo := pd.peers[ns][randPeerNumber]
		var isAlreadyExistInResList bool
		for _, v := range peers {
			if v.ID.String() == peerInfo.ID.String() {
				isAlreadyExistInResList = true
			}
		}
		if isAlreadyExistInResList {
			continue
		}
		peers = append(peers, *peerInfo)
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
	defer pd.peersMutex.RUnlock()
	for i, v := range pd.peers[msg.NetworkID] {
		peers := pd.peers[msg.NetworkID]
		pd.peersMutex.RUnlock()
		pd.peersMutex.Lock()
		if v.ID.String() == msg.PeerInfo.ID.String() {
			// delete old peer info
			peers = pd.deletePeer(peers, i)
		}
		peers = append(peers, &msg.PeerInfo)
		pd.peers[msg.NetworkID] = peers
		pd.peersMutex.Unlock()
		pd.peersMutex.RLock()

		// notify subscribers about new peers
		pd.newPeersMutex.RLock()
		if v, ok := pd.newPeers[msg.NetworkID]; ok {
			pd.newPeersMutex.RUnlock()
			pd.newPeersMutex.Lock()
			v.count++
			if v.count > v.maxCount {
				close(v.NewPeersChannel)
				delete(pd.newPeers, msg.NetworkID)
				continue
			}
			v.NewPeersChannel <- msg.PeerInfo.AddrInfo
			pd.newPeersMutex.Unlock()
			pd.newPeersMutex.RLock()
		}
		pd.newPeersMutex.RUnlock()
	}
	return nil, nil
}

func (pd *PEXDiscovery) deletePeer(peers []*peerInfo, i int) []*peerInfo {
	peers[i] = peers[len(peers)-1]
	peers[len(peers)-1] = nil
	peers = peers[:len(peers)-1]
	return peers
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
