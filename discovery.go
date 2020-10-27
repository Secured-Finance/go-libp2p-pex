package pex

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sasha-s/go-deadlock"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	"time"
)

const (
	PEXProtocolID = "/pex/1.0.0"

	DefaultGetPeersMaxCount = 30
)

type cryptoSource struct{}

func (s cryptoSource) Seed(seed int64) {}

func (s cryptoSource) Int63() int64 {
	return int64(s.Uint64() & ^uint64(1<<63))
}

func (s cryptoSource) Uint64() (v uint64) {
	err := binary.Read(crand.Reader, binary.BigEndian, &v)
	if err != nil {
		log.Fatal(err)
	}
	return v
}

var (
	Random = rand.New(cryptoSource{})
)

var pdLog = logrus.WithFields(logrus.Fields{
	"subsystem": "pex",
})

type PEXDiscovery struct {
	bootstrapNodes          []ma.Multiaddr
	host                    host.Host
	peersMutex              deadlock.RWMutex
	newPeers                map[string]*newPeersChannelWrapper
	newPeersMutex           deadlock.RWMutex
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

	if pd.bootstrapNodes != nil {
		for _, n := range pd.bootstrapNodes {
			bAddr, err := peer.AddrInfoFromP2pAddr(n)
			if err != nil {
				pdLog.Error(err)
				break
			}

			// get new peers from bootstrap
			getPeersMessage := &PEXMessage{
				Type:          MessageTypeGetPeers,
				GetPeersCount: DefaultGetPeersMaxCount,
				NetworkID:     "",
			}
			resp, err := pd.discoveryNetworkManager.sendRequest(context.TODO(), bAddr.ID, getPeersMessage)
			if err != nil {
				pdLog.Error(err)
				break
			}

			// add new peers to peer list
			pd.peersMutex.RUnlock()
			for _, newPeer := range resp.Peers {
				newPeer.AddedTs = time.Now().UTC()
				pd.addOrUpdatePeer(newPeer.NetworkID, &newPeer)
			}
			pd.peersMutex.RLock()
		}
	}

	for networkID, peers := range pd.peers {
		for i, p := range peers {
			// invalidate peers which have expired ttl
			if p.TTL == 0 {
				continue
			}
			if time.Since(p.AddedTs) > p.TTL {
				pd.peersMutex.RUnlock()
				peers = pd.deletePeer(peers, i)
				pd.peers[networkID] = peers
				pd.peersMutex.RLock()
			}
		}

		go func(peers []*peerInfo, networkID string) {
			pd.peersMutex.RLock()
			peerListSize := len(peers)

			// select random peer from our list
			if peerListSize == 0 {
				return
			}

			randomPeerIndex := Random.Intn(peerListSize)

			pingMessage := &PEXMessage{
				Type: MessageTypePing,
			}

			// ping him
			resp, err := pd.discoveryNetworkManager.sendRequest(context.TODO(), peers[randomPeerIndex].ID, pingMessage)
			if err != nil {
				pdLog.Debugf("failed to send ping to %s: %s, we are %s", peers[randomPeerIndex].ID, err, pd.host.ID())
				pd.peersMutex.RUnlock()
				pdLog.Debugf("deleting peer %s", peers[randomPeerIndex].ID)
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
				return
			}
			if resp.Type != MessageTypePong {
				pdLog.Debugf("incorrect ping response")
				pd.peersMutex.RUnlock()
				pdLog.Debugf("deleting peer %s", peers[randomPeerIndex].ID)
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
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
				pdLog.Debugf("failed to send get peers request: %s", err)
				pd.peersMutex.RUnlock()
				pd.peersMutex.Lock()
				pdLog.Debugf("deleting peer %s", peers[randomPeerIndex].ID)
				pd.peers[networkID] = pd.deletePeer(peers, randomPeerIndex)
				pd.peersMutex.Unlock()
				return
			}

			// add new peers to peer list
			pd.peersMutex.RUnlock()
			for _, newPeer := range resp.Peers {
				newPeer.AddedTs = time.Now().UTC()
				pd.addOrUpdatePeer(newPeer.NetworkID, &newPeer)
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
		}, dOpts.Ttl, ns, time.Time{},
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
			PeerInfo:  &pInfo,
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
	if ns == "" {
		for k, _ := range pd.peers {
			for _, _ = range pd.peers[k] {
				curCount++
				if curCount > maxCount {
					break
				}

				peerListSize := len(pd.peers[k])
				randPeerNumber := Random.Intn(peerListSize)

				peerInfo := pd.peers[k][randPeerNumber]
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
		}
	} else {
		for _, _ = range pd.peers[ns] {
			curCount++
			if curCount > maxCount {
				break
			}
			randPeerNumber := Random.Intn(len(pd.peers[ns]))
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
		return pd.handlePing
	default:
		return nil
	}
}

func (pd *PEXDiscovery) handleAdvertise(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	pdLog.Debugf("received advertise request from %s", peerID.String())
	msg.PeerInfo.AddedTs = time.Now().UTC()
	pd.addOrUpdatePeer(msg.NetworkID, msg.PeerInfo)
	return nil, nil
}

func (pd *PEXDiscovery) addOrUpdatePeer(networkID string, peerInfo *peerInfo) {
	if peerInfo.ID == pd.host.ID() {
		return
	}
	peerInfo.NetworkID = networkID

	pd.peersMutex.RLock()
	for i, v := range pd.peers[networkID] {
		peers := pd.peers[networkID]
		if v.ID.String() == peerInfo.ID.String() {
			// delete old peer info

			pd.peersMutex.RUnlock()
			peers = pd.deletePeer(peers, i)
			pd.peersMutex.Lock()
			pd.peers[networkID] = peers
			pd.peersMutex.Unlock()
			pd.peersMutex.RLock()
		}
	}
	pd.peersMutex.RUnlock()
	pd.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.ProviderAddrTTL)

	pd.peersMutex.Lock()
	pd.peers[networkID] = append(pd.peers[networkID], peerInfo)
	pd.peersMutex.Unlock()

	// notify subscribers about new peer
	_ = pd.notifyAboutNewPeer(peerInfo.NetworkID, peerInfo.AddrInfo)
}

func (pd *PEXDiscovery) notifyAboutNewPeer(networkID string, addrInfo peer.AddrInfo) error {
	pd.newPeersMutex.RLock()
	defer pd.newPeersMutex.RUnlock()
	if v, ok := pd.newPeers[networkID]; ok {
		pd.newPeersMutex.RUnlock()
		pd.newPeersMutex.Lock()
		v.count++
		pd.newPeersMutex.Unlock()
		pd.newPeersMutex.RLock()
		if v.count > v.maxCount {
			close(v.NewPeersChannel)
			delete(pd.newPeers, networkID)
			return fmt.Errorf("limit of found peers has reached")
		}
		v.NewPeersChannel <- addrInfo
	}
	return nil
}

func (pd *PEXDiscovery) deletePeer(peers []*peerInfo, i int) []*peerInfo {
	pd.peersMutex.Lock()
	defer pd.peersMutex.Unlock()
	peers[i] = peers[len(peers)-1]
	peers[len(peers)-1] = nil
	peers = peers[:len(peers)-1]
	return peers
}

func (pd *PEXDiscovery) handleGetPeers(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	pdLog.Debugf("received get peers request from %s, we are %s", peerID.String(), pd.host.ID())
	peers := pd.getPeers(msg.NetworkID, int(msg.GetPeersCount))
	return &PEXMessage{
		Type:  MessageTypeGetPeers,
		Peers: peers,
	}, nil
}

func (pd *PEXDiscovery) handlePing(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	pdLog.Debugf("received ping from %s, we are %s", peerID.String(), pd.host.ID())
	return &PEXMessage{
		Type: MessageTypePong,
	}, nil
}
