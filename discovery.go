package pex

import (
	"container/list"
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
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	"sync"
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

type synchronizedList struct {
	*list.List
	mutex *sync.RWMutex
}

func (sl *synchronizedList) elementAt(i int) *list.Element {
	if i >= sl.Len() {
		return nil
	}
	curIndex := 0
	firstIndexPassed := false
	var res *list.Element
	for e := sl.Front(); e != nil; e = e.Next() {
		if firstIndexPassed {
			curIndex++
		} else {
			firstIndexPassed = true
		}

		if i == curIndex {
			res = e
			break
		} else {
			continue
		}
	}
	return res
}

func (sl *synchronizedList) deleteElement(e *list.Element) interface{} {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	v := sl.Remove(e)
	return v
}

type PEXDiscovery struct {
	bootstrapNodes          []ma.Multiaddr
	host                    host.Host
	newPeers                sync.Map
	peers                   sync.Map
	updateStopper           chan bool
	discoveryNetworkManager *DiscoveryNetworkManager
}

type newPeersChannelWrapper struct {
	Channel  chan peer.AddrInfo
	count    int
	maxCount int
	mutex    sync.Mutex
}

func NewPEXDiscovery(h host.Host, bootstrapNodes []ma.Multiaddr, updateInterval time.Duration) (*PEXDiscovery, error) {
	pd := &PEXDiscovery{host: h}
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
			for _, newPeer := range resp.Peers {
				newPeer.AddedTs = time.Now().UTC()
				pd.addOrUpdatePeer(newPeer.NetworkID, newPeer)
			}
		}
	}

	pd.peers.Range(func(networkID, value interface{}) bool {
		peers := value.(*synchronizedList)

		// invalidate peers which have expired ttl
		peers.mutex.RLock()
		for e := peers.Front(); e != nil; e = e.Next() {
			peer := e.Value.(*peerInfo)
			if peer.TTL == 0 {
				continue
			}
			if time.Since(peer.AddedTs) > peer.TTL {
				// remove expired peer
				peers.mutex.RUnlock()
				peers.deleteElement(e)
				peers.mutex.RLock()
			}
		}
		peers.mutex.RUnlock()

		go func(networkID string, peers *synchronizedList) {
			peers.mutex.RLock()
			peerListSize := peers.Len()

			if peerListSize == 0 {
				peers.mutex.RUnlock()
				return
			}

			// select random peer from our list
			randomPeerIndex := Random.Intn(peerListSize)

			el := peers.elementAt(randomPeerIndex)
			p := el.Value.(*peerInfo)
			peers.mutex.RUnlock()

			pingMessage := &PEXMessage{
				Type: MessageTypePing,
			}

			// ping him
			resp, err := pd.discoveryNetworkManager.sendRequest(context.TODO(), p.AddrInfo.ID, pingMessage)
			if err != nil {
				pdLog.Debugf("failed to send ping to %s: %s, we are %s", p.AddrInfo.ID, err, pd.host.ID())
				pdLog.Debugf("deleting peer %s", p.AddrInfo.ID)
				peers.deleteElement(el)
				return
			}

			if resp.Type != MessageTypePong {
				pdLog.Debugf("incorrect ping response")
				pdLog.Debugf("deleting peer %s", p.AddrInfo.ID)
				peers.deleteElement(el)
				return
			}

			// get new peers from him
			getPeersMessage := &PEXMessage{
				Type:          MessageTypeGetPeers,
				GetPeersCount: DefaultGetPeersMaxCount,
				NetworkID:     networkID,
			}
			resp, err = pd.discoveryNetworkManager.sendRequest(context.TODO(), p.AddrInfo.ID, getPeersMessage)
			if err != nil {
				pdLog.Debugf("failed to send get peers request to %s: %s, we are %s", p.AddrInfo.ID, err, pd.host.ID())
				pdLog.Debugf("deleting peer %s", p.AddrInfo.ID)
				peers.deleteElement(el)
				return
			}

			for _, newPeer := range resp.Peers {
				newPeer.AddedTs = time.Now().UTC()
				pd.addOrUpdatePeer(newPeer.NetworkID, newPeer)
			}
		}(networkID.(string), peers)

		return true
	})
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

	c, _ := pd.newPeers.LoadOrStore(ns, &newPeersChannelWrapper{
		Channel:  make(chan peer.AddrInfo),
		maxCount: dOpts.Limit,
	})
	nwpc := c.(*newPeersChannelWrapper)
	return nwpc.Channel, nil
}

func (pd *PEXDiscovery) getPeers(requester peer.ID, ns string, maxCount int) []*peerInfo {

	rand.Seed(time.Now().UnixNano())
	var peers []*peerInfo

	curCount := 0

	// randomly select peers which we have
	if ns == "" {
		pd.peers.Range(func(networkID, value interface{}) bool {
			peerList := value.(*synchronizedList)
			peerList.mutex.RLock()
			for e := peerList.Front(); e != nil; e = e.Next() {
				curCount++
				if curCount > maxCount {
					return false
				}
				randPeerNum := Random.Intn(peerList.Len())
				randEl := peerList.elementAt(randPeerNum)
				p := randEl.Value.(*peerInfo)
				if p.AddrInfo.ID.String() == requester.String() {
					continue
				}
				var isAlreadyExistInResList bool
				for _, v := range peers {
					if v.AddrInfo.ID.String() == p.AddrInfo.ID.String() {
						isAlreadyExistInResList = true
					}
				}
				if isAlreadyExistInResList {
					continue
				}
				peers = append(peers, p)
			}
			peerList.mutex.RUnlock()
			return true
		})
	} else {
		v, ok := pd.peers.Load(ns)
		if !ok {
			return peers
		}
		peerList := v.(*synchronizedList)
		peerList.mutex.RLock()
		for e := peerList.Front(); e != nil; e = e.Next() {
			curCount++
			if curCount > maxCount {
				break
			}
			randPeerNum := Random.Intn(peerList.Len())
			randEl := peerList.elementAt(randPeerNum)
			p := randEl.Value.(*peerInfo)
			if p.AddrInfo.ID.String() == requester.String() {
				continue
			}
			var isAlreadyExistInResList bool
			for _, v := range peers {
				if v.AddrInfo.ID.String() == p.AddrInfo.ID.String() {
					isAlreadyExistInResList = true
				}
			}
			if isAlreadyExistInResList {
				continue
			}
			peers = append(peers, p)
		}
		peerList.mutex.RUnlock()
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

func (pd *PEXDiscovery) addOrUpdatePeer(networkID string, pi *peerInfo) {
	if pi == nil {
		return
	}

	if pi.AddrInfo.ID.String() == pd.host.ID().String() {
		dnLog.Debugf("Attempt to add self (%s) to peer list, we are %s", pi.AddrInfo.ID, pd.host.ID())
		return
	}

	value, _ := pd.peers.LoadOrStore(networkID, &synchronizedList{List: list.New(), mutex: &sync.RWMutex{}})
	peers := value.(*synchronizedList)
	peers.mutex.Lock()
	for e := peers.Front(); e != nil; e = e.Next() {
		peer := e.Value.(*peerInfo)
		if peer.AddrInfo.ID.String() == pi.AddrInfo.ID.String() {
			peers.Remove(e)
		}
	}
	pd.host.Peerstore().AddAddrs(pi.AddrInfo.ID, pi.AddrInfo.Addrs, peerstore.ProviderAddrTTL)
	peers.PushFront(pi)
	peers.mutex.Unlock()

	// notify subscribers about new peer
	_ = pd.notifyAboutNewPeer(pi.NetworkID, pi.AddrInfo)
}

func (pd *PEXDiscovery) notifyAboutNewPeer(networkID string, addrInfo peer.AddrInfo) error {
	if v, ok := pd.newPeers.Load(networkID); ok {
		npcw := v.(*newPeersChannelWrapper)
		npcw.mutex.Lock()
		defer npcw.mutex.Unlock()
		npcw.count++
		if npcw.count > npcw.maxCount {
			close(npcw.Channel)
			pd.newPeers.Delete(networkID)
			return fmt.Errorf("limit of found peers has reached")
		}
		npcw.Channel <- addrInfo
	}
	return nil
}

func (pd *PEXDiscovery) handleGetPeers(ctx context.Context, peerID peer.ID, msg *PEXMessage) (*PEXMessage, error) {
	pdLog.Debugf("received get peers request from %s, we are %s", peerID.String(), pd.host.ID())
	peers := pd.getPeers(peerID, msg.NetworkID, int(msg.GetPeersCount))
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
