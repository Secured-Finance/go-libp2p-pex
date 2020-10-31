package pex

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestDiscovery(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	// setup new libp2p hosts - one bootstrap and two regular peers
	rand.Seed(time.Now().UnixNano())
	port1 := rand.Intn(100) + 10000

	bNode := makeNode(port1)
	_, err := NewPEXDiscovery(bNode, nil, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Bootstrap ID: %s", bNode.ID())

	bootstrapMultiaddrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{bNode.ID(), bNode.Addrs()})
	if err != nil {
		t.Fatal(err)
	}

	maxNodes := 8
	port := port1

	resultingPeerSet := &sync.Map{}
	var wg sync.WaitGroup
	wg.Add(maxNodes)

	for i := 1; i <= maxNodes; i++ {
		port += 1
		node := makeNode(port)
		t.Logf("Node %d ID: %s", i, node.ID())
		d, err := NewPEXDiscovery(node, []ma.Multiaddr{bootstrapMultiaddrs[0]}, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// setup new peers listener
		go func(i int) {
			newPeers, err := d.FindPeers(context.TODO(), "test", discovery.Limit(maxNodes-1))
			if err != nil {
				t.Fatal(err)
			}
			for {
				p, ok := <-newPeers
				if !ok {
					t.Log("received all peers")
					wg.Done()
					return
				}
				m, _ := resultingPeerSet.LoadOrStore(node.ID(), &sync.Map{})
				foundNodes := m.(*sync.Map)
				foundNodes.Store(p.ID.String(), p)
			}
		}(i)

		// advertise node
		_, err = d.Advertise(context.TODO(), "test")
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
	resultingPeerSet.Range(func(node, value interface{}) bool {
		i := 0
		foundNodes := value.(*sync.Map)
		foundNodes.Range(func(key, value interface{}) bool {
			i++
			return true
		})

		if i != maxNodes-1 {
			t.Errorf("Expected %d found peers, got %d", maxNodes-1, i)
		}
		return true
	})
}

func makeNode(port int) host.Host {
	// Ignoring most errors for brevity
	// See echo example for more details and better implementation
	priv, _, _ := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	listen, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
	host, _ := libp2p.New(
		context.Background(),
		libp2p.ListenAddrs(listen),
		libp2p.Identity(priv),
	)

	return host
}
