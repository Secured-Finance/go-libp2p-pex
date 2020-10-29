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
	"testing"
	"time"
)

func TestDiscovery(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	// setup new libp2p hosts - one bootstrap and two regular peers
	rand.Seed(time.Now().UnixNano())
	port1 := rand.Intn(100) + 10000
	port2 := port1 + 1
	port3 := port2 + 1
	port4 := port3 + 1
	port5 := port4 + 1

	bNode := makeNode(port1)
	_, err := NewPEXDiscovery(bNode, nil, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Bootstrap ID: %s", bNode.ID())

	rNode1 := makeNode(port2)
	t.Logf("Node 1 ID: %s", rNode1.ID())
	rNode2 := makeNode(port3)
	t.Logf("Node 2 ID: %s", rNode2.ID())
	rNode3 := makeNode(port4)
	t.Logf("Node 3 ID: %s", rNode3.ID())
	rNode4 := makeNode(port5)
	t.Logf("Node 4 ID: %s", rNode4.ID())

	bootstrapMultiaddrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{bNode.ID(), bNode.Addrs()})
	if err != nil {
		t.Fatal(err)
	}

	d1, err := NewPEXDiscovery(rNode1, []ma.Multiaddr{bootstrapMultiaddrs[0]}, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewPEXDiscovery(rNode2, []ma.Multiaddr{bootstrapMultiaddrs[0]}, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d3, err := NewPEXDiscovery(rNode3, []ma.Multiaddr{bootstrapMultiaddrs[0]}, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d4, err := NewPEXDiscovery(rNode4, []ma.Multiaddr{bootstrapMultiaddrs[0]}, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// setup new peers listener
	go func() {
		newPeers, err := d1.FindPeers(context.TODO(), "test", discovery.Limit(10))
		if err != nil {
			t.Fatal(err)
		}
		for {
			p, ok := <-newPeers
			if !ok {
				t.Log("received all peers")
				return
			}
			t.Logf("d1 new peer: %s", p)
		}
	}()

	go func() {
		newPeers, err := d2.FindPeers(context.TODO(), "test", discovery.Limit(10))
		if err != nil {
			t.Fatal(err)
		}
		for {
			p, ok := <-newPeers
			if !ok {
				t.Log("received all peers")
				return
			}
			t.Logf("d2 new peer: %s", p)
		}
	}()

	go func() {
		newPeers, err := d3.FindPeers(context.TODO(), "test", discovery.Limit(10))
		if err != nil {
			t.Fatal(err)
		}
		for {
			p, ok := <-newPeers
			if !ok {
				t.Log("received all peers")
				return
			}
			t.Logf("d3 new peer: %s", p)
		}
	}()

	go func() {
		newPeers, err := d4.FindPeers(context.TODO(), "test", discovery.Limit(10))
		if err != nil {
			t.Fatal(err)
		}
		for {
			p, ok := <-newPeers
			if !ok {
				t.Log("received all peers")
				return
			}
			t.Logf("d4 new peer: %s", p)
		}
	}()

	// do advertising for regular peers
	_, err = d1.Advertise(context.TODO(), "test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d2.Advertise(context.TODO(), "test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d3.Advertise(context.TODO(), "test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = d4.Advertise(context.TODO(), "test")
	if err != nil {
		t.Fatal(err)
	}

	select {}
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
