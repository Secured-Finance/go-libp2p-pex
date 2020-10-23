package pex

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

type peerInfo struct {
	peer.AddrInfo
	TTL time.Duration
}
