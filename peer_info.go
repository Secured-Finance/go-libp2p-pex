package pex

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

type peerInfo struct {
	AddrInfo  peer.AddrInfo
	TTL       time.Duration `json:"ttl"`
	NetworkID string        `json:"networkID"`
	AddedTs   time.Time     `json:"-"`
}
