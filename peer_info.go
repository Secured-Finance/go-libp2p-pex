package pex

import (
	"github.com/fxamacker/cbor/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"time"
)

type peerInfo struct {
	AddrInfo  addrInfo
	TTL       time.Duration `json:"ttl"`
	NetworkID string        `json:"networkID"`
	AddedTs   time.Time     `json:"-"`
}

type addrInfo peer.AddrInfo

type rawAddrInfo struct {
	ID []byte
	Addrs [][]byte
}

func (a addrInfo) MarshalCBOR() ([]byte, error) {
	var rawInfo rawAddrInfo

	for _, v := range a.Addrs {
		addrData, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		rawInfo.Addrs = append(rawInfo.Addrs, addrData)
	}

	rawInfo.ID = []byte(a.ID)
	data, err := cbor.Marshal(rawInfo)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (a *addrInfo) UnmarshalCBOR(data []byte) error {
	ai := peer.AddrInfo{}

	var rawInfo rawAddrInfo
	err := cbor.Unmarshal(data, &rawInfo)
	if err != nil {
		return err
	}


	var addrs []multiaddr.Multiaddr
	for _, v := range rawInfo.Addrs {
		m, err := multiaddr.NewMultiaddrBytes(v)
		if err != nil {
			return err
		}
		addrs = append(addrs, m)
	}

	ai.Addrs = addrs
	ai.ID = peer.ID(rawInfo.ID)

	*a = addrInfo(ai)
	return nil
}