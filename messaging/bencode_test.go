package messaging

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergingEmptyMembership(t *testing.T) {
	require := require.New(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	require.Nil(err)

	id := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	aDesc := &MembershipDescription{
		Version:  0,
		Protocol: 0,
		Endpoints: map[string]*EndpointInfo{
			"link://something": {Priority: 0},
		},
		IntroKey: [32]byte(pub),
	}

	sig, err := signMembershipDesc(priv, id, id, aDesc)
	require.Nil(err)

	a := &GroupDescription{
		Name:        &LWWString{Value: "hi", Time: 0},
		Description: &LWWString{Value: "hi", Time: 0},
		Icon:        &LWWBlob{Time: 0},
		IconType:    &LWWString{},
		Identities:  IdentityMap{id: {id: &Membership{Description: aDesc, Signature: sig[:]}}},
	}

	b := &GroupDescription{
		Name:        &LWWString{Value: "hi", Time: 0},
		Description: &LWWString{Value: "hi", Time: 0},
		Icon:        &LWWBlob{Time: 0},
		IconType:    &LWWString{},
		Identities:  IdentityMap{id: {id: &Membership{Signature: []byte{}, Description: &MembershipDescription{Version: 1, Protocol: 0, IntroKey: [32]byte(pub), Endpoints: map[string]*EndpointInfo{}}}}},
	}

	c, err := a.merge(b)
	require.Nil(err)
	require.Equal(0, len(c.Identities[id][id].Description.Endpoints))
}

func TestMergingLWWString(t *testing.T) {
	require := require.New(t)
	a := &LWWString{"hi", 0}
	b := &LWWString{"there", 1}

	c := a.merge(b)
	require.Equal(uint64(1), c.Time)
	require.Equal("there", c.Value)
}
