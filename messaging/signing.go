package messaging

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/ids"
)

func signMembershipDesc(priv ed25519.PrivateKey, identityID, membershipID ids.ID, desc *MembershipDescription) ([64]byte, error) {
	if !bytes.Equal(priv.Public().(ed25519.PublicKey), desc.IntroKey[:]) {
		return [64]byte{}, fmt.Errorf("expected public key to be %x, got %x", priv.Public(), desc.IntroKey)
	}

	msg, err := constructDescMessage(identityID, membershipID, desc)
	if err != nil {
		return [64]byte{}, err
	}
	return [64]byte(ed25519.Sign(priv, msg)), nil
}

func verifyMembershipDesc(sig [64]byte, identityID, membershipID ids.ID, desc *MembershipDescription) (bool, error) {
	msg, err := constructDescMessage(identityID, membershipID, desc)
	if err != nil {
		return false, err
	}
	return ed25519.Verify(desc.IntroKey[:], msg, sig[:]), nil
}

func signGroupDesc(priv ed25519.PrivateKey, identityID, membershipID ids.ID, desc *GroupDescription) ([64]byte, error) {
	if !bytes.Equal(priv.Public().(ed25519.PublicKey), desc.Identities[identityID][membershipID].Description.IntroKey[:]) {
		return [64]byte{}, fmt.Errorf("expected public key to be %x, got %x", priv.Public(), desc.Identities[identityID][membershipID].Description.IntroKey)
	}

	msg, err := constructDescMessage(identityID, membershipID, desc)
	if err != nil {
		return [64]byte{}, err
	}
	return [64]byte(ed25519.Sign(priv, msg)), nil
}

func verifyGroupDesc(sig [64]byte, identityID, membershipID ids.ID, desc *GroupDescription) (bool, error) {
	msg, err := constructDescMessage(identityID, membershipID, desc)
	if err != nil {
		return false, err
	}
	return ed25519.Verify(desc.Identities[identityID][membershipID].Description.IntroKey[:], msg, sig[:]), nil
}

func constructDescMessage(identityID, membershipID ids.ID, other interface{}) ([]byte, error) {
	b, err := bencode.Serialize(other)
	if err != nil {
		return nil, err
	}
	return concat(identityID[:], membershipID[:], b), nil
}

func concat(parts ...[]byte) []byte {
	msg := []byte{}
	for _, m := range parts {
		msg = binary.BigEndian.AppendUint64(msg, uint64(len(m)))
		msg = append(msg, m...)
	}
	return msg
}
