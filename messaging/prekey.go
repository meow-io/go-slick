package messaging

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"

	"github.com/kevinburke/nacl"
	"github.com/kevinburke/nacl/box"
	"github.com/kevinburke/nacl/scalarmult"
	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/crypto"
	"github.com/meow-io/go-slick/ids"
)

const (
	prekeyConfirmKey = "PREKEY_CONFIRM_KEY"
	prekeyMacKey     = "PREKEY_MAC_KEY"
	prekeySessionKey = "PREKEY_SESSION_KEY"
	prekeySelect     = "PREKEY_SELECT"
)

type prekeyState struct {
	ID                []byte `db:"id"`
	Nonce             []byte `db:"nonce"`
	Initiator         bool   `db:"initiator"`
	PrivKey           []byte `db:"priv_key"`
	OtherPublicKey    []byte `db:"other_public_key"`
	GroupID           []byte `db:"group_id"`
	IdentityID        []byte `db:"identity_id"`
	MembershipID      []byte `db:"membership_id"`
	OtherIdentityID   []byte `db:"other_identity_id"`
	OtherMembershipID []byte `db:"other_membership_id"`
	OtherSigningKey   []byte `db:"other_signing_key"`
	PrivSigningKey    []byte `db:"priv_signing_key"`
}

type deferredPrekeyState struct {
	ID             []byte `db:"id"`
	OtherNonce     []byte `db:"other_nonce"`
	OtherSig1      []byte `db:"other_sig1"`
	OtherPublicKey []byte `db:"other_public_key"`
	FromURL        string `db:"from_url"`
}

func (ps *prekeyState) prekey1() *prekey1 {
	pubKey := ps.publicKey()
	msg := concat(ps.Nonce, ps.IdentityID, ps.MembershipID, ps.OtherIdentityID, ps.OtherMembershipID, pubKey[:])
	sig := ed25519.Sign(ps.PrivSigningKey, msg)
	return &prekey1{
		Nonce:     [16]byte(ps.Nonce),
		PublicKey: ps.publicKey(),
		Sig1:      [64]byte(sig),
	}
}

func (ps *prekeyState) processPrekey1(g *group, gb *GroupDescription, pk1 *deferredPrekeyState) (*prekey2, bool, error) {
	selfIdentityID := ids.IDFromBytes(g.SelfIdentityID)
	selfMembershipID := ids.IDFromBytes(g.SelfMembershipID)
	for identityID, identity := range gb.Identities {
		for membershipID, membership := range identity {
			if selfIdentityID == identityID && selfMembershipID == membershipID {
				continue
			}
			if _, ok := membership.Description.Endpoints[pk1.FromURL]; !ok {
				continue
			}
			msg := concat(pk1.OtherNonce[:], identityID[:], membershipID[:], selfIdentityID[:], selfMembershipID[:], pk1.OtherPublicKey)
			if !ed25519.Verify(membership.Description.IntroKey[:], msg, pk1.OtherSig1[:]) {
				continue
			}
			ps.Nonce = pk1.OtherNonce
			ps.OtherPublicKey = pk1.OtherPublicKey
			ps.OtherSigningKey = membership.Description.IntroKey[:]
			ps.OtherIdentityID = identityID[:]
			ps.OtherMembershipID = membershipID[:]

			e1e2 := *box.Precompute(nacl.Key(ps.OtherPublicKey[:]), nacl.Key(ps.PrivKey))
			keyMac := hmac.New(sha256.New, e1e2[:])
			keyMac.Write([]byte(prekeyMacKey))
			s := keyMac.Sum(nil)

			pk := ps.publicKey()
			mac := hmac.New(sha256.New, s)
			if _, err := mac.Write(concat(ps.Nonce, ps.IdentityID, ps.MembershipID, ps.OtherPublicKey[:], pk[:])); err != nil {
				return nil, false, err
			}
			hmac := mac.Sum(nil)
			sig2 := ed25519.Sign(ps.PrivSigningKey, hmac)
			return &prekey2{
				Nonce:     [16]byte(ps.Nonce),
				PublicKey: pk,
				Sig2:      [64]byte(sig2),
			}, true, nil
		}
	}
	return nil, false, nil
}

func (ps *prekeyState) processPrekey2(pk2 *prekey2) (*prekey3, bool, error) {
	// 2          [prekey pass2](#1217-prekey-pass1)
	// <---- e2+pub
	// 			n1
	// 			sig2=sign(s2, mac(s, n1 || id2 || e1+pub || e2+pub))

	e1e2 := *box.Precompute(nacl.Key(pk2.PublicKey[:]), nacl.Key(ps.PrivKey))
	keyMac := hmac.New(sha256.New, e1e2[:])
	keyMac.Write([]byte(prekeyMacKey))
	s := keyMac.Sum(nil)
	pk := ps.publicKey()
	mac := hmac.New(sha256.New, s)
	if _, err := mac.Write(concat(ps.Nonce, ps.OtherIdentityID, ps.OtherMembershipID, pk[:], pk2.PublicKey[:])); err != nil {
		return nil, false, err
	}
	verifyHmac := mac.Sum(nil)
	if !ed25519.Verify(ps.OtherSigningKey, verifyHmac, pk2.Sig2[:]) {
		return nil, false, nil
	}

	mac = hmac.New(sha256.New, s)
	if _, err := mac.Write(concat(ps.Nonce, ps.IdentityID, ps.MembershipID, pk2.PublicKey[:], pk[:])); err != nil {
		return nil, false, err
	}
	signedHmac := mac.Sum(nil)
	sig2 := ed25519.Sign(ps.PrivSigningKey, signedHmac)
	ps.OtherPublicKey = pk2.PublicKey[:]

	return &prekey3{
		Nonce: [16]byte(ps.Nonce),
		Sig3:  [64]byte(sig2),
	}, true, nil
}

func (ps *prekeyState) processPrekey3(pk3 *prekey3, desc *GroupDescription) (*prekey4, bool, error) {
	e1e2 := *box.Precompute(nacl.Key(ps.OtherPublicKey[:]), nacl.Key(ps.PrivKey))
	keyMac := hmac.New(sha256.New, e1e2[:])
	keyMac.Write([]byte(prekeyMacKey))
	s := keyMac.Sum(nil)
	pk := ps.publicKey()
	mac := hmac.New(sha256.New, s)
	if _, err := mac.Write(concat(ps.Nonce, ps.OtherIdentityID, ps.OtherMembershipID, pk[:], ps.OtherPublicKey)); err != nil {
		return nil, false, err
	}
	verifyHmac := mac.Sum(nil)
	if !ed25519.Verify(ps.OtherSigningKey, verifyHmac, pk3.Sig3[:]) {
		return nil, false, nil
	}
	// k1 = hash(e1 ^ e2+pub || "PREKEY_CONFIRM_KEY" || id1)
	confirmMac := hmac.New(sha256.New, e1e2[:])
	confirmMac.Write(concat([]byte(prekeyConfirmKey), ps.IdentityID, ps.MembershipID))
	k := confirmMac.Sum(nil)

	descSig, err := signGroupDesc(ps.PrivSigningKey, ids.ID(ps.IdentityID), ids.ID(ps.MembershipID), desc)
	if err != nil {
		return nil, false, err
	}
	innerPrekey := &prekeyInner{
		Sig:  descSig,
		Desc: desc,
	}
	innerBytes, err := bencode.Serialize(innerPrekey)
	if err != nil {
		return nil, false, err
	}
	encryptedInner, err := crypto.EncryptWithKey(k, innerBytes, nil)
	if err != nil {
		return nil, false, err
	}
	return &prekey4{
		Nonce: [16]byte(ps.Nonce),
		Desc1: encryptedInner,
	}, true, nil
}

func (ps *prekeyState) processPrekey4(pk4 *prekey4, desc *GroupDescription) (*prekey5, *GroupDescription, bool, error) {
	e1e2 := *box.Precompute(nacl.Key(ps.OtherPublicKey[:]), nacl.Key(ps.PrivKey))
	confirmMac := hmac.New(sha256.New, e1e2[:])
	confirmMac.Write(concat([]byte(prekeyConfirmKey), ps.OtherIdentityID, ps.OtherMembershipID))
	k := confirmMac.Sum(nil)
	decryptedInner, err := crypto.DecryptWithKey(k, pk4.Desc1, nil)
	if err != nil {
		return nil, nil, false, err
	}
	var otherInner prekeyInner
	if err := bencode.Deserialize(decryptedInner, &otherInner); err != nil {
		return nil, nil, false, err
	}
	ok, err := verifyGroupDesc(otherInner.Sig, ids.ID(ps.OtherIdentityID), ids.ID(ps.OtherMembershipID), otherInner.Desc)
	if err != nil {
		return nil, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	// k1 = hash(e1 ^ e2+pub || "PREKEY_CONFIRM_KEY" || id1)
	keyMac := hmac.New(sha256.New, e1e2[:])
	keyMac.Write(concat([]byte(prekeyConfirmKey), ps.IdentityID, ps.MembershipID))
	k = keyMac.Sum(nil)
	descSig, err := signGroupDesc(ps.PrivSigningKey, ids.ID(ps.IdentityID), ids.ID(ps.MembershipID), desc)
	if err != nil {
		return nil, nil, false, err
	}
	innerPrekey := &prekeyInner{
		Sig:  descSig,
		Desc: desc,
	}
	innerBytes, err := bencode.Serialize(innerPrekey)
	if err != nil {
		return nil, nil, false, err
	}
	encryptedInner, err := crypto.EncryptWithKey(k, innerBytes, nil)
	if err != nil {
		return nil, nil, false, err
	}
	return &prekey5{
		Nonce: [16]byte(ps.Nonce),
		Desc2: encryptedInner,
	}, otherInner.Desc, true, nil
}

func (ps *prekeyState) processPrekey5(pk5 *prekey5) (*GroupDescription, bool, error) {
	e1e2 := *box.Precompute(nacl.Key(ps.OtherPublicKey[:]), nacl.Key(ps.PrivKey))
	confirmMac := hmac.New(sha256.New, e1e2[:])
	confirmMac.Write(concat([]byte(prekeyConfirmKey), ps.OtherIdentityID, ps.OtherMembershipID))
	k := confirmMac.Sum(nil)
	decryptedInner, err := crypto.DecryptWithKey(k, pk5.Desc2, nil)
	if err != nil {
		return nil, false, err
	}
	var otherInner prekeyInner
	if err := bencode.Deserialize(decryptedInner, &otherInner); err != nil {
		return nil, false, err
	}
	ok, err := verifyGroupDesc(otherInner.Sig, ids.ID(ps.OtherIdentityID), ids.ID(ps.OtherMembershipID), otherInner.Desc)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return otherInner.Desc, true, nil
}

func (ps *prekeyState) publicKey() [32]byte {
	return *scalarmult.Base((*[32]byte)(ps.PrivKey))
}

func (ps *prekeyState) sessionKey() []byte {
	e1e2 := *box.Precompute(nacl.Key(ps.OtherPublicKey[:]), nacl.Key(ps.PrivKey))
	mac := hmac.New(sha256.New, e1e2[:])
	mac.Write(concat([]byte(prekeySessionKey)))
	return mac.Sum(nil)
}
