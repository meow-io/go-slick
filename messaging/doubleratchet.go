package messaging

import (
	"bytes"
	crypto_rand "crypto/rand"
	"errors"
	"fmt"

	"github.com/kevinburke/nacl/box"
	"github.com/meow-io/go-slick/crypto"
	"github.com/status-im/doubleratchet"
)

type dhPairImpl struct {
	privateKey [32]byte
	publicKey  [32]byte
}

func (pair dhPairImpl) PrivateKey() doubleratchet.Key {
	return pair.privateKey[:]
}

func (pair dhPairImpl) PublicKey() doubleratchet.Key {
	return pair.publicKey[:]
}

type sessionStorageImpl struct {
	db *database
}

func (ss *sessionStorageImpl) Load(id []byte) (*doubleratchet.State, error) {
	s, err := ss.db.doubleratchetState(id)
	if err != nil {
		return nil, err
	}

	drc := ss.db.doubleratchetCrypto()

	return &doubleratchet.State{
		Crypto: drc,
		DHr:    s.Dhr,
		DHs:    dhPairImpl{privateKey: *crypto.SliceToKey(s.DhsPriv), publicKey: *crypto.SliceToKey(s.DhsPub)},
		RootCh: struct {
			Crypto doubleratchet.KDFer
			CK     doubleratchet.Key
		}{Crypto: drc, CK: s.RootChKey},
		SendCh: struct {
			Crypto doubleratchet.KDFer
			CK     doubleratchet.Key
			N      uint32
		}{Crypto: drc, CK: s.SendChKey, N: s.SendChCount},
		RecvCh: struct {
			Crypto doubleratchet.KDFer
			CK     doubleratchet.Key
			N      uint32
		}{Crypto: drc, CK: s.RecvChKey, N: s.RecvChCount},
		PN:                       s.PN,
		MkSkipped:                keysStorageImpl{sessionID: id, db: ss.db},
		MaxSkip:                  s.MaxSkip,
		HKr:                      s.HKr,
		NHKr:                     s.NHKr,
		HKs:                      s.HKs,
		NHKs:                     s.NHKs,
		MaxKeep:                  s.MaxKeep,
		MaxMessageKeysPerSession: s.MaxMessageKeysPerSession,
		Step:                     s.Step,
		KeysCount:                s.KeysCount,
	}, nil
}

func (ss *sessionStorageImpl) Save(id []byte, state *doubleratchet.State) error {
	s := &doubleratchetState{
		ID:                       id,
		Dhr:                      state.DHr,
		DhsPub:                   state.DHs.PublicKey(),
		DhsPriv:                  state.DHs.PrivateKey(),
		RootChKey:                state.RootCh.CK,
		SendChKey:                state.SendCh.CK,
		SendChCount:              state.SendCh.N,
		RecvChKey:                state.RecvCh.CK,
		RecvChCount:              state.RecvCh.N,
		PN:                       state.PN,
		MaxSkip:                  state.MaxSkip,
		HKr:                      state.HKr,
		NHKr:                     state.NHKr,
		HKs:                      state.HKs,
		NHKs:                     state.NHKs,
		MaxKeep:                  state.MaxKeep,
		MaxMessageKeysPerSession: state.MaxMessageKeysPerSession,
		Step:                     state.Step,
		KeysCount:                state.KeysCount,
	}
	return ss.db.upsertDoubleratchetState(s)
}

type cryptoImpl struct {
	defaultCrypto doubleratchet.DefaultCrypto
}

func (c *cryptoImpl) GenerateDH() (doubleratchet.DHPair, error) {
	pubk, privk, err := box.GenerateKey(crypto_rand.Reader)
	if err != nil {
		return nil, err
	}

	return dhPairImpl{privateKey: *privk, publicKey: *pubk}, nil
}

func (c *cryptoImpl) DH(dhPair doubleratchet.DHPair, dhPub doubleratchet.Key) (doubleratchet.Key, error) {
	dhPairKey := crypto.SliceToKey(dhPair.PrivateKey())
	dhPubKey := crypto.SliceToKey(dhPub)
	out := box.Precompute(dhPubKey, dhPairKey)
	return out[:], nil
}

func (c *cryptoImpl) Encrypt(mk doubleratchet.Key, plaintext, ad []byte) ([]byte, error) {
	return crypto.EncryptWithKey(mk, plaintext, ad)
}

func (c *cryptoImpl) Decrypt(mk doubleratchet.Key, ciphertext, ad []byte) ([]byte, error) {
	return crypto.DecryptWithKey(mk, ciphertext, ad)
}

func (c *cryptoImpl) KdfRK(rk, dhOut doubleratchet.Key) (doubleratchet.Key, doubleratchet.Key, doubleratchet.Key) {
	return c.defaultCrypto.KdfRK(rk, dhOut)
}

func (c *cryptoImpl) KdfCK(ck doubleratchet.Key) (doubleratchet.Key, doubleratchet.Key) {
	return c.defaultCrypto.KdfCK(ck)
}

type keysStorageImpl struct {
	sessionID []byte
	db        *database
}

func (ks keysStorageImpl) Get(k doubleratchet.Key, msgNum uint) (doubleratchet.Key, bool, error) {
	kr, ok, err := ks.db.keyByMsgNum(ks.sessionID, k, msgNum)
	if !ok || err != nil {
		return doubleratchet.Key{}, ok, err
	}
	return kr.MessageKey, ok, err
}

func (ks keysStorageImpl) Put(sessionID []byte, k doubleratchet.Key, msgNum uint, mk doubleratchet.Key, keySeqNum uint) error {
	if !bytes.Equal(sessionID, ks.sessionID) {
		return fmt.Errorf("expected %x to equal %x", sessionID, ks.sessionID)
	}
	return ks.db.upsertKeyByMsgNum(sessionID, k, msgNum, mk, keySeqNum)
}

func (ks keysStorageImpl) DeleteMk(k doubleratchet.Key, msgNum uint) error {
	return ks.db.deleteKeyByMsgNum(ks.sessionID, k, msgNum)
}

func (ks keysStorageImpl) DeleteOldMks(sessionID []byte, deleteUntilSeqKey uint) error {
	if !bytes.Equal(sessionID, ks.sessionID) {
		return fmt.Errorf("expected %x to equal %x", sessionID, ks.sessionID)
	}
	return ks.db.deleteOldMks(sessionID, deleteUntilSeqKey)
}

func (ks keysStorageImpl) TruncateMks(sessionID []byte, maxKeys int) error {
	if !bytes.Equal(sessionID, ks.sessionID) {
		return fmt.Errorf("expected %x to equal %x", sessionID, ks.sessionID)
	}
	return ks.db.truncateMks(sessionID, maxKeys)
}

func (ks keysStorageImpl) Count(k doubleratchet.Key) (uint, error) {
	return ks.db.countKeys(k)
}

func (ks keysStorageImpl) All() (map[string]map[uint]doubleratchet.Key, error) {
	return nil, errors.New("not implemented")
}
