package crypto

import (
	"github.com/kevinburke/nacl"
	"github.com/kevinburke/nacl/box"
	"golang.org/x/crypto/chacha20poly1305"
)

var zeroNonce12 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func SliceToKey(b []byte) nacl.Key {
	return nacl.Key(b)
}

func EncryptWithDH(pub, priv, msg, ad []byte) ([]byte, error) {
	key := box.Precompute(SliceToKey(pub), SliceToKey(priv))
	return EncryptWithKey(key[:], msg, ad)
}

func DecryptWithDH(pub, priv, enc, ad []byte) ([]byte, error) {
	key := box.Precompute(SliceToKey(pub), SliceToKey(priv))
	return DecryptWithKey(key[:], enc, ad)
}

func EncryptWithKey(key, msg, ad []byte) ([]byte, error) {
	if len(key) != 32 {
		panic("key is wrong length")
	}
	cipher, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.Seal(nil, zeroNonce12, msg, ad), nil
}

func DecryptWithKey(key, enc, ad []byte) ([]byte, error) {
	if len(key) != 32 {
		panic("key is wrong length")
	}
	cipher, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.Open(nil, zeroNonce12, enc, ad)
}
