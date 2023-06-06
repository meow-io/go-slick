// This package defines a common id type which is used through out slick. It is based on random 16 byte values.
package ids

import (
	"bytes"
	crypto_rand "crypto/rand"
	"io"
)

type ID [16]byte

func IDFromBytes(b []byte) ID {
	return [16]byte(b)
}

func NewID() ID {
	var id [16]byte
	_, err := io.ReadFull(crypto_rand.Reader, id[:])
	if err != nil {
		panic("short read from random source")
	}
	return id
}

func Compare(a, b ID) int {
	return bytes.Compare(a[:], b[:])
}

type ByLexicographical []ID

func (s ByLexicographical) Len() int           { return len(s) }
func (s ByLexicographical) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLexicographical) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) == -1 }
