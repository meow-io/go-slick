package messaging

import (
	"sort"
)

// slightly copied from https://github.com/boljen/go-bitmap/blob/master/bitmap.go
var (
	tA = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	tB = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)

type bitmap []byte

// Get returns the value of bit i from map m.
// It checks the bounds of the slice.
func (b *bitmap) Get(i int) bool {
	si := i / 8
	if len(*b) <= si {
		return false
	}

	return (*b)[si]&tA[i%8] != 0
}

// Set sets bit i of map m to value v.
// It zero fills the slice if i is out of bounds.
func (b *bitmap) Set(i int, v bool) bool {
	si := i / 8

	if len(*b) <= si {
		newB := append(*b, make([]byte, si-len(*b)+1)...)
		*b = newB
	}
	bit := i % 8
	prev := (*b)[si]
	if v {
		(*b)[si] = (*b)[si] | tA[bit]
	} else {
		(*b)[si] = (*b)[si] & tB[bit]
	}
	return prev != (*b)[si]
}

type acks struct {
	seq     uint64
	sparse  map[uint64]bool
	changed bool
}

func newAcksFromBitmap(seq uint64, bm []byte) *acks {
	b := bitmap(bm)
	sparse := make(map[uint64]bool)
	for i := 0; i != len(b)*8; i++ {
		if b.Get(i) {
			sparse[seq+uint64(i)+2] = true
		}
	}

	return &acks{seq, sparse, false}
}

func (a *acks) sparseBitmap() []byte {
	bm := bitmap(nil)
	for i := range a.sparse {
		bm.Set(int(i-a.seq)-2, true)
	}
	return bm
}

func (a *acks) update(n *acks) []uint64 {
	var newIDs []uint64
	if n.seq > a.seq {
		for i := a.seq + 1; i <= n.seq; i++ {
			if !a.sparse[i] {
				newIDs = append(newIDs, i)
			}
		}
		a.seq = n.seq
	}

	for i := range a.sparse {
		if i <= a.seq {
			delete(a.sparse, i)
		}
	}

	for i := range n.sparse {
		if i > a.seq && !a.sparse[i] {
			newIDs = append(newIDs, i)
			a.sparse[i] = true
		}
	}
	a.compact()
	if len(newIDs) != 0 {
		a.changed = true
	}
	sort.Slice(newIDs, func(i, j int) bool { return newIDs[i] < newIDs[j] })

	return newIDs
}

func (a *acks) add(n uint64) bool {
	if n <= a.seq {
		return false
	} else if !a.sparse[n] {
		a.sparse[n] = true
		a.changed = true
		a.compact()
		return true
	} else {
		return false
	}
}

func (a *acks) compact() {
	for a.sparse[a.seq+1] {
		a.seq++
		delete(a.sparse, a.seq)
	}
}
