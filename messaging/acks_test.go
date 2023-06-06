package messaging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcksUpdateAcksSimple(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{3})    // 7,8
	newA := newAcksFromBitmap(6, []byte{4}) // 10
	newIDs := a.update(newA)
	require.Equal([]uint64{6, 10}, newIDs)
	require.Equal(uint64(8), a.seq)
	require.Equal(map[uint64]bool{10: true}, a.sparse)
	require.Equal([]byte{0x1}, a.sparseBitmap())
}

func TestAcksUpdateAcksSimple2(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(4, []byte{6})    // 7,8
	newA := newAcksFromBitmap(2, []byte{6}) // 4,5
	newIDs := a.update(newA)
	require.Equal([]uint64{5, 6}, newIDs)
	require.Equal(map[uint64]bool{}, a.sparse)
	require.Equal(uint64(8), a.seq)
}

func TestAcksUpdateAcksOnlySeq(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{3}) // 7,8
	newA := newAcksFromBitmap(10, []byte{})
	newIDs := a.update(newA)
	require.Equal([]uint64{6, 9, 10}, newIDs)
	require.Equal(uint64(10), a.seq)
	require.Equal(0, len(a.sparse))
}

func TestAcksUpdateAcksSeqAndSparse(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{3}) // 7,8
	newA := newAcksFromBitmap(10, []byte{3})
	newIDs := a.update(newA)
	require.Equal([]uint64{6, 9, 10, 12, 13}, newIDs)
	require.Equal(uint64(10), a.seq)
	require.Equal(map[uint64]bool{12: true, 13: true}, a.sparse)
	require.Equal([]byte{0x3}, a.sparseBitmap())
}

func TestAcksUpdateAcksOnlySparse(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{3})     // 7,8
	newA := newAcksFromBitmap(5, []byte{20}) // 9,11
	newIDs := a.update(newA)
	require.Equal([]uint64{9, 11}, newIDs)
	require.Equal(uint64(5), a.seq)
	require.Equal([]byte{23}, a.sparseBitmap())
}

func TestAcksNoopAdd(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{0x3}) // 7,8
	require.False(a.add(5))

	require.Equal(uint64(5), a.seq)
	require.Equal([]byte{3}, a.sparseBitmap())
}

func TestAcksAddInSparse(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{3}) // 7,8
	require.True(a.add(9))

	require.Equal(uint64(5), a.seq)
	require.Equal([]byte{7}, a.sparseBitmap())
}

func TestAcksAddWithCompaction(t *testing.T) {
	require := require.New(t)

	a := newAcksFromBitmap(5, []byte{0xb}) // 7 (1) ,8 (2) ,11 (8)
	require.True(a.add(6))

	require.Equal(uint64(8), a.seq)
	require.Equal([]byte{1}, a.sparseBitmap())
}

func TestBitmapSet(t *testing.T) {
	require := require.New(t)

	a := bitmap([]byte{0x00, 0x00, 0x00})
	require.Equal(false, a.Set(0, false))
	require.Equal(true, a.Set(10, true))
	require.Equal(true, a.Set(1, true))
	require.Equal(true, a.Set(26, true))
	require.Equal(false, a.Set(26, true))

	require.Equal([]byte{0x2, 0x4, 0x0, 0x4}, []byte(a))
}

func TestBitmapGet(t *testing.T) {
	require := require.New(t)

	a := bitmap([]byte{0x3, 0xff})
	require.Equal(true, a.Get(0))
	require.Equal(true, a.Get(1))
	require.Equal(false, a.Get(2))
	require.Equal(true, a.Get(8))
	require.Equal(false, a.Get(20))
}
