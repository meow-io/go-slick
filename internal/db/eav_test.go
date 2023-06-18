package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testClock struct {
	currentMicro uint64
}

func (tc *testClock) CurrentTimeMicro() uint64 {
	return tc.currentMicro
}

func (tc *testClock) CurrentTimeMs() uint64 {
	return tc.CurrentTimeMicro() / 1000
}

func (tc *testClock) CurrentTimeSec() uint64 {
	return tc.CurrentTimeMs() / 1000
}

func (tc *testClock) Now() time.Time {
	return time.Unix(int64(tc.currentMicro)/1000000, int64((tc.currentMicro%1000000)*1000))
}

func (tc *testClock) AdvanceMicros(a uint64) {
	tc.currentMicro += a
}

func TestSimpleAdd(t *testing.T) {
	require := require.New(t)
	e := eav{&testClock{0}}
	empty := e.MakeEmptyRecord()
	e.cl.(*testClock).AdvanceMicros(65535)
	newBytes, err := e.eavSet(empty, EAVPack(0, 0, false, []byte("hello")))
	require.Nil(err)
	require.Equal([]byte{
		0x0,      // version
		0x0, 0x1, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
	}, newBytes)
}

func TestSimpleNewValue(t *testing.T) {
	require := require.New(t)
	e := eav{&testClock{0}}
	empty := e.MakeEmptyRecord()
	e.cl.(*testClock).AdvanceMicros(65535)
	newBytes, err := e.eavSet(empty, EAVPack(0, 0, false, []byte("hello")))
	require.Nil(err)
	newBytes, err = e.eavSet(newBytes, EAVPack(0, 1, false, []byte("there!")))
	require.Nil(err)
	require.Equal([]byte{
		0x0,      // version
		0x0, 0x1, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x6, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // time
		't', 'h', 'e', 'r', 'e', '!',
	}, newBytes)
}

func TestSimpleOldValue(t *testing.T) {
	require := require.New(t)
	e := eav{&testClock{0}}
	empty := e.MakeEmptyRecord()
	newBytes, err := e.eavSet(empty, EAVPack(0, 1, false, []byte("hello")))
	require.Nil(err)
	newBytes, err = e.eavSet(newBytes, EAVPack(0, 0, false, []byte("there!")))
	e.cl.(*testClock).AdvanceMicros(65535)
	require.Nil(err)
	require.Equal([]byte{
		0x0,      // version
		0x0, 0x1, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // time
		'h', 'e', 'l', 'l', 'o',
	}, newBytes)
}

func TestDoubleValue(t *testing.T) {
	require := require.New(t)
	e := eav{&testClock{}}
	newBytes, err := e.eavSet([]byte{
		0x0,      // version
		0x0, 0x1, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
	}, EAVPack(1, 1, false, []byte("there!")))
	require.Nil(err)
	require.Equal([]byte{
		0x0,      // version
		0x0, 0x2, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x1, // name index
		0x0, 0x0, 0x0, 0x11, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
		0x0, 0x0, 0x0, 0x6, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // time
		't', 'h', 'e', 'r', 'e', '!',
	}, newBytes)
}

func TestGetValue(t *testing.T) {
	require := require.New(t)
	rec := []byte{
		0x0,      // version
		0x0, 0x2, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x1, // name index
		0x0, 0x0, 0x0, 0x11, // pos
		0x0,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
		0x0, 0x0, 0x0, 0x6, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // time
		't', 'h', 'e', 'r', 'e', '!',
	}
	e := eav{&testClock{}}
	b1, err := e.eavGet(rec, 0)
	require.Nil(err)
	b2, err := e.eavGet(rec, 1)
	require.Nil(err)
	require.Equal([]byte("hello"), b1)
	require.Equal([]byte("there!"), b2)
}

func TestGetNullValue(t *testing.T) {
	require := require.New(t)
	e := eav{&testClock{}}
	b, err := e.eavGet([]byte{
		0x0,      // version
		0x0, 0x1, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x1,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
	}, 0)
	require.Nil(err)
	require.Equal(nil, b)
}

func TestHasValue(t *testing.T) {
	in := []byte{
		0x0,      // version
		0x0, 0x2, // count
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // mtime
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // wtime
		0x0, 0x0, 0x0, 0x0, // name index
		0x0, 0x0, 0x0, 0x0, // pos
		0x1,                // flag
		0x0, 0x0, 0x0, 0x1, // name index
		0x0, 0x0, 0x0, 0x11, // pos
		0x1,                // flag
		0x0, 0x0, 0x0, 0x5, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // time
		'h', 'e', 'l', 'l', 'o',
		0x0, 0x0, 0x0, 0x6, // len
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // time
		't', 'h', 'e', 'r', 'e', '!',
	}
	e := eav{&testClock{}}
	c, err := e.eavHas(in, 0, 1)
	if err != nil {
		t.Fatalf("error %#v", err)
	}
	if c != 1 {
		t.Fatalf("expected a match")
	}

	c, err = e.eavHas(in, 0, 1, 2)
	if err != nil {
		t.Fatalf("error %#v", err)
	}
	if c != 0 {
		t.Fatalf("expected not a match")
	}
}