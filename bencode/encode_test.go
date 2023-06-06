package bencode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleEncode(t *testing.T) {
	require := require.New(t)

	obj := struct {
		Mary   []byte `bencode:"m"`
		Joseph []byte `bencode:"j"`
		Peter  int64  `bencode:"p"`
		Paul   string `bencode:"pp"`
	}{
		Peter:  1234,
		Paul:   "abcdefghij",
		Joseph: []byte("0123456789"),
		Mary:   []byte("0123"),
	}
	buf, err := Serialize(&obj)
	require.Nil(err)
	require.Equal([]byte("d1:j10:01234567891:m4:01231:pi1234e2:pp10:abcdefghije"), buf)
}

func TestEncodeStructField(t *testing.T) {
	require := require.New(t)

	type inner struct {
		One string `bencode:"a"`
		Two string `bencode:"b"`
	}

	obj := struct {
		Three inner `bencode:"t"`
	}{
		Three: inner{One: "abcde", Two: "abcabc"},
	}
	buf, err := Serialize(&obj)
	require.Nil(err)
	require.Equal([]byte("d1:td1:a5:abcde1:b6:abcabcee"), buf)
}

func TestEncodeMapOfStruct(t *testing.T) {
	require := require.New(t)
	type inner struct {
		One string `bencode:"a"`
		Two string `bencode:"b"`
	}

	obj := struct {
		Mary map[[8]byte]inner `bencode:"m"`
	}{
		Mary: map[[8]byte]inner{
			{0xad, 0x62, 0x63, 0x63, 0x65, 0x66, 0x67, 0x68}: {
				One: "efghi",
				Two: "cbacba",
			},
			{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}: {
				One: "abcde",
				Two: "abcabc",
			},
			{0x31, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68}: {
				One: "efghi",
				Two: "cbacba",
			},
		},
	}
	buf, err := Serialize(&obj)
	require.Nil(err)
	require.Equal([]byte{
		0x64, 0x31, 0x3a, 0x6d, 0x64, 0x38, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x64,
		0x31, 0x3a, 0x61, 0x35, 0x3a, 0x61, 0x62, 0x63, 0x64, 0x65, 0x31, 0x3a, 0x62, 0x36, 0x3a, 0x61,
		0x62, 0x63, 0x61, 0x62, 0x63, 0x65, 0x38, 0x3a, 0x31, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
		0x64, 0x31, 0x3a, 0x61, 0x35, 0x3a, 0x65, 0x66, 0x67, 0x68, 0x69, 0x31, 0x3a, 0x62, 0x36, 0x3a,
		0x63, 0x62, 0x61, 0x63, 0x62, 0x61, 0x65, 0x38, 0x3a, 0xad, 0x62, 0x63, 0x63, 0x65, 0x66, 0x67,
		0x68, 0x64, 0x31, 0x3a, 0x61, 0x35, 0x3a, 0x65, 0x66, 0x67, 0x68, 0x69, 0x31, 0x3a, 0x62, 0x36,
		0x3a, 0x63, 0x62, 0x61, 0x63, 0x62, 0x61, 0x65, 0x65, 0x65,
	}, buf)
}

func TestEncodeArrayOfStruct(t *testing.T) {
	require := require.New(t)
	type inner struct {
		One string `bencode:"a"`
		Two string `bencode:"b"`
	}
	obj := struct {
		Mary []inner `bencode:"m"`
	}{
		Mary: []inner{
			{
				One: "abcde",
				Two: "abcabc",
			},
			{
				One: "efghi",
				Two: "cbacba",
			},
		},
	}
	buf, err := Serialize(&obj)
	require.Nil(err)
	require.Equal([]byte("d1:mld1:a5:abcde1:b6:abcabced1:a5:efghi1:b6:cbacbaeee"), buf)
}
