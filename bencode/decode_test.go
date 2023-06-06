package bencode

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStruct(t *testing.T) {
	require := require.New(t)

	obj := struct {
		Mary   []byte `bencode:"m"`
		Joseph []byte `bencode:"j"`
		Peter  int64  `bencode:"p"`
		Paul   string `bencode:"pp"`
	}{}
	buf := []byte("d1:j10:01234567891:m4:01231:pi1234e2:pp10:abcdefghije")
	err := Deserialize(buf, &obj)
	require.Nil(err)
	require.Equal(obj.Peter, int64(1234))
	require.Equal(obj.Joseph, []byte("0123456789"))
	require.Equal(obj.Mary, []byte("0123"))
	require.Equal(obj.Paul, "abcdefghij")
}

func TestDecodeMap(t *testing.T) {
	require := require.New(t)

	obj := make(map[string]string)
	buf := []byte("d10:abcdefghij10:abcdefghije")
	err := Deserialize(buf, &obj)
	require.Nil(err)
	require.Equal(obj["abcdefghij"], "abcdefghij")
}

func TestOutOfOrderDictionary(t *testing.T) {
	require := require.New(t)

	obj := struct {
		Mary   []byte `bencode:"m"`
		Joseph []byte `bencode:"j"`
		Peter  string `bencode:"p"`
		Paul   string `bencode:"pp"`
	}{}
	buf := []byte("d1:m4:01231:j10:01234567891:p4:12342:pp10:abcdefghije")
	err := Deserialize(buf, &obj)
	require.NotNil(err)
}

func TestMissingKey(t *testing.T) {
	require := require.New(t)

	obj := struct {
		Mary   []byte `bencode:"m"`
		Joseph []byte `bencode:"j"`
		Peter  string `bencode:"p"`
		Paul   string `bencode:"pp"`
	}{}
	buf := []byte("d1:j10:01234567891:p4:12342:pp10:abcdefghije")
	err := Deserialize(buf, &obj)
	require.NotNil(err)
}

func TestDecodeMapOfStruct(t *testing.T) {
	require := require.New(t)
	type inner struct {
		One string `bencode:"a"`
		Two string `bencode:"b"`
	}

	obj := struct {
		Mary map[[8]byte]inner `bencode:"m"`
	}{}
	buf := []byte(strings.Replace("d 1:m d 8:12345678 d 1:a 5:abcde 1:b 6:abcabc e 8:abcdefgh d 1:a 5:efghi 1:b 6:cbacba e e e", " ", "", -1))
	err := Deserialize(buf, &obj)
	require.Nil(err)
	k := [8]byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}
	require.Equal("abcde", obj.Mary[k].One)
}

func TestArrayOfStruct(t *testing.T) {
	require := require.New(t)
	type inner struct {
		One string `bencode:"a"`
		Two string `bencode:"b"`
	}
	obj := struct {
		Mary []inner `bencode:"m"`
	}{}
	buf := []byte(strings.Replace("d 1:m l d 1:a 5:abcde 1:b 6:abcabc e d 1:a 5:efghi 1:b 6:cbacba e e e", " ", "", -1))
	err := Deserialize(buf, &obj)
	require.Nil(err)
	require.Equal("abcde", obj.Mary[0].One)
}

func TestNumberOverflow(t *testing.T) {
	require := require.New(t)
	obj := struct {
		Mary int64 `bencode:"m"`
	}{}
	buf := []byte("d1:mi9223372036854775808ee")
	err := Deserialize(buf, &obj)
	require.NotNil(err)
}
