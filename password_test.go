package slick

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	// "log"
)

func TestMakePassword(t *testing.T) {
	require := require.New(t)
	tmp := os.TempDir()
	key1, err := newKey("some password", tmp, "salt")
	require.Nil(err)
	key2, err := newKey("some password", tmp, "salt")
	require.Nil(err)
	require.Equal(key1, key2)
	require.Equal(32, len(key1))
}

func TestMakePasswordDifferentSalt(t *testing.T) {
	require := require.New(t)
	tmp := os.TempDir()
	key1, err := newKey("some password", tmp, "salt1")
	require.Nil(err)
	key2, err := newKey("some password", tmp, "salt2")
	require.Nil(err)
	require.NotEqual(key1, key2)
}
