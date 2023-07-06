package test

import (
	crypto_rand "crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	db "github.com/meow-io/go-slick/internal/db"
)

type ID [8]byte

func newID() ID {
	var id [8]byte
	_, err := io.ReadFull(crypto_rand.Reader, id[:])
	if err != nil {
		panic("short read from random source")
	}
	return id
}

func DeleteAll(glob string) {
	files, err := filepath.Glob(glob)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		fileInfo, err := os.Stat(f)
		if err != nil {
			panic(err)
		}

		if fileInfo.IsDir() {
			DeleteAll(path.Join(f, "*"))
		} else {
			if err := os.Remove(f); err != nil {
				panic(err)
			}
		}
	}
}

func DBCleanup(run func() int) int {
	c := run()
	testCleanup()
	return c
}

func testCleanup() {
	DeleteAll("*-journal")
	DeleteAll("test-*")
}

func NewTestDatabase(c *config.Config) *db.Database {
	id := newID()
	path := fmt.Sprintf("test-%x", id[:])
	db, err := db.NewDatabase(c, clock.NewSystemClock(), path)
	if err != nil {
		panic(err)
	}
	key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
	if err := db.Initialize(key); err != nil {
		panic(err)
	}
	if err := db.Open(key); err != nil {
		panic(err)
	}
	return db
}
