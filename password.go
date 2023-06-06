package slick

import (
	crypto_rand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/crypto/argon2"
)

func newKey(password, root, saltName string) ([]byte, error) {
	var salt [16]byte
	saltPath := filepath.Join(root, saltName)
	if _, err := os.Stat(saltPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			f, err := os.OpenFile(saltPath, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0o400) // #nosec G304
			if err != nil {
				return nil, err
			}
			if _, err := crypto_rand.Read(salt[:]); err != nil {
				if err := f.Close(); err != nil {
					fmt.Printf("error while closing %#v", err)
				}
				return nil, err
			}
			n, err := f.Write(salt[:])
			if err != nil {
				if err := f.Close(); err != nil {
					fmt.Printf("error while closing %#v", err)
				}
				return nil, err
			}
			if n != 16 {
				if err := f.Close(); err != nil {
					fmt.Printf("error while closing %#v", err)
				}
				return nil, fmt.Errorf("expected 16 bytes, got %d", n)
			}
			if err := f.Close(); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		f, err := os.OpenFile(saltPath, os.O_RDONLY, 0o400) // #nosec G304
		if err != nil {
			return nil, err
		}
		n, err := io.ReadFull(f, salt[:])
		if err != nil {
			if err := f.Close(); err != nil {
				fmt.Printf("error while closing %#v", err)
			}
			return nil, err
		}
		if n != 16 {
			if err := f.Close(); err != nil {
				fmt.Printf("error while closing %#v", err)
			}
			return nil, fmt.Errorf("expected 16 bytes, got %d", n)
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return argon2.IDKey([]byte(password), salt[:], 1, 64*1024, 4, 32), nil
}
