package slick

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/messaging"
	"github.com/meow-io/go-slick/transport/heya"
	"github.com/meow-io/go-slick/transport/local"
)

const (
	typeLocal = 0
	typeHeya  = 1
)

type heyaURL struct {
	Host        string   `bencode:"h"`
	Port        uint32   `bencode:"p"`
	PublicBytes [32]byte `bencode:"b"`
	SendToken   [32]byte `bencode:"s"`
}

type compactJpake1 struct {
	ID           [16]byte          `bencode:"i"`
	UserID       [16]byte          `bencode:"u"`
	PublicKey    [32]byte          `bencode:"k"`
	X1G          []byte            `bencode:"1"`
	X2G          []byte            `bencode:"2"`
	X1ZKP        messaging.ZKPBody `bencode:"x"`
	X2ZKP        messaging.ZKPBody `bencode:"y"`
	ReplyTo      [][]byte          `bencode:"r"`
	ReplyToTypes []uint8           `bencode:"t"`
	Priorities   []uint8           `bencode:"p"`
}

type compactDeviceGroupInvite struct {
	Jpake1   compactJpake1 `bencode:"j"`
	Password string        `bencode:"w"`
}

func SerializeInvite(i *messaging.Jpake1) (string, error) {
	cjp1, err := compactifyJpake1(i)
	if err != nil {
		return "", err
	}
	s, err := bencode.Serialize(cjp1)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(s), nil
}

func compactifyJpake1(i *messaging.Jpake1) (*compactJpake1, error) {
	replyTo := make([][]byte, 0, len(i.ReplyTo))
	replyToTypes := make([]uint8, 0, len(i.ReplyTo))
	priorities := make([]uint8, 0, len(i.ReplyTo))

	for u, p := range i.ReplyTo {
		if strings.HasPrefix(u, local.DigestScheme) {
			digest, err := local.ParseURL(u)
			if err != nil {
				return nil, err
			}
			replyTo = append(replyTo, digest[:])
			replyToTypes = append(replyToTypes, typeLocal)
		} else if strings.HasPrefix(u, heya.HeyaScheme) {
			parsed, err := heya.ParseURL(u)
			if err != nil {
				return nil, err
			}
			h := &heyaURL{
				Host:        parsed.Host,
				Port:        uint32(parsed.Port),
				PublicBytes: parsed.PublicBytes,
				SendToken:   parsed.SendToken,
			}

			hb, err := bencode.Serialize(h)
			if err != nil {
				return nil, err
			}
			replyTo = append(replyTo, hb)
			replyToTypes = append(replyToTypes, typeHeya)
		} else {
			return nil, errors.New("unknown url type")
		}
		priorities = append(priorities, p.Priority)
	}

	return &compactJpake1{
		ID:           i.ID,
		UserID:       i.UserID,
		PublicKey:    i.PublicKey,
		X1G:          i.X1G,
		X2G:          i.X2G,
		X1ZKP:        *i.X1ZKP,
		X2ZKP:        *i.X2ZKP,
		ReplyTo:      replyTo,
		ReplyToTypes: replyToTypes,
		Priorities:   priorities,
	}, nil
}

func DeserializeInvite(s string) (*messaging.Jpake1, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	cjp1 := &compactJpake1{}

	if err := bencode.Deserialize(b, cjp1); err != nil {
		return nil, err
	}

	return decompactifyJpake1(cjp1)
}

func decompactifyJpake1(cjp1 *compactJpake1) (*messaging.Jpake1, error) {
	endpoints := make(map[string]*messaging.EndpointInfo)
	for i := 0; i != len(cjp1.ReplyTo); i++ {
		switch cjp1.ReplyToTypes[i] {
		case typeHeya:
			hu := &heyaURL{}
			if err := bencode.Deserialize(cjp1.ReplyTo[i], hu); err != nil {
				return nil, err
			}

			u := heya.ParsedURL{
				Host:        hu.Host,
				Port:        int(hu.Port),
				PublicBytes: hu.PublicBytes,
				SendToken:   hu.SendToken,
			}
			endpoints[u.URL()] = &messaging.EndpointInfo{Priority: cjp1.Priorities[i]}
		case typeLocal:
			endpoints[local.NewURL([32]byte(cjp1.ReplyTo[i]))] = &messaging.EndpointInfo{Priority: cjp1.Priorities[i]}
		default:
			return nil, fmt.Errorf("unknown type %d", cjp1.ReplyToTypes[i])
		}
	}
	return &messaging.Jpake1{
		ID:        cjp1.ID,
		UserID:    cjp1.UserID,
		PublicKey: cjp1.PublicKey,
		X1G:       cjp1.X1G,
		X2G:       cjp1.X2G,
		X1ZKP:     &cjp1.X1ZKP,
		X2ZKP:     &cjp1.X2ZKP,
		ReplyTo:   endpoints,
	}, nil
}

func SerializeDeviceInvite(i *DeviceGroupInvite) (string, error) {
	cjp1, err := compactifyJpake1(i.Invite)
	if err != nil {
		return "", err
	}
	cdgi := &compactDeviceGroupInvite{
		Jpake1: *cjp1, Password: i.Password,
	}
	b, err := bencode.Serialize(cdgi)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func DeserializeDeviceInvite(s string) (*DeviceGroupInvite, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	cdgi := &compactDeviceGroupInvite{}
	if err := bencode.Deserialize(b, cdgi); err != nil {
		return nil, err
	}

	jp1, err := decompactifyJpake1(&cdgi.Jpake1)
	if err != nil {
		return nil, err
	}
	return &DeviceGroupInvite{
		Invite:   jp1,
		Password: cdgi.Password,
	}, nil
}
