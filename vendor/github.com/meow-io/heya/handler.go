package heya

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type clientHandler struct {
	server      *Server
	conn        net.Conn
	certDigest  [32]byte
	nextSeqs    map[[32]byte]uint64
	log         *zap.SugaredLogger
	subscriber  *redis.Client
	sub         *redis.PubSub
	subCommands chan subCommand
}

func newClientHandler(s *Server, c net.Conn, redisOptions *redis.Options, certDigest [32]byte, log *zap.SugaredLogger) (*clientHandler, error) {
	subscriber := redis.NewClient(redisOptions)
	ch := &clientHandler{
		server:      s,
		conn:        c,
		certDigest:  certDigest,
		nextSeqs:    make(map[[32]byte]uint64),
		log:         log,
		subscriber:  subscriber,
		sub:         nil,
		subCommands: make(chan subCommand, 100),
	}
	return ch, nil
}

type sendCommand struct {
	ch     *clientHandler
	token  []byte
	digest []byte
	body   []byte
}

type trimCommand struct {
	ch    *clientHandler
	token []byte
	seq   uint64
}

type extendCommand struct {
	ch             *clientHandler
	token          []byte
	additionalTime uint64
}

type deauthAllCommand struct {
	ch *clientHandler
}

type pingCommand struct {
	ch *clientHandler
}

type listCommand struct {
	ch *clientHandler
}

type incomingCommand struct {
	ch    *clientHandler
	token []byte
}

type authCommand struct {
	ch        *clientHandler
	startTime uint64
	endTime   uint64
}

type deauthCommand struct {
	ch    *clientHandler
	token []byte
}

type wantCommand struct {
	ch    *clientHandler
	token []byte
	seq   uint64
}

type haveCommand struct {
	ch    *clientHandler
	token []byte
	seq   uint64
}

type iosAddCommand struct {
	ch    *clientHandler
	token string
}

type iosRemoveCommand struct {
	ch    *clientHandler
	token string
}

type iosListCommand struct {
	ch *clientHandler
}

func (ch *clientHandler) handle() error {
	ch.log.Debugf("%x: handling a connection", ch.certDigest[:])

	if err := ch.writef("HEYA 0\n"); err != nil {
		return err
	}

	mailbox, err := ch.server.mailbox(ch.certDigest[:])
	if err != nil {
		return err
	}
	if mailbox != nil {
		ch.log.Debugf("%x: connection has an mailbox", ch.certDigest[:])
		tokens, err := ch.server.sendTokens(mailbox.Digest)
		if err != nil {
			return err
		}
		for _, token := range tokens {
			ch.log.Debugf("%x: sending LAST value for this token %x %d", ch.certDigest, token.Token, token.NextSeq)
			if err := ch.writef("LAST %x %d\n", token.Token, token.NextSeq); err != nil {
				return err
			}
		}
	}
	if err := ch.writef("DONE\n"); err != nil {
		return err
	}
	ch.startSubscriber()

	for {
		// wait for a command
		if err := ch.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}

		cmd := make([]byte, 5)
		if _, err := io.ReadFull(ch.conn, cmd); err != nil {
			if opError, ok := err.(*net.OpError); ok {
				if opError.Op == "read" {
					return nil
				}
			}
			return err
		}

		// set a deadline
		if err := ch.conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			return err
		}

		// log.Printf("log command %s", string(cmd[0:4]))

		switch string(cmd) {
		case "SEND ":
			if err := ch.readSendCommand(); err != nil {
				return err
			}
		case "INCO ":
			if err := ch.readIncomingCommand(); err != nil {
				return err
			}
		case "DALL\n":
			if err := ch.readDeauthAllCommand(); err != nil {
				return err
			}
		case "AUTH ":
			if err := ch.readAuthCommand(); err != nil {
				return err
			}
		case "DEAU ":
			if err := ch.readDeauthCommand(); err != nil {
				return err
			}
		case "WANT ":
			if err := ch.readWantCommand(); err != nil {
				return err
			}
		case "PING\n":
			if err := ch.readPingCommand(); err != nil {
				return err
			}
		case "LIST\n":
			if err := ch.readListCommand(); err != nil {
				return err
			}
		case "IOSA ":
			if err := ch.readIOSAddCommand(); err != nil {
				return err
			}
		case "IOSD ":
			if err := ch.readIOSRemoveCommand(); err != nil {
				return err
			}
		case "IOSL\n":
			if err := ch.readIOSListCommand(); err != nil {
				return err
			}
		case "TRIM ":
			if err := ch.readTrimCommand(); err != nil {
				return err
			}
		case "EXTD ":
			if err := ch.readExtendCommand(); err != nil {
				return err
			}
		case "QUIT\n":
			ch.close()
			return nil
		default:
			ch.close()
			return fmt.Errorf("unexpected command %x %s", cmd, cmd)
		}
	}
}

func (ch *clientHandler) readUint(maxlen int, terminator byte) (uint64, error) {
	intBytes := make([]byte, maxlen+1)
	i := 0
	terminatorRead := false
	for i = 0; i != maxlen+1; i++ {
		if _, err := io.ReadFull(ch.conn, intBytes[i:i+1]); err != nil {
			return 0, err
		}

		if intBytes[i] == terminator {
			terminatorRead = true
			break
		}
		if intBytes[i] < 0x30 || intBytes[i] > 0x39 {
			return 0, fmt.Errorf("got non-number 0x%x", intBytes[i])
		}
	}

	if !terminatorRead {
		return 0, fmt.Errorf("expected to read 0x%x", terminator)
	}

	readInt, err := strconv.ParseUint(string(intBytes[0:i]), 10, 64)
	if err != nil {
		return 0, err
	}
	return readInt, nil
}

func (ch *clientHandler) readHex(len int) ([]byte, error) {
	hexBytes := make([]byte, len)
	if _, err := io.ReadFull(ch.conn, hexBytes); err != nil {
		return nil, err
	}

	return hex.DecodeString(string(hexBytes))
}

func (ch *clientHandler) ensureByte(expectedByte byte) error {
	b, err := ch.readByte()
	if err != nil {
		return err
	}
	if b != expectedByte {
		return fmt.Errorf("expected byte 0x%x, got 0x%x", expectedByte, b)
	}
	return nil
}

func (ch *clientHandler) readByte() (byte, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(ch.conn, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

// SEND [token] [digest-hex] [length]\n[len bytes]
//
//	RECV [digest-hex]
func (ch *clientHandler) readSendCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return fmt.Errorf("SEND: %w", err)
	}
	if err := ch.ensureByte(0x20); err != nil {
		return fmt.Errorf("SEND: %w", err)
	}
	digest, err := ch.readHex(64)
	if err != nil {
		return fmt.Errorf("SEND: %w", err)
	}
	if err := ch.ensureByte(0x20); err != nil {
		return fmt.Errorf("SEND: %w", err)
	}
	lenInt, err := ch.readUint(10, 0xa)
	if err != nil {
		return fmt.Errorf("SEND2: %w", err)
	}
	body := make([]byte, 0, lenInt)
	tmp := make([]byte, 1024)
	hash := sha256.New()
	for lenInt > 0 {
		var l int
		var err error
		if lenInt >= 1024 {
			l, err = ch.conn.Read(tmp)
		} else {
			l, err = ch.conn.Read(tmp[0:lenInt])
		}
		if err != nil {
			return err
		}
		lenInt -= uint64(l)
		body = append(body, tmp[0:l]...)
		if _, err := hash.Write(tmp[0:l]); err != nil {
			return err
		}
	}
	actualDigest := hash.Sum(nil)
	if !bytes.Equal(actualDigest[:], digest) {
		return fmt.Errorf("SEND: digest mismatch, got %x expected %s", actualDigest, digest)
	}
	if err := ch.ensureByte(0xa); err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}

	ch.server.commands <- &sendCommand{ch, token, digest, body}
	return nil
}

// INCO [access token]
//
//	HELD [access token]
//	NOPE [access token]
func (ch *clientHandler) readIncomingCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return err
	}
	if err := ch.ensureByte(0xa); err != nil {
		return err
	}

	ch.server.commands <- &incomingCommand{ch, token}
	return nil
}

// DALL
//
//	DALL
func (ch *clientHandler) readDeauthAllCommand() error {
	ch.server.commands <- &deauthAllCommand{ch}
	return nil
}

// WANT [token] [seq]
//
//	GIVE [token] [seq] [length]
//	GONE [token] [seq]
func (ch *clientHandler) readWantCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return err
	}
	if err := ch.ensureByte(0x20); err != nil {
		return err
	}
	// {seq-ascii:1-10}\n
	seqInt, err := ch.readUint(10, 0xa)
	if err != nil {
		return fmt.Errorf("WANT: %w", err)
	}

	ch.server.commands <- &wantCommand{ch, token, seqInt}
	return nil
}

// AUTH [incoming token] [start-time] [end-time]
//
//	AUTH [incoming token]
func (ch *clientHandler) readAuthCommand() error {
	startTime, err := ch.readUint(10, 0x20)
	if err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}
	endTime, err := ch.readUint(10, 0xa)
	if err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}

	ch.server.commands <- &authCommand{ch, startTime, endTime}
	return nil
}

// DEAU [token]
//
//	DEAU [incoming token]
func (ch *clientHandler) readDeauthCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return fmt.Errorf("DEAU: %w", err)
	}
	if err := ch.ensureByte(0xa); err != nil {
		return err
	}
	ch.server.commands <- &deauthCommand{ch, token}
	return nil

}

// PING
//
//	PONG
func (ch *clientHandler) readPingCommand() error {
	ch.server.commands <- &pingCommand{ch}
	return nil
}

// LIST
//
//	LIST [num]\n...
func (ch *clientHandler) readListCommand() error {
	ch.server.commands <- &listCommand{ch}
	return nil
}

// TRIM [token] [seq]
//
//	TRIM [token] [seq] [number of messages]
func (ch *clientHandler) readTrimCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return err
	}
	if err := ch.ensureByte(0x20); err != nil {
		return err
	}
	// {seq-ascii:1-10}\n
	seqInt, err := ch.readUint(10, 0xa)
	if err != nil {
		return fmt.Errorf("TRIM: %w", err)
	}
	ch.server.commands <- &trimCommand{ch, token, seqInt}
	return nil
}

// client: EXTD [token-hex:64] [seconds:ascii-int] --->
// server: <--- EXTD [token-key] [new-end-time:ascii-int]
func (ch *clientHandler) readExtendCommand() error {
	token, err := ch.readHex(64)
	if err != nil {
		return fmt.Errorf("DEAU: %w", err)
	}
	if err := ch.ensureByte(0x20); err != nil {
		return err
	}
	additionalTime, err := ch.readUint(10, 0xa)
	if err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}
	ch.server.commands <- &extendCommand{ch, token, additionalTime}
	return nil
}

// IOSA [token]
//
//	IOSA [token]
func (ch *clientHandler) readIOSAddCommand() error {
	token := make([]byte, 64)
	l, err := ch.conn.Read(token)
	if l != 64 {
		return fmt.Errorf("IOSA: expected 64 bytes, got %d", l)
	}
	if err != nil {
		return fmt.Errorf("IOSA: %w", err)
	}
	b := make([]byte, 1)
	if _, err := ch.conn.Read(b); err != nil {
		return err
	}
	if b[0] != 0xa {
		return fmt.Errorf("IOSA: expected 0xa got %x", b[0])
	}
	ch.server.commands <- &iosAddCommand{ch, string(token)}
	return nil
}

// IOSD [token]
//
//	IOSD [token]
func (ch *clientHandler) readIOSRemoveCommand() error {
	token := make([]byte, 64)
	l, err := ch.conn.Read(token)
	if l != 64 {
		return fmt.Errorf("IOSD: expected 64 bytes, got %d", l)
	}
	if err != nil {
		return fmt.Errorf("IOSD: %w", err)
	}
	b := make([]byte, 1)
	if _, err := ch.conn.Read(b); err != nil {
		return err
	}
	if b[0] != 0xa {
		return fmt.Errorf("IOSD: expected 0xa got %x", b[0])
	}
	ch.server.commands <- &iosRemoveCommand{ch, string(token)}
	return nil
}

// IOSL
//
//	IOSL [num]\n...
func (ch *clientHandler) readIOSListCommand() error {
	ch.server.commands <- &iosListCommand{ch}
	return nil
}

func (ch *clientHandler) have(token []byte, seq uint64) error {
	token32 := ([32]byte)(token)
	if i, ok := ch.nextSeqs[token32]; ok {
		if i == seq {
			return nil
		}
	}
	ch.nextSeqs[token32] = seq
	return ch.writef("HAVE %x %d\n", token, seq)
}

func (ch *clientHandler) writef(f string, args ...interface{}) error {
	m := fmt.Sprintf(f, args...)
	ch.server.log.Debugf("writing %s", m)
	if _, err := ch.conn.Write([]byte(m)); err != nil {
		return err
	}
	return nil
}

func (ch *clientHandler) startSubscriber() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sub := ch.subscriber.Subscribe(ctx)
		ch.sub = sub
		msgs := sub.Channel()
		for {
			select {
			case cmd := <-ch.subCommands:
				ch.log.Debugf("subcommand recv %#v", cmd)
				if cmd.Add {
					if err := sub.Subscribe(ctx, cmd.Channel); err != nil {
						ch.log.Errorf("subcommand recv %#v", err)
						ch.close()
					}
				} else {
					if err := sub.Unsubscribe(ctx, cmd.Channel); err != nil {
						ch.log.Errorf("subcommand recv %#v", err)
						ch.close()
					}
				}
			case m := <-msgs:
				if m == nil {
					return
				}
				t, err := hex.DecodeString(m.Channel)
				if err != nil {
					panic(err)
				}
				s, err := strconv.ParseUint(m.Payload, 10, 64)
				if err != nil {
					panic(err)
				}
				if err := ch.have(t, s); err != nil {
					panic(err)
				}
			}
		}
	}()
}

func (ch *clientHandler) subscribe(token []byte) {
	ch.subCommands <- subCommand{
		Add:     true,
		Channel: fmt.Sprintf("%x", token),
	}
}

func (ch *clientHandler) unsubscribe(token []byte) {
	ch.subCommands <- subCommand{
		Add:     false,
		Channel: fmt.Sprintf("%x", token),
	}
}

func (ch *clientHandler) stop() {
	if ch.sub != nil {
		if err := ch.sub.Close(); err != nil {
			ch.log.Warnf("error while closing sub %#v", err)
		}
	}
	if err := ch.subscriber.Close(); err != nil {
		ch.log.Warnf("error while closing subscriber %#v", err)
	}
}

func (ch *clientHandler) close() {
	if err := ch.conn.Close(); err != nil {
		ch.log.Warnf("error while closing %#v", err)
	}
}
