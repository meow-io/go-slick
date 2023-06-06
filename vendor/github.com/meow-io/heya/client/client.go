package client

import (
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
)

const (
	Open = iota
	Closing
	Closed
	Reconnecting

	DefaultPort = 8337
)

type Token struct {
	Value     []byte
	Seq       uint64
	StartTime time.Time
	EndTime   time.Time
}

type PushToken struct {
	Value     string
	StartTime time.Time
}

type Message struct {
	Body []byte
	Seq  uint64
}

type listResponse struct {
	tokens []*Token
}

type iosListResponse struct {
	tokens []*PushToken
}

type iosAddResponse struct {
	token string
}

type iosDeleteResponse struct {
	token string
}

type opening struct {
	Version uint64
}

type recvResponse struct {
	digest []byte
}

type RegisterIncomingResponse struct {
	token []byte
}

type pongResponse struct {
}

type deauthAllResponse struct {
}

type authResponse struct {
	token     []byte
	startTime time.Time
	endTime   time.Time
}

type messageResponse struct {
	body  *[]byte
	token []byte
	seq   uint64
}

type trimResponse struct {
	token []byte
	seq   uint64
	count uint64
}

type deauthResponse struct {
	token []byte
}

type extendResponse struct {
	token      []byte
	newEndTime time.Time
}

type Notification struct {
	Seq   uint64
	Token []byte
}

type DoneIntro struct{}

type command struct {
	ctx       context.Context
	name      string
	response  chan any
	err       chan error
	writer    func() error
	readerMap map[string]func() (any, error)
}

type StateRecv func(int)

type Config struct {
	Host            string
	Port            int
	Reconnect       bool
	Ping            bool
	NewState        StateRecv
	Debug           bool
	PrivateKeyPKCS1 []byte
	Cert            []byte
}

type Client struct {
	PrivateKeyPKCS1 []byte
	Certificate     []byte
	Host            string
	Port            int
	Version         uint64

	afterIntro       bool
	notifications    chan interface{}
	conn             *tls.Conn
	tlsConfig        *tls.Config
	log              *zap.SugaredLogger
	State            int
	reconnectOnClose bool
	commands         chan *command
	responses        chan *command
	incomingMessages chan []byte
	ctx              context.Context
	wg               sync.WaitGroup
	cancelFunc       context.CancelFunc
	ping             bool
	newState         StateRecv
	stateLock        sync.Mutex
}

func NewClient(config *Config) (*Client, error) {
	if len(config.Cert) != 0 || len(config.PrivateKeyPKCS1) != 0 {
		return nil, errors.New("expected Cert and PrivateKeyPKCS1 to be zero-length")
	}

	priv, err := rsa.GenerateKey(crypto_rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	publicBytesDigest := sha256.Sum256(priv.Public().(*rsa.PublicKey).N.Bytes())
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.Unix()),
		NotBefore:             now,
		NotAfter:              now.AddDate(10, 0, 0), // Valid for one day
		SubjectKeyId:          publicBytesDigest[:],
		BasicConstraintsValid: true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageKeyEncipherment,
	}
	cert, err := x509.CreateCertificate(crypto_rand.Reader, template, template, priv.Public(), priv)
	if err != nil {
		return nil, err
	}
	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	config.Cert = cert
	config.PrivateKeyPKCS1 = privBytes

	return NewClientFromKey(config)
}

func NewClientFromKey(config *Config) (*Client, error) {
	privateKey, err := x509.ParsePKCS1PrivateKey(config.PrivateKeyPKCS1)
	if err != nil {
		return nil, err
	}
	var outCert tls.Certificate
	outCert.Certificate = append(outCert.Certificate, config.Cert)
	outCert.PrivateKey = privateKey
	tlsConfig := &tls.Config{
		InsecureSkipVerify: os.Getenv("SKIP_VERIFY") == "1",
		Certificates:       []tls.Certificate{outCert},
		CipherSuites:       []uint16{tls.TLS_CHACHA20_POLY1305_SHA256},
		MinVersion:         tls.VersionTLS13,
	} // #nosec G402

	log := newLogger(config.Debug)

	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Client{
		PrivateKeyPKCS1: config.PrivateKeyPKCS1,
		Certificate:     config.Cert,
		Host:            config.Host,
		Port:            config.Port,
		State:           Closed,

		notifications:    make(chan interface{}, 100),
		responses:        make(chan *command, 3),
		commands:         make(chan *command, 3),
		incomingMessages: make(chan []byte),
		afterIntro:       false,
		tlsConfig:        tlsConfig,
		log:              log,
		reconnectOnClose: config.Reconnect,
		ping:             config.Ping,
		newState:         config.NewState,
		ctx:              ctx,
		cancelFunc:       cancelFunc,
	}

	return c, nil
}

func (c *Client) Notifications() chan interface{} {
	return c.notifications
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.command(ctx, "ping", func() error {
		_, err := c.conn.Write([]byte("PING\n"))
		return err
	}, map[string]func() (any, error){"PONG\n": func() (any, error) {
		return &pongResponse{}, nil
	}})
	return err
}

func (c *Client) Send(ctx context.Context, sendToken, body []byte) error {
	digest := sha256.Sum256(body)
	_, err := c.command(ctx, "send", func() error {
		if _, err := c.conn.Write([]byte(fmt.Sprintf("SEND %x %x %d\n", sendToken, digest[:], len(body)))); err != nil {
			return err
		}
		if _, err := c.conn.Write(body); err != nil {
			return err
		}
		if _, err := c.conn.Write([]byte("\n")); err != nil {
			return err
		}
		return nil
	}, map[string]func() (any, error){"RECV ": func() (any, error) {
		hexDigest, err := c.readHex(64, 0xa)
		if err != nil {
			return nil, err
		}
		return &recvResponse{hexDigest}, nil
	}})
	return err
}

func (c *Client) RegisterIncoming(ctx context.Context, tokenHex string) (*RegisterIncomingResponse, error) {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return nil, err
	}

	response, err := c.command(ctx, "inco", func() error {
		return c.writef("INCO %x\n", token)
	}, map[string]func() (any, error){"INCO ": func() (any, error) {
		hexDigest, err := c.readHex(64, 0xa)
		if err != nil {
			return nil, err
		}
		return &RegisterIncomingResponse{hexDigest}, nil
	}})
	return response.(*RegisterIncomingResponse), err
}

func (c *Client) MakeSendToken(ctx context.Context, startTime time.Time, endTime time.Time) ([]byte, error) {
	response, err := c.command(ctx, "auth", func() error {
		return c.writef("AUTH %d %d\n", startTime.Unix(), endTime.Unix())
	}, map[string]func() (any, error){"AUTH ": func() (any, error) {
		hexDigest, err := c.readHex(64, 0x20)
		if err != nil {
			return nil, err
		}
		startInt, err := c.readUint(10, 0x20)
		if err != nil {
			return nil, err
		}
		endInt, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}
		inStartTime := time.Unix(int64(startInt), 0)
		inEndTime := time.Unix(int64(endInt), 0)
		if inStartTime.Unix() != startTime.Unix() {
			return nil, c.handleError(fmt.Errorf("start time %d didn't match %d", inStartTime.Unix(), startTime.Unix()))
		}
		if inEndTime.Unix() != endTime.Unix() {
			return nil, c.handleError(fmt.Errorf("end time %d didn't match %d", inEndTime.Unix(), endTime.Unix()))
		}

		return &authResponse{hexDigest, inStartTime, inEndTime}, nil
	}})
	if err != nil {
		return nil, err
	}
	return response.(*authResponse).token, nil
}

func (c *Client) RevokeSendToken(ctx context.Context, token []byte) error {
	_, err := c.command(ctx, "deau", func() error {
		return c.writef("DEAU %x\n", token)
	}, map[string]func() (any, error){"DEAU ": func() (any, error) {
		hexDigest, err := c.readHex(64, 0xa)
		if err != nil {
			return nil, err
		}
		return &deauthResponse{hexDigest}, nil
	}})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) ExtendSendToken(ctx context.Context, token []byte, addedSeconds int) (time.Time, error) {
	resp, err := c.command(ctx, "extd", func() error {
		return c.writef("EXTD %x %d\n", token, addedSeconds)
	}, map[string]func() (any, error){"EXTD ": func() (any, error) {
		hexDigest, err := c.readHex(64, 0x20)
		if err != nil {
			return nil, err
		}
		newEndTime, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}
		return &extendResponse{hexDigest, time.Unix(int64(newEndTime), 0)}, nil
	}})
	if err != nil {
		return time.Time{}, err
	}
	return resp.(*extendResponse).newEndTime, nil
}

func (c *Client) ListTokens(ctx context.Context) ([]*Token, error) {
	resp, err := c.command(ctx, "list", func() error {
		return c.writef("LIST\n")
	}, map[string]func() (any, error){"LIST ": func() (any, error) {
		tokenCount, err := c.readUint(10, 0xa)
		if err != nil {
			c.log.Infof("!")
			return nil, err
		}

		tokens := make([]*Token, tokenCount)
		for i := range tokens {
			token, err := c.readHex(64, 0x20)
			if err != nil {
				return nil, err
			}
			seq, err := c.readUint(10, 0x20)
			if err != nil {
				return nil, err
			}
			startTime, err := c.readUint(10, 0x20)
			if err != nil {
				return nil, err
			}
			endTime, err := c.readUint(10, 0xa)
			if err != nil {
				return nil, err
			}
			tokens[i] = &Token{token, seq, time.Unix(int64(startTime), 0), time.Unix(int64(endTime), 0)}
		}

		return &listResponse{tokens}, nil
	}})
	if err != nil {
		return nil, err
	}
	return resp.(*listResponse).tokens, nil
}

func (c *Client) ListIOSPushTokens(ctx context.Context) ([]*PushToken, error) {
	resp, err := c.command(ctx, "iosl", func() error {
		return c.writef("IOSL\n")
	}, map[string]func() (any, error){"IOSL ": func() (any, error) {
		tokenCount, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}

		tokens := make([]*PushToken, tokenCount)
		for i := range tokens {
			digest, err := c.readBytes(64, 0x20)
			if err != nil {
				return nil, err
			}
			createdAtTime, err := c.readUint(10, 0xa)
			if err != nil {
				return nil, err
			}
			tokens[i] = &PushToken{string(digest), time.Unix(int64(createdAtTime), 0)}
		}

		return &iosListResponse{tokens}, nil
	}})
	if err != nil {
		return nil, err
	}
	return resp.(*iosListResponse).tokens, nil
}

func (c *Client) AddIOSPushToken(ctx context.Context, token string) error {
	_, err := c.command(ctx, "iosa", func() error {
		return c.writef("IOSA %s\n", token)
	}, map[string]func() (any, error){"IOSA ": func() (any, error) {
		token, err := c.readHex(64, 0xa)
		if err != nil {
			return nil, err
		}
		return &iosAddResponse{string(token)}, nil
	}})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) DeleteIOSPushToken(ctx context.Context, token string) error {
	_, err := c.command(ctx, "iosd", func() error {
		return c.writef("IOSD %s\n", token)
	}, map[string]func() (any, error){"IOSD ": func() (any, error) {
		token, err := c.readBytes(64, 0xa)
		if err == nil {
			return nil, err
		}
		return &iosDeleteResponse{string(token)}, nil
	}})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Want(ctx context.Context, token []byte, seq uint64) (*Message, error) {
	resp, err := c.command(ctx, "want", func() error {
		return c.writef("WANT %x %d\n", token, seq)
	}, map[string]func() (any, error){"GIVE ": func() (any, error) {
		token, err := c.readHex(64, 0x20)
		if err != nil {
			return nil, err
		}
		seq, err := c.readUint(10, 0x20)
		if err != nil {
			return nil, err
		}
		length, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}
		body, err := c.readBytes(int(length), 0xa)
		if err != nil {
			return nil, err
		}
		return &messageResponse{&body, token, seq}, nil
	}, "GONE ": func() (any, error) {
		token, err := c.readHex(64, 0x20)
		if err != nil {
			return nil, err
		}
		seq, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}
		return &messageResponse{nil, token, seq}, nil
	}})
	if err != nil {
		return nil, err
	}
	msgResponse := resp.(*messageResponse)
	if msgResponse.body == nil {
		return nil, nil
	}
	return &Message{*msgResponse.body, msgResponse.seq}, nil
}

func (c *Client) Trim(ctx context.Context, token []byte, seq uint64) (uint64, error) {
	resp, err := c.command(ctx, "trim", func() error {
		return c.writef("TRIM %x %d\n", token, seq)
	}, map[string]func() (any, error){"TRIM ": func() (any, error) {
		token, err := c.readHex(64, 0x20)
		if err != nil {
			return nil, err
		}
		seq, err := c.readUint(10, 0x20)
		if err != nil {
			return nil, err
		}
		count, err := c.readUint(10, 0xa)
		if err != nil {
			return nil, err
		}
		return &trimResponse{token, seq, count}, nil
	}})
	if err != nil {
		return 0, err
	}
	return resp.(*trimResponse).count, nil
}

func (c *Client) DeauthAll(ctx context.Context) error {
	_, err := c.command(ctx, "deauthAll", func() error {
		return c.writef("DALL\n")
	}, map[string]func() (any, error){"DALL\n": func() (any, error) {
		return &deauthAllResponse{}, nil
	}})
	return err
}

func (c *Client) Connect(ctx context.Context) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if c.State == Open {
		return nil
	}
	d := tls.Dialer{
		Config: c.tlsConfig,
	}
	hostPort := fmt.Sprintf("%s:%d", c.Host, c.Port)
	c.log.Debugf("starting connection to %s", hostPort)
	netconn, err := d.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		c.log.Warnf("error while starting %#v", err)
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			c.log.Warnf("error while starting op error %#v", opErr.Err)
		}
		if c.reconnectOnClose {
			go c.reconnect()
		}
		return err
	}
	c.conn = netconn.(*tls.Conn)
	if d, ok := ctx.Deadline(); ok {
		if err := c.conn.SetReadDeadline(d); err != nil {
			return c.handleError(err)
		}
	}
	opening, err := c.readOpening(ctx)
	if err != nil {
		return c.handleError(err)
	}
	c.Version = opening.Version

	c.afterIntro = false
	c.setState(Open)
	c.startCommandProcessor()
	c.startResponseProcessor()
	if c.ping {
		c.startPinger()
	}
	go c.readIncomingMessage()

	return nil
}

func (c *Client) CloseWithoutReconnect() {
	c.reconnectOnClose = false
	c.Close()
}

func (c *Client) Close() {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if c.State != Open {
		return
	}

	c.setState(Closing)
	if err := c.conn.Close(); err != nil {
		c.log.Warnf("error while closing %#v", err)
	}
	c.cancelFunc()
	c.wg.Wait()
	c.setState(Closed)
	ctx, cancelFunc := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancelFunc = cancelFunc
	if c.reconnectOnClose {
		go c.reconnect()
	} else {
		c.log.Debugf("not reconnecting")
	}
}

func (c *Client) reconnect() {
	c.log.Debugf("reconnecting")
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if c.State != Closed {
		c.log.Debugf("aborting reconnect, connection is not closed")
		return
	}
	c.setState(Reconnecting)
	maxWait := 30 * time.Second
	go func() {
		i := 0
		c.log.Debugf("reconnect attempt %d", i)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.Connect(ctx); err != nil {
				c.log.Debugf("error while reconnecting %#v", err)
			}
			if c.State == Open {
				return
			}
			t := (2 << i) * 100 * time.Millisecond
			if t > maxWait {
				t = maxWait
			}
			i++
			c.log.Debugf("reconnecting in %#v", t)
			time.Sleep(t)
			c.log.Debugf("done sleeping")
		}
	}()
}

func (c *Client) writef(tmp string, args ...interface{}) error {
	b := []byte(fmt.Sprintf(tmp, args...))
	_, err := c.conn.Write(b)
	if err != nil {
		return c.handleError(err)
	}
	return nil
}

func (c *Client) handleError(err error) error {
	go c.Close()
	return err
}

func (c *Client) readOpening(ctx context.Context) (*opening, error) {
	if d, ok := ctx.Deadline(); ok {
		if err := c.conn.SetReadDeadline(d); err != nil {
			return nil, err
		}
	}
	commandBuf := make([]byte, 5)
	_, err := io.ReadFull(c.conn, commandBuf)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(commandBuf, []byte("HEYA ")) {
		return nil, fmt.Errorf("expected HEYA, got %x", commandBuf)
	}
	version, err := c.readUint(10, 0xa)
	if err != nil {
		return nil, err
	}

	return &opening{Version: version}, nil
}

func (c *Client) readBytes(l int, terminator byte) ([]byte, error) {
	b := make([]byte, l+1)
	if _, err := io.ReadFull(c.conn, b); err != nil {
		return nil, err
	}
	if b[l] != terminator {
		return nil, fmt.Errorf("expected to read %x, got %x", terminator, b[l])
	}
	return b[0:l], nil
}

func (c *Client) readHex(l int, terminator byte) ([]byte, error) {
	b, err := c.readBytes(l, terminator)
	if err != nil {
		return nil, err
	}
	h, err := hex.DecodeString(string(b))
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (c *Client) readUint(maxlen int, terminator byte) (uint64, error) {
	intBytes := make([]byte, maxlen+1)
	i := 0
	terminatorRead := false
	for i = 0; i != maxlen+1; i++ {
		if _, err := io.ReadFull(c.conn, intBytes[i:i+1]); err != nil {
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

func (c *Client) startCommandProcessor() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case cmd := <-c.commands:
				c.log.Debugf("processing command %s for write", cmd.name)
				if d, ok := cmd.ctx.Deadline(); ok {
					c.log.Debugf("command %s has write timeout of %s", cmd.name, time.Until(d))
					if err := c.conn.SetWriteDeadline(d); err != nil {
						cmd.err <- err
						return
					}
				}
				c.log.Debugf("command %s calling its writer", cmd.name)
				if err := cmd.writer(); err != nil {
					c.log.Debugf("command %s had an error while writing %#v", cmd.name, err)
					cmd.err <- err
					return
				}
				select {
				case c.responses <- cmd:
					c.log.Debugf("command %s queued for response", cmd.name)
				case <-c.ctx.Done():
					c.log.Debugf("command %s aborted", cmd.name)
				}
			}
		}
	}()
}

func (c *Client) startResponseProcessor() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case cmd := <-c.responses:
				c.log.Debugf("processing command %s for read", cmd.name)
				if d, ok := cmd.ctx.Deadline(); ok {
					c.log.Debugf("command %s has read timeout of %s", cmd.name, time.Until(d))
					if err := c.conn.SetReadDeadline(d); err != nil {
						cmd.err <- err
						return
					}
				}
				var reader func() (any, error)

				c.log.Debugf("getting reader for %s", cmd.name)
				select {
				case <-c.ctx.Done():
					return
				case start := <-c.incomingMessages:
					c.log.Debugf("getting reader for %s with start %s", cmd.name, start)
					var ok bool
					if reader, ok = cmd.readerMap[string(start)]; !ok {
						cmd.err <- fmt.Errorf("expected %#v, got %s", maps.Keys(cmd.readerMap), start)
						return
					}
				}

				c.log.Debugf("calling reader for %s", cmd.name)
				response, err := reader()
				if err != nil {
					c.log.Debugf("reader for %s returned an error %#v", cmd.name, err)
					cmd.err <- err
					return
				}
				c.log.Debugf("reader for %s returned a response %#v", cmd.name, response)
				cmd.response <- response
				go c.readIncomingMessage()
			}
		}
	}()
}

func (c *Client) startPinger() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			c.log.Debugf("pinging")
			startTime := time.Now()
			ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			defer cancel()
			err := c.Ping(ctx)
			if err != nil {
				c.log.Warnf("error during ping %#v", c.handleError(err))
				return
			}
			c.log.Debugf("pinged duration=%s", time.Since(startTime))

			select {
			case <-time.After(5*time.Second - time.Since(startTime)):
				// do nothing
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func (c *Client) readIncomingMessage() {
	c.log.Debugf("reading incoming message")
	conn := c.conn
	if c.State != Open {
		c.log.Debugf("stop incoming message, closed")
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		e := c.handleError(err)
		if c.State == Open {
			c.log.Warnf("error while reading %#v", e)
		}
		return
	}
	c.log.Debugf("reading message...")
	commandBuf := make([]byte, 5)
	_, err := io.ReadFull(c.conn, commandBuf)
	if err != nil {
		if c.State == Open {
			c.log.Warnf("error while reading message %#v", c.handleError(err))
		}
		return
	}

	c.log.Debugf("incoming message read %s", commandBuf[0:4])
	c.log.Debugf("afterIntro %t", c.afterIntro)

	if !c.afterIntro {
		c.log.Debugf("reading in intro")
		if bytes.Equal(commandBuf, []byte("LAST ")) {
			c.log.Debugf("reading LAST")
			token, err := c.readHex(64, 0x20)
			if err != nil {
				c.log.Warnf("error while reading last message %#v", c.handleError(err))
				return
			}
			seq, err := c.readUint(10, 0xa)
			if err != nil {
				c.log.Warnf("error while reading last message %#v", c.handleError(err))
				return
			}
			c.log.Debugf("sending notification for %d", seq)
			c.notifications <- &Notification{seq, token}
			go c.readIncomingMessage()
			return
		} else if bytes.Equal(commandBuf, []byte("DONE\n")) {
			c.log.Debugf("reading DONE")
			c.afterIntro = true
			c.notifications <- &DoneIntro{}
			go c.readIncomingMessage()
			return
		} else {
			c.log.Errorf("expected LAST or DONE, got %x", commandBuf)
			c.Close()
			return
		}
	}

	if bytes.Equal(commandBuf, []byte("HAVE ")) { // "HAVE "
		token, err := c.readHex(64, 0x20)
		if err != nil {
			c.log.Warnf("error while reading have message %#v", c.handleError(err))
			return
		}
		seq, err := c.readUint(10, 0xa)
		if err != nil {
			c.log.Warnf("error while reading have message %#v", c.handleError(err))
			return
		}
		c.log.Debugf("sending notification for %d", seq)
		c.notifications <- &Notification{seq, token}
		go c.readIncomingMessage()
		return
	}

	select {
	case c.incomingMessages <- commandBuf:
	case <-c.ctx.Done():
	}
}

func (c *Client) command(ctx context.Context, name string, writer func() error, readerMap map[string]func() (any, error)) (any, error) {
	cmd := &command{
		ctx:       ctx,
		name:      name,
		writer:    writer,
		readerMap: readerMap,
		response:  make(chan any),
		err:       make(chan error),
	}
	if d, ok := ctx.Deadline(); ok {
		c.log.Debugf("executing %s with a timer %s", name, time.Until(d))
		timer := time.After(time.Until(d))
		select {
		case c.commands <- cmd:
			c.log.Debugf("command %s enqueued", name)
			// do nothing
		case <-timer:
			c.log.Debugf("command %s exceeded timer", name)
			return nil, context.DeadlineExceeded
		case <-c.ctx.Done():
			c.log.Debugf("command %s stopping because client is stopping", name)
			return nil, fmt.Errorf("connection closed")
		}

		c.log.Debugf("command %s getting response", name)
		select {
		case r := <-cmd.response:
			c.log.Debugf("command %s response recv %#v", name, r)
			return r, nil
		case err := <-cmd.err:
			c.log.Debugf("command %s response error recv %#v", name, err)
			return nil, c.handleError(err)
		case <-timer:
			c.log.Debugf("command %s timer exceeded", name)
			return nil, c.handleError(context.DeadlineExceeded)
		case <-c.ctx.Done():
			c.log.Debugf("command %s stopping because client is stopping", name)
			return nil, c.handleError(fmt.Errorf("connection closed"))
		}
	} else {
		c.log.Debugf("executing %s without a timer", name)
		select {
		case c.commands <- cmd:
			c.log.Debugf("command %s enqueued", name)
			// do nothing
		case <-c.ctx.Done():
			c.log.Debugf("command %s stopping because client is stopping", name)
			return nil, c.handleError(fmt.Errorf("connection closed"))
		}

		select {
		case r := <-cmd.response:
			c.log.Debugf("command %s response recv %#v", name, r)
			return r, nil
		case err := <-cmd.err:
			c.log.Debugf("command %s response error recv %#v", name, err)
			return nil, c.handleError(err)
		case <-c.ctx.Done():
			c.log.Debugf("command %s stopping because client is stopping", name)
			return nil, c.handleError(fmt.Errorf("connection closed"))
		}
	}
}

func (c *Client) setState(s int) {
	c.State = s
	if c.newState != nil {
		c.newState(s)
	}
}

func newLogger(debug bool) *zap.SugaredLogger {
	level := zapcore.InfoLevel
	if debug {
		level = zapcore.DebugLevel
	}
	opts := []zap.Option{
		zap.Fields(zap.String("source", "server")),
	}

	de := zap.NewDevelopmentEncoderConfig()
	consoleEncoder := zapcore.NewConsoleEncoder(de)
	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)
	logger := zap.New(core, opts...)
	sugar := logger.Sugar()
	return sugar
}
