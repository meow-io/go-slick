package heya

import (
	"context"
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/kevinburke/nacl"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/internal/test"
	heya_server "github.com/meow-io/heya"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const PORT = 10124

func TestMain(m *testing.M) {
	// disables tlsverify
	os.Setenv("SKIP_VERIFY", "1")
	os.Exit(test.DBCleanup(m.Run))
}

type testPusher struct {
	testPushes []string
}

func (tp *testPusher) DoPush(token string) error {
	tp.testPushes = append(tp.testPushes, token)
	return nil
}

type testServer struct {
	pusher *testPusher
	server *heya_server.Server
}

var (
	ts             *testServer
	token1, token2 []byte
)

func newServer(port int) (*testServer, error) {
	os.Remove("key.pem")
	os.Remove("cert.pem")

	key, err := rsa.GenerateKey(crypto_rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	keyOut, err := os.Create("key.pem")
	if err != nil {
		return nil, err
	}

	// Generate a pem block with the private key
	if err := pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}); err != nil {
		return nil, err
	}

	tml := x509.Certificate{
		// you can add any attr that you need
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(5, 0, 0),
		// you have to generate a different serial number each execution
		SerialNumber: big.NewInt(123123),
		Subject: pkix.Name{
			CommonName:   "New Name",
			Organization: []string{"New Org."},
		},
		BasicConstraintsValid: true,
	}
	cert, err := x509.CreateCertificate(crypto_rand.Reader, &tml, &tml, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	// Generate a pem block with the certificate
	certOut, err := os.Create("cert.pem")
	if err != nil {
		return nil, err
	}
	if err := pem.Encode(certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	}); err != nil {
		return nil, err
	}

	postgresHost := os.Getenv("POSTGRES_HOST")
	if postgresHost == "" {
		postgresHost = "localhost"
	}
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}
	c := &heya_server.Config{
		Port:         port,
		APNSCertPath: "",
		TLSCertPath:  "cert.pem",
		TLSKeyPath:   "key.pem",
		DatabaseURL:  fmt.Sprintf("pg://heya_development:heya@%s:5432/heya_development?sslmode=disable", postgresHost),
		RedisURL:     fmt.Sprintf("redis://%s:6379", redisHost),
		Debug:        true,
	}

	testPushes := make([]string, 0)

	p := &testPusher{testPushes}
	s, err := heya_server.NewServerWithPusher(c, makeLogger(true), p)
	if err != nil {
		return nil, err
	}
	return &testServer{p, s}, nil
}

type report struct {
	groupID ids.ID
	url     string
	added   bool
}

type testManager struct {
	manager  *Manager
	db       *db.Database
	messages chan *MessageImpl
	reports  chan *report
}

type testManagerMaker struct {
	managers []*testManager
}

func newTestManagerMaker() *testManagerMaker {
	return &testManagerMaker{
		managers: make([]*testManager, 0),
	}
}

func setup(t *testing.T) {
	require := require.New(t)
	var err error
	ts, err = newServer(PORT)
	require.Nil(err)

	tokenBytes1 := make([]byte, 32)
	tokenBytes2 := make([]byte, 32)
	if _, err := crypto_rand.Read(tokenBytes1); err != nil {
		require.Nil(err)
	}
	token1 = tokenBytes1[:]
	if _, err := crypto_rand.Read(tokenBytes2); err != nil {
		require.Nil(err)
	}
	token2 = tokenBytes2[:]

	require.Nil(ts.server.Start())
}

func (tmm *testManagerMaker) teardown() {
	shutdownErrors := make(chan error, len(tmm.managers))

	for _, tm := range tmm.managers {
		go func(tm *testManager) {
			if err := tm.manager.Shutdown(); err != nil {
				shutdownErrors <- err
				return
			}

			if err := tm.db.Shutdown(); err != nil {
				shutdownErrors <- err
				return
			}

			shutdownErrors <- nil
		}(tm)
	}

	for range tmm.managers {
		if err := <-shutdownErrors; err != nil {
			panic(err)
		}
	}
	ts.server.Stop()
}

func (tmm *testManagerMaker) AddManager(prefix string) *testManager {
	messages := make(chan *MessageImpl, 100)
	config := config.NewConfig(config.WithLoggingPrefix(prefix))
	db := test.NewTestDatabase(config)
	reports := make(chan *report)
	manager, err := NewManager(config, db, false, func(mi []*MessageImpl) error {
		for _, m := range mi {
			messages <- m
		}
		return nil
	}, func(id ids.ID, url string, added bool) error {
		reports <- &report{id, url, added}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if err := manager.Start(); err != nil {
		panic(err)
	}

	tm := &testManager{manager, manager.db, messages, reports}
	tmm.managers = append(tmm.managers, tm)
	return tm
}

func TestTwoPartyConnection(t *testing.T) {
	setup(t)
	require := require.New(t)
	require.Nil(ts.server.AddInboxToken(token1))
	require.Nil(ts.server.AddInboxToken(token2))

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	m2 := tmm.AddManager("manager2")
	g1 := ids.NewID()
	g2 := ids.NewID()

	require.Nil(m1.db.Run("create transport", func() error {
		if err := m1.manager.CreateTransport(hex.EncodeToString(token1), "localhost", PORT); err != nil {
			return err
		}
		return m1.manager.ReportGroup(g1)
	}))
	require.Nil(m2.db.Run("create transport", func() error {
		if err := m2.manager.CreateTransport(hex.EncodeToString(token2), "localhost", PORT); err != nil {
			return err
		}
		return m2.manager.ReportGroup(g2)
	}))

	report1 := <-m1.reports
	report2 := <-m2.reports

	require.Nil(m1.manager.Send(report1.url, report2.url, []byte("hello")))
	m := <-m2.messages
	require.Equal(report1.url, m.From())
	require.Equal([]byte("hello"), m.Body())

	require.Nil(m2.manager.Send(report2.url, report1.url, []byte("there")))
	m = <-m1.messages
	require.Equal(report2.url, m.From())
	require.Equal([]byte("there"), m.Body())
}

func TestManyMessagesWhileOffline(t *testing.T) {
	setup(t)
	require := require.New(t)
	require.Nil(ts.server.AddInboxToken(token1))
	require.Nil(ts.server.AddInboxToken(token2))

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	m2 := tmm.AddManager("manager2")
	g1 := ids.NewID()
	g2 := ids.NewID()

	require.Nil(m1.db.Run("create transport", func() error {
		if err := m1.manager.CreateTransport(hex.EncodeToString(token1), "localhost", PORT); err != nil {
			return err
		}
		return m1.manager.ReportGroup(g1)
	}))
	require.Nil(m2.db.Run("create transport", func() error {
		if err := m2.manager.CreateTransport(hex.EncodeToString(token2), "localhost", PORT); err != nil {
			return err
		}
		return m2.manager.ReportGroup(g2)
	}))

	report1 := <-m1.reports
	report2 := <-m2.reports

	require.Nil(m2.manager.Shutdown())

	for i := 0; i != 505; i++ {
		require.Nil(m1.manager.Send(report1.url, report2.url, []byte(fmt.Sprintf("hello %d", i))))
	}
	require.Nil(m2.manager.Start())
	for i := 0; i != 505; i++ {
		m := <-m2.messages
		require.Equal(report1.url, m.From())
		require.Equal([]byte(fmt.Sprintf("hello %d", i)), m.Body())
	}
}

func TestGroupReporting(t *testing.T) {
	setup(t)
	require := require.New(t)
	require.Nil(ts.server.AddInboxToken(token1))

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	g1 := ids.NewID()

	require.Nil(m1.db.Run("create transport", func() error {
		if err := m1.manager.CreateTransport(hex.EncodeToString(token1), "localhost", PORT); err != nil {
			return err
		}
		m1.manager.transportLock.RLock()
		require.Equal(1, len(m1.manager.transportMap))
		m1.manager.transportLock.RUnlock()
		return nil
	}))

	require.Eventually(func() bool {
		m1.manager.sendTokensLock.RLock()
		defer m1.manager.sendTokensLock.RUnlock()
		return len(m1.manager.sendTokens) == 5
	}, 3*time.Second, 100*time.Millisecond)

	require.Nil(m1.db.Run("report group", func() error {
		return m1.manager.ReportGroup(g1)
	}))
	report1 := <-m1.reports
	require.Equal(g1, report1.groupID)
}

func TestBadEncryptionKeyMessage(t *testing.T) {
	setup(t)
	require := require.New(t)
	require.Nil(ts.server.AddInboxToken(token1))
	require.Nil(ts.server.AddInboxToken(token2))

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	m2 := tmm.AddManager("manager2")
	g1 := ids.NewID()
	g2 := ids.NewID()

	require.Nil(m1.db.Run("create transport", func() error {
		if err := m1.manager.CreateTransport(hex.EncodeToString(token1), "localhost", PORT); err != nil {
			return err
		}
		return m1.manager.ReportGroup(g1)
	}))
	require.Nil(m2.db.Run("create transport", func() error {
		if err := m2.manager.CreateTransport(hex.EncodeToString(token2), "localhost", PORT); err != nil {
			return err
		}
		return m2.manager.ReportGroup(g2)
	}))

	report1 := <-m1.reports
	report2 := <-m2.reports

	parsed, err := ParseURL(report2.url)
	if err != nil {
		require.Nil(err)
	}
	parsed.PublicBytes = *nacl.NewKey()

	require.Nil(m1.manager.Send(report1.url, parsed.URL(), []byte("hello")))
}

func TestBadEncodedMessage(t *testing.T) {
	setup(t)
	require := require.New(t)
	require.Nil(ts.server.AddInboxToken(token1))
	require.Nil(ts.server.AddInboxToken(token2))

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	m2 := tmm.AddManager("manager2")
	g1 := ids.NewID()
	g2 := ids.NewID()

	require.Nil(m1.db.Run("create transport", func() error {
		if err := m1.manager.CreateTransport(hex.EncodeToString(token1), "localhost", PORT); err != nil {
			return err
		}
		return m1.manager.ReportGroup(g1)
	}))
	require.Nil(m2.db.Run("create transport", func() error {
		if err := m2.manager.CreateTransport(hex.EncodeToString(token2), "localhost", PORT); err != nil {
			return err
		}
		return m2.manager.ReportGroup(g2)
	}))

	var transport *transport
	for _, trans := range m1.manager.transportMap {
		transport = trans
		break
	}
	report2 := <-m2.reports
	parsed, err := ParseURL(report2.url)
	if err != nil {
		require.Nil(err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()
	require.Nil(transport.client.Send(ctx, parsed.SendToken[:], []byte("hello there!")))
}

func makeLogger(debug bool) *zap.SugaredLogger {
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
