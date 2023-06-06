package heya

import (
	"io"

	// load postgres
	_ "github.com/lib/pq"

	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/lopezator/migrator"
	"github.com/xo/dburl"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	StateNew = iota
	StateRunning
	StateStopping
	StateStopped
)

type runnerFunc func(tx *sqlx.Tx) error

type mailbox struct {
	Digest []byte
}

type inboxToken struct {
	Token string `db:"token"`
}

type sendToken struct {
	Digest    []byte `db:"digest"`
	Token     []byte `db:"token"`
	NextSeq   uint64 `db:"next_seq"`
	StartTime uint64 `db:"start_time"`
	EndTime   uint64 `db:"end_time"`
}

type iosPushToken struct {
	Digest    []byte    `db:"digest"`
	Token     string    `db:"token"`
	CreatedAt time.Time `db:"created_at"`
}

type message struct {
	KeyDigest  []byte    `db:"key_digest"`
	SendToken  []byte    `db:"send_token"`
	Seq        uint64    `db:"seq"`
	Body       []byte    `db:"body"`
	BodyDigest []byte    `db:"body_digest"`
	CreatedAt  time.Time `db:"created_at"`
}

type subCommand struct {
	Add     bool
	Channel string
}

type pushCommand struct {
	CertDigest []byte
	Seq        uint64
}

type Config struct {
	APNSCertPath          string
	APNSTopic             string
	APNSProductionMode    bool
	TLSCertPath           string
	TLSKeyPath            string
	DatabaseURL           string
	RedisURL              string
	Port                  int
	Debug                 bool
	LogPath               string
	OverrideIncomingToken []byte
}

func NewConfig() *Config {
	return &Config{
		Port: 8337,
	}
}

type Server struct {
	config       *Config
	state        int
	listener     net.Listener
	db           *sqlx.DB
	commands     chan interface{}
	publisher    *redis.Client
	pusher       Pusher
	finished     sync.WaitGroup
	ctx          context.Context
	cancelFunc   context.CancelFunc
	pushCommands chan *pushCommand
	log          *zap.SugaredLogger
	redisOptions *redis.Options
}

func NewServer(config *Config) (*Server, error) {
	log := newLogger(config)

	pusher, err := newApplePusher(config, log.With("source", "pusher"))
	if err != nil {
		return nil, err
	}
	return NewServerWithPusher(config, log, pusher)
}

func NewServerWithPusher(config *Config, log *zap.SugaredLogger, pusher Pusher) (*Server, error) {
	log.Debug("opening database connection")
	rawDB, err := dburl.Open(config.DatabaseURL)
	if err != nil {
		return nil, err
	}
	db := sqlx.NewDb(rawDB, "postgres")

	migrate, err := migrator.New(
		migrator.Migrations(
			&migrator.Migration{
				Name: "Create initial tables",
				Func: func(tx *sql.Tx) error {
					_, err := tx.Exec(`

    CREATE TABLE inbox_tokens (
      token BYTEA PRIMARY KEY CHECK(length(token) = 32)
    );

    CREATE TABLE send_tokens (
      token BYTEA PRIMARY KEY CHECK(length(token) = 32),
      digest BYTEA NOT NULL CHECK(length(digest) = 32),
      next_seq BIGINT NOT NULL DEFAULT 0,
      start_time BIGINT NOT NULL,
      end_time BIGINT NOT NULL
    );

    CREATE TABLE mailbox (
      digest BYTEA PRIMARY KEY CHECK(length(digest) = 32)
    );

    CREATE TABLE messages (
      key_digest BYTEA NOT NULL CHECK(length(key_digest) = 32),
      seq BIGINT NOT NULL,
      send_token BYTEA NOT NULL CHECK(length(send_token) = 32),
      body BYTEA NOT NULL,
      body_digest BYTEA NOT NULL CHECK(length(body_digest) = 32),
      created_at timestamp NOT NULL DEFAULT NOW(),
      PRIMARY KEY(send_token, seq)
    );
    CREATE INDEX created_at_idx ON messages (created_at);

    CREATE TABLE ios_push_token (
      digest BYTEA NOT NULL,
      token VARCHAR(64) NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      PRIMARY KEY(digest, token)
    );

              `)
					return err
				},
			},
		),
	)

	if err != nil {
		return nil, fmt.Errorf("messaging: error creating migrations %w", err)
	}

	log.Debug("running migrations")
	// Migrate up
	if err := migrate.Migrate(db.DB); err != nil {
		return nil, err
	}

	redisOptions, err := redis.ParseURL(config.RedisURL)
	if err != nil {
		return nil, err
	}

	log.Debug("creating redis clients")
	publisher := redis.NewClient(redisOptions)

	return &Server{
		config:       config,
		state:        StateNew,
		listener:     nil,
		db:           db,
		commands:     make(chan interface{}, 100),
		publisher:    publisher,
		pusher:       pusher,
		pushCommands: make(chan *pushCommand, 100),
		log:          log,
		redisOptions: redisOptions,
	}, nil
}

func (s *Server) Start() error {
	if s.state != StateNew {
		return errors.New("server already started")
	}
	s.log.Debug("loading x509 key pair")
	cert, err := tls.LoadX509KeyPair(s.config.TLSCertPath, s.config.TLSKeyPath)
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		CipherSuites: []uint16{tls.TLS_CHACHA20_POLY1305_SHA256},
		MinVersion:   tls.VersionTLS13,
	}

	listenConfig := net.ListenConfig{
		KeepAlive: 30 * time.Second,
	}

	inner, err := listenConfig.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return err
	}

	s.log.Debugf("listening on %d", s.config.Port)
	s.listener = tls.NewListener(inner, tlsConfig)
	s.state = StateRunning

	s.finished.Add(1)
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		defer s.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case command := <-s.pushCommands:
				tokens, err := s.iosPushTokens(command.CertDigest)
				if err != nil {
					s.log.Warnf("error getting tokens %#v", err)
					continue
				}
				for _, token := range tokens {
					if err := s.pusher.DoPush(token.Token); err != nil {
						s.log.Warnf("error pushing token %#v", err)
						continue
					}
				}
			}
		}
	}()

	s.finished.Add(1)
	go func() {
		defer s.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case command := <-s.commands:
				s.log.Debugf("handling %#v command", command)
				switch c := command.(type) {
				case *sendCommand:
					s.handleSendCommand(c)
				case *wantCommand:
					s.handleWantCommand(c)
				case *iosAddCommand:
					s.handleIOSAddCommand(c)
				case *iosRemoveCommand:
					s.handleIOSRemoveCommand(c)
				case *iosListCommand:
					s.handleIOSListCommand(c)
				case *incomingCommand:
					s.handleIncomingCommand(c)
				case *authCommand:
					s.handleAuthCommand(c)
				case *deauthCommand:
					s.handleDeauthCommand(c)
				case *deauthAllCommand:
					s.handleDeauthAllCommand(c)
				case *pingCommand:
					s.handlePingCommand(c)
				case *listCommand:
					s.handleListCommand(c)
				case *trimCommand:
					s.handleTrimCommand(c)
				case *haveCommand:
					s.handleHaveCommand(c)
				case *extendCommand:
					s.handleExtendCommand(c)
				default:
					s.log.Fatalf("unhandled command %#v", c)
				}
			}
		}
	}()

	go func() {
		for {
			// Wait for a connection.
			conn, err := s.listener.Accept()
			if err != nil {
				if s.state != StateRunning {
					return
				}
				s.log.Errorf("error accepting %#v", err)
				panic(err)
			}
			s.log.Debugf("accepting connection %#v", conn)

			go func(c net.Conn) {
				s.log.Debugf("running a handler %#v", c)
				tlscon := conn.(*tls.Conn)
				if err := tlscon.Handshake(); err != nil {
					return
				}
				state := tlscon.ConnectionState()
				cert := state.PeerCertificates[0]
				certDigest := sha256.Sum256(cert.Raw)
				handler, err := newClientHandler(s, conn, s.redisOptions, certDigest, s.log.With("source", "client_handler"))
				defer handler.stop()
				if err != nil {
					s.log.Errorf("error creating handler %#v", err)
					return
				}

				if err := handler.handle(); err != nil {
					if err != io.EOF {
						s.log.Errorf("unexpected error %#v", err)
					}
				}
			}(conn)
		}
	}()
	s.ctx = ctx
	s.cancelFunc = cancelFunc

	return nil
}

func (s *Server) Wait() {
	s.finished.Wait()
}

func (s *Server) Stop() {
	s.state = StateStopping
	s.cancelFunc()
	s.finished.Wait()
	if err := s.listener.Close(); err != nil {
		s.log.Warnf("error while closing listener %#v", err)
	}
	s.state = StateStopped
}

func (s *Server) AddInboxToken(token []byte) error {
	if _, err := s.db.Exec("insert into inbox_tokens (token) VALUES ($1)", token); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleHaveCommand(c *haveCommand) {
	if err := c.ch.writef("HAVE %x %d\n", c.token, c.seq); err != nil {
		s.log.Errorf("error writing have: %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleSendCommand(c *sendCommand) {
	var certDigest []byte
	var nextSeqID uint64

	if err := s.runTx("inserting message", func(tx *sqlx.Tx) error {
		st := &sendToken{}
		if err := tx.Get(st, "select * from send_tokens where token = $1 for update", c.token); err != nil {
			return err
		}
		nextSeqID = st.NextSeq
		certDigest = st.Digest
		if _, err := tx.Exec("INSERT into messages (key_digest, body, body_digest, seq, send_token) VALUES ($1, $2, $3, $4, $5)", st.Digest, c.body, c.digest, nextSeqID, c.token); err != nil {
			return err
		}
		result, err := tx.Exec("UPDATE send_tokens SET next_seq = $1 WHERE token = $2", nextSeqID+1, c.token)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("expected to update 1 row, updated %d rows", rows)
		}
		return nil
	}); err != nil {
		s.log.Errorf("error while handling send command %#v", err)
		c.ch.close()
		return
	}

	tokenStr := hex.EncodeToString(c.token)

	s.log.Debugf("publishing to %s:%d", tokenStr, nextSeqID)
	i := s.publisher.Publish(s.ctx, tokenStr, strconv.FormatUint(nextSeqID+1, 10))

	if i.Err() != nil {
		s.log.Warnf("error while publishing %#v", i.Err())
	}
	s.pushCommands <- &pushCommand{certDigest, nextSeqID}

	if _, err := c.ch.conn.Write([]byte(fmt.Sprintf("RECV %x\n", c.digest))); err != nil {
		s.log.Errorf("error writing %#v", err)
	}
}

func (s *Server) handleWantCommand(c *wantCommand) {
	m := &message{}
	if err := s.db.Get(m, "select * from messages where seq = $1 and send_token = $2 and key_digest = $3", c.seq, c.token, c.ch.certDigest[:]); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := c.ch.writef("GONE %x %d\n", c.token, c.seq); err != nil {
				s.log.Errorf("error writing gone during want %#v", err)
			}
			return
		}
		c.ch.close()
		return
	}

	if _, err := c.ch.conn.Write([]byte(fmt.Sprintf("GIVE %x %d %d\n", m.SendToken, m.Seq, len(m.Body)))); err != nil {
		s.log.Errorf("error writing during want %#v", err)
	}

	if _, err := c.ch.conn.Write(m.Body); err != nil {
		s.log.Errorf("error writing during want %#v", err)
	}

	if _, err := c.ch.conn.Write([]byte{0xa}); err != nil {
		s.log.Errorf("error writing during want %#v", err)
	}
}

func (s *Server) handleIOSAddCommand(c *iosAddCommand) {
	if _, err := s.db.Exec("INSERT INTO ios_push_token (digest, token) VALUES ($1, $2) ON CONFLICT DO NOTHING", c.ch.certDigest[:], c.token); err != nil {
		s.log.Errorf("error while inserting %#v", err)
		c.ch.close()
		return
	}

	if _, err := c.ch.conn.Write([]byte(fmt.Sprintf("IOSA %s\n", c.token))); err != nil {
		s.log.Errorf("error while writing %#v", err)
	}
}

func (s *Server) handleIOSRemoveCommand(c *iosRemoveCommand) {
	if _, err := s.db.Exec("DELETE FROM ios_push_token where digest = $1 and token = $2", c.ch.certDigest[:], c.token); err != nil {
		s.log.Errorf("error while deleting %#v", err)
		c.ch.close()
		return
	}

	if _, err := c.ch.conn.Write([]byte(fmt.Sprintf("IOSD %s\n", c.token))); err != nil {
		s.log.Errorf("error while writing %#v", err)
	}
}

func (s *Server) handleIOSListCommand(c *iosListCommand) {
	tokens, err := s.iosPushTokens(c.ch.certDigest[:])
	if err != nil {
		s.log.Errorf("error while listing %#v", err)
		c.ch.close()
		return
	}

	if err := c.ch.writef("IOSL %d\n", len(tokens)); err != nil {
		s.log.Errorf("error while responding to IOSL %#v", err)
		c.ch.close()
		return
	}

	for _, t := range tokens {
		if err := c.ch.writef("%s %d\n", t.Token, t.CreatedAt.Unix()); err != nil {
			s.log.Errorf("error while responding to IOSL %#v", err)
			c.ch.close()
			return
		}
	}
}

func (s *Server) handleIncomingCommand(c *incomingCommand) {
	if bytes.Equal(c.token, s.config.OverrideIncomingToken) {
		s.log.Debugf("skipping validation, using override token")
	} else {
		_, err := s.inboxToken(c.token)
		if err != nil {
			s.log.Errorf("error while getting token %#v", err)
			c.ch.close()
			return
		}

		if err := s.deleteInboxToken(c.token); err != nil {
			s.log.Errorf("error deleting token %#v", err)
			c.ch.close()
			return
		}
	}

	if err := s.insertMailbox(c.ch.certDigest[:]); err != nil {
		s.log.Errorf("error while inserting mailbox %#v", err)
		c.ch.close()
		return
	}

	if err := c.ch.writef("INCO %x\n", c.token); err != nil {
		s.log.Errorf("error while responding to INCO %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleDeauthAllCommand(c *deauthAllCommand) {
	mailbox, err := s.mailbox(c.ch.certDigest[:])
	if err != nil {
		s.log.Errorf("error while getting token %#v", err)
		c.ch.close()
		return
	}

	if mailbox == nil {
		s.log.Debugf("mailbox doesn't exist for %x", c.ch.certDigest)
		c.ch.close()
		return
	}

	if err := s.runTx("deleting mailbox", func(tx *sqlx.Tx) error {
		if _, err := tx.Exec("delete from messages where key_digest = $1", c.ch.certDigest[:]); err != nil {
			return err
		}
		if _, err := tx.Exec("delete from send_tokens where digest = $1", c.ch.certDigest[:]); err != nil {
			return err
		}
		if _, err := tx.Exec("delete from ios_push_token where digest = $1", c.ch.certDigest[:]); err != nil {
			return err
		}
		if _, err := tx.Exec("delete from mailbox where digest = $1", c.ch.certDigest[:]); err != nil {
			return err
		}
		return c.ch.writef("DALL\n")
	}); err != nil {
		s.log.Errorf("error handling DALL message %#v", err)
		c.ch.close()
		return
	}
}

func (s *Server) handlePingCommand(c *pingCommand) {
	if err := c.ch.writef("PONG\n"); err != nil {
		s.log.Errorf("error while responding to PING %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleListCommand(c *listCommand) {
	tokens, err := s.sendTokens(c.ch.certDigest[:])
	if err != nil {
		s.log.Errorf("error while responding to LIST %#v", err)
		c.ch.close()
		return
	}

	if err := c.ch.writef("LIST %d\n", len(tokens)); err != nil {
		s.log.Errorf("error while responding to LIST %#v", err)
		c.ch.close()
		return
	}

	for _, t := range tokens {
		if err := c.ch.writef("%x %d %d %d\n", t.Token, t.NextSeq, t.StartTime, t.EndTime); err != nil {
			s.log.Errorf("error while responding to LIST %#v", err)
			c.ch.close()
			return
		}
	}
}

func (s *Server) handleTrimCommand(c *trimCommand) {
	num, err := s.trimMessages(c.ch.certDigest[:], c.seq)
	if err != nil {
		s.log.Errorf("error while trimming %#v", err)
		c.ch.close()
		return
	}
	if err := c.ch.writef("TRIM %x %d %d\n", c.token, c.seq, num); err != nil {
		s.log.Errorf("error while responding to TRIM %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleExtendCommand(c *extendCommand) {
	newTime, err := s.extendSendToken(c.ch.certDigest[:], c.token, c.additionalTime)
	if err != nil {
		s.log.Errorf("error while extending %#v", err)
		c.ch.close()
		return
	}
	if err := c.ch.writef("EXTD %x %d\n", c.token, newTime); err != nil {
		s.log.Errorf("error while responding to EXTD %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleAuthCommand(c *authCommand) {
	token := make([]byte, 32)
	if _, err := crypto_rand.Read(token); err != nil {
		s.log.Errorf("error reading crypto %#v", err)
		c.ch.close()
		return
	}

	if err := s.insertSendToken(c.ch.certDigest[:], token, c.startTime, c.endTime); err != nil {
		s.log.Errorf("error inserting send token %#v", err)
		c.ch.close()
		return
	}

	c.ch.subscribe(token)

	if err := c.ch.writef("AUTH %x %d %d\n", token, c.startTime, c.endTime); err != nil {
		s.log.Errorf("error writing back %#v", err)
		c.ch.close()
	}
}

func (s *Server) handleDeauthCommand(c *deauthCommand) {
	if err := s.deleteSendToken(c.ch.certDigest[:], c.token); err != nil {
		s.log.Errorf("error deleting send token %#v", err)
		c.ch.close()
		return
	}

	c.ch.unsubscribe(c.token)

	if err := c.ch.writef("DEAU %x\n", c.token); err != nil {
		s.log.Errorf("error writing back %#v", err)
		c.ch.close()
	}
}

func (s *Server) iosPushTokens(digest []byte) ([]*iosPushToken, error) {
	tokens := []*iosPushToken{}
	if err := s.db.Select(&tokens, "select * from ios_push_token where digest = $1 order by created_at, token", digest); err != nil {
		return nil, err
	}
	return tokens, nil
}

func (s *Server) sendTokens(digest []byte) ([]*sendToken, error) {
	tokens := []*sendToken{}
	if err := s.db.Select(&tokens, "select * from send_tokens where digest = $1 ORDER BY start_time, end_time, token", digest); err != nil {
		return nil, err
	}
	return tokens, nil
}

func (s *Server) mailbox(digest []byte) (*mailbox, error) {
	m := &mailbox{}
	if err := s.db.Get(m, "select * from mailbox where digest = $1", digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return m, nil
}

func (s *Server) inboxToken(token []byte) (*inboxToken, error) {
	i := &inboxToken{}
	if err := s.db.Get(i, "select * from inbox_tokens where token = $1", token); err != nil {
		return nil, err
	}
	return i, nil
}

func (s *Server) deleteInboxToken(token []byte) error {
	if _, err := s.db.Exec("DELETE FROM inbox_tokens where token = $1", token); err != nil {
		return err
	}
	return nil
}

func (s *Server) insertMailbox(digest []byte) error {
	if _, err := s.db.Exec("insert into mailbox (digest) VALUES ($1)", digest); err != nil {
		return err
	}
	return nil
}

func (s *Server) insertSendToken(digest []byte, token []byte, startTime, endTime uint64) error {
	if _, err := s.db.Exec("insert into send_tokens (digest, token, start_time, end_time) VALUES ($1, $2, $3, $4)", digest, token, startTime, endTime); err != nil {
		return err
	}
	return nil
}

func (s *Server) deleteSendToken(digest []byte, token []byte) error {
	if _, err := s.db.Exec("DELETE FROM send_tokens WHERE digest = $1 AND token = $2", digest, token); err != nil {
		return err
	}
	return nil
}

func (s *Server) trimMessages(digest []byte, seq uint64) (int64, error) {
	res, err := s.db.Exec("delete from messages where key_digest = $1 and seq <= $2", digest, seq)
	if err != nil {
		return 0, err
	}
	num, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (s *Server) extendSendToken(digest, token []byte, additionalTime uint64) (uint64, error) {
	var newTime uint64
	if err := s.db.QueryRowx(`
		UPDATE send_tokens SET end_time = end_time + $1
		WHERE digest = $2 AND token = $3
		RETURNING end_time`, additionalTime, digest, token).Scan(&newTime); err != nil {
		return 0, err
	}

	return newTime, nil
}

func (s *Server) runTx(label string, runner runnerFunc) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return err
	}

	if runErr := runner(tx); runErr != nil {
		s.log.Warnf("error while running %s %#v", label, runErr)
		if err := tx.Rollback(); err != nil {
			s.log.Errorf("error while rolling back %s %#v", label, err)
			return err
		}
		return runErr
	}
	if err := tx.Commit(); err != nil {
		s.log.Errorf("error while committing %s %#v", label, err)
		return err
	}
	return nil
}

func newLogger(config *Config) *zap.SugaredLogger {
	writer := &lumberjack.Logger{
		Filename:   config.LogPath,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   // days
		Compress:   true, // disabled by default
	}

	level := zapcore.InfoLevel
	if config.Debug {
		level = zapcore.DebugLevel
	}
	opts := []zap.Option{
		zap.Fields(zap.String("source", "server")),
	}

	de := zap.NewDevelopmentEncoderConfig()
	fileEncoder := zapcore.NewJSONEncoder(de)
	consoleEncoder := zapcore.NewConsoleEncoder(de)
	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, zapcore.AddSync(writer), level),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)
	logger := zap.New(core, opts...)
	sugar := logger.Sugar()
	return sugar
}
