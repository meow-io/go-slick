// Defines package responsible for performing the actual sending and receiving of messages to URLs.
package heya

import (
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kevinburke/nacl"
	"github.com/kevinburke/nacl/box"
	"github.com/kevinburke/nacl/scalarmult"
	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/crypto"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/migration"
	heya_client "github.com/meow-io/heya/client"
	"go.uber.org/zap"
)

const (
	StateNew        = 0
	StateUnassigned = 1
	StateAssigned   = 2

	HeyaScheme  = "heya"
	DefaultPort = heya_client.DefaultPort
)

type StateUpdate struct {
	Host  string
	Port  int
	State string
}

type ParsedURL struct {
	Host        string
	Port        int
	PublicBytes [32]byte
	SendToken   [32]byte
}

func (pu *ParsedURL) URL() string {
	return fmt.Sprintf("heya://%s:%d/%s/%s",
		pu.Host,
		pu.Port,
		base64.RawURLEncoding.EncodeToString(pu.PublicBytes[:]),
		base64.RawURLEncoding.EncodeToString(pu.SendToken[:]))
}

func ParseURL(u string) (*ParsedURL, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	if pu.Scheme != HeyaScheme {
		err = fmt.Errorf("expected scheme %s, got %s", HeyaScheme, pu.Scheme)
		return nil, err
	}

	parts := strings.Split(pu.Path, "/")

	publicKeyBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}

	sendTokenBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}

	if len(publicKeyBytes) != 32 {
		err = fmt.Errorf("expected length 32, got %d", len(publicKeyBytes))
		return nil, err
	}
	if len(sendTokenBytes) != 32 {
		err = fmt.Errorf("expected length 32, got %d", len(sendTokenBytes))
		return nil, err
	}

	var publicBytes [32]byte
	var sendToken [32]byte
	var port int
	copy(publicBytes[:], publicKeyBytes)
	copy(sendToken[:], sendTokenBytes)

	if pu.Port() == "" {
		port = heya_client.DefaultPort
	} else {
		portUint, err := strconv.ParseUint(pu.Port(), 10, 64)
		if err != nil {
			return nil, err
		}
		port = int(portUint)
	}

	return &ParsedURL{pu.Hostname(), port, publicBytes, sendToken}, err
}

type MessageImpl struct {
	from string
	body []byte
}

func (m *MessageImpl) From() string {
	return m.from
}

func (m *MessageImpl) Body() []byte {
	return m.body
}

type envelope struct {
	PublicKey [32]byte `bencode:"pk"`
	Body      []byte   `bencode:"b"`
}

type interiorMessage struct {
	From string `bencode:"f"`
	Body []byte `bencode:"b"`
}

type transport struct {
	ID              []byte `db:"id"`
	PrivateKeyPKCS1 []byte `db:"private_key_pkcs1"`
	Certificate     []byte `db:"certificate"`
	Host            string `db:"host"`
	Port            int    `db:"port"`

	incoming chan *MessageImpl
	client   *heya_client.Client
}

func (t *transport) hostPort() string {
	return fmt.Sprintf("%s:%d", t.Host, t.Port)
}

type sendToken struct {
	ID             []byte `db:"id"`
	TransportID    []byte `db:"transport_id"`
	State          int    `db:"state"`
	ExpiresAt      uint64 `db:"expires_at"`
	SendToken      []byte `db:"send_token"`
	PrivateKeyNacl []byte `db:"private_key_nacl"`
	NextSeq        uint64 `db:"next_seq"`
	GroupID        []byte `db:"group_id"`

	transport *transport
}

func (s *sendToken) url() string {
	return fmt.Sprintf("heya://%s:%d/%s/%s",
		s.transport.Host,
		s.transport.Port,
		base64.RawURLEncoding.EncodeToString(s.publicKeyNacl()),
		base64.RawURLEncoding.EncodeToString(s.SendToken))
}

func (s *sendToken) publicKeyNacl() []byte {
	var privateKey [32]byte
	copy(privateKey[:], s.PrivateKeyNacl)
	return scalarmult.Base(&privateKey)[:]
}

func (s *sendToken) privateKeyNacl() nacl.Key {
	var privateKey [32]byte
	copy(privateKey[:], s.PrivateKeyNacl)
	return &privateKey
}

func (t *transport) shutdown() {
	if t.client == nil {
		return
	}
	t.client.CloseWithoutReconnect()
}

type MessageProcessor func([]*MessageImpl) error
type Manager struct {
	config                   *config.Config
	db                       *db.Database
	log                      *zap.SugaredLogger
	processor                MessageProcessor
	cancelFunc               context.CancelFunc
	finished                 sync.WaitGroup
	processedIntro           sync.WaitGroup
	readyTransports          chan *transport
	transportMap             map[ids.ID]*transport
	transportLock            sync.RWMutex
	updates                  chan interface{}
	processSendTokens        chan bool
	assignmentChangeReporter func(ids.ID, string, bool) error
	sendTokens               map[[32]byte]*sendToken
	sendTokensLock           sync.RWMutex
	processOnce              bool
}

func NewManager(config *config.Config, d *db.Database, processOnce bool, processor MessageProcessor, assignmentChangeReporter func(ids.ID, string, bool) error) (*Manager, error) {
	log := config.Logger("transport/heya/manager")

	if err := d.MigrateNoLock("_transport_heya", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
	CREATE TABLE _heya_transports (
		id BLOB PRIMARY KEY,
		private_key_pkcs1 BLOB NOT NULL,
		certificate BLOB NOT NULL,
		host STRING NOT NULL,
		port INTEGER NOT NULL
	);

	CREATE TABLE _heya_send_tokens (
		id BLOB PRIMARY KEY,
		transport_id BLOB NOT NULL,
		group_id BLOB NOT NULL,
		state INTEGER,
		expires_at INTEGER,
		send_token BLOB NOT NULL,
		private_key_nacl BLOB NOT NULL,
		next_seq INTEGER,
		FOREIGN KEY(transport_id) REFERENCES _heya_transports(id) ON DELETE CASCADE
	);

	CREATE UNIQUE INDEX send_token_token on _heya_send_tokens (send_token);

	`)
				return err
			},
		},
	}); err != nil {
		return nil, err
	}

	m := &Manager{
		config:                   config,
		db:                       d,
		log:                      log,
		processor:                processor,
		readyTransports:          make(chan *transport, 100),
		transportMap:             make(map[ids.ID]*transport),
		updates:                  make(chan interface{}, 100),
		processSendTokens:        make(chan bool, 100),
		assignmentChangeReporter: assignmentChangeReporter,
		sendTokens:               make(map[[32]byte]*sendToken),
		processOnce:              processOnce,
	}

	// load transports here, and create clients
	return m, nil
}

func (m *Manager) Start() error {
	m.transportLock.Lock()
	defer m.transportLock.Unlock()
	m.sendTokensLock.Lock()
	defer m.sendTokensLock.Unlock()
	if err := m.db.Run("load heya transports & send tokens", func() error {
		transports, err := m.transports()
		if err != nil {
			return err
		}
		for i := range transports {
			t := transports[i]
			t.incoming = make(chan *MessageImpl, 100)

			conf := &heya_client.Config{
				Host:            t.Host,
				Port:            t.Port,
				Reconnect:       true,
				Ping:            true,
				NewState:        m.stateUpdater(t.Host, t.Port),
				Debug:           false,
				PrivateKeyPKCS1: t.PrivateKeyPKCS1,
				Cert:            t.Certificate,
			}

			client, err := heya_client.NewClientFromKey(conf)
			if err != nil {
				return err
			}
			t.client = client
			m.transportMap[ids.ID(t.ID)] = t
			m.db.AfterCommit(func() {
				m.readyTransports <- t
			})
		}

		sendTokens, err := m.allSendTokens()
		if err != nil {
			return err
		}
		for _, s := range sendTokens {
			t, ok := m.transportMap[ids.ID(s.TransportID)]
			if !ok {
				if err := m.deleteSendToken(s.ID); err != nil {
					return err
				}
				continue
			}
			s.transport = t
			m.sendTokens[[32]byte(s.SendToken)] = s
		}

		return nil
	}); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancelFunc = cancelFunc
	m.startTransportStarter(ctx)
	m.startSendTokenProcessor(ctx)

	return nil
}

func (m *Manager) Updates() chan interface{} {
	return m.updates
}

func (m *Manager) URLsForGroup(id ids.ID) ([]string, error) {
	addedRows := false
	m.transportLock.RLock()
	defer m.transportLock.RUnlock()
	urls := []string{}
	for _, t := range m.transportMap {
		tokens, err := m.sendTokensForGroup(t.ID, id[:])
		if err != nil {
			return nil, err
		}
		if len(tokens) != 0 {
			for _, t := range tokens {
				urls = append(urls, t.url())
			}
			continue
		}
		unassignedTokens, err := m.unassignedSendToken(t.ID)
		if err != nil {
			return nil, err
		}
		if len(unassignedTokens) != 0 {
			token := unassignedTokens[0]
			token.State = StateAssigned
			token.GroupID = id[:]
			if err := m.upsertSendToken(token); err != nil {
				return nil, err
			}
			token.transport = t
			m.db.AfterCommit(func() {
				if err := m.reportAssignmentChange(id, token.url(), true); err != nil {
					m.log.Fatalf("error while reporting assignment change: %#v", err)
				}
			})
			urls = append(urls, token.url())
			continue
		}

		key := nacl.NewKey()

		id := ids.NewID()
		token := &sendToken{
			ID:             id[:],
			TransportID:    t.ID,
			State:          StateNew,
			SendToken:      []byte{},
			PrivateKeyNacl: key[:],
			NextSeq:        0,
			GroupID:        id[:],
			transport:      t,
			ExpiresAt:      0,
		}
		if err := m.upsertSendToken(token); err != nil {
			return nil, err
		}
		addedRows = true
	}

	if addedRows {
		m.db.AfterCommit(func() {
			m.processSendTokens <- true
		})
	}
	return urls, nil
}

func (m *Manager) ReportGroup(id ids.ID) error {
	_, err := m.URLsForGroup(id)
	return err
}

func (m *Manager) CreateTransport(authToken, host string, port int) error {
	conf := &heya_client.Config{
		Host:      host,
		Port:      port,
		Reconnect: true,
		Ping:      true,
		NewState:  m.stateUpdater(host, port),
		Debug:     false,
	}
	client, err := heya_client.NewClient(conf)
	if err != nil {
		return err
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()
	if err := client.Connect(ctx); err != nil {
		return err
	}

	if _, err := client.RegisterIncoming(ctx, authToken); err != nil {
		return err
	}

	id := ids.NewID()
	t := &transport{
		ID:              id[:],
		PrivateKeyPKCS1: client.PrivateKeyPKCS1,
		Certificate:     client.Certificate,
		Host:            host,
		Port:            port,
		incoming:        make(chan *MessageImpl, 100),
		client:          client,
	}

	// create private key

	m.db.AfterCommit(func() {
		m.readyTransports <- t
	})

	if err := m.insertTransport(t); err != nil {
		return err
	}
	m.transportLock.Lock()
	m.transportMap[id] = t
	m.transportLock.Unlock()
	return nil
}

func (m *Manager) Shutdown() error {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.finished.Wait()
	}
	m.transportLock.Lock()
	defer m.transportLock.Unlock()
	for _, t := range m.transportMap {
		t.shutdown()
	}
	return nil
}

func (m *Manager) Send(from, to string, body []byte) error {
	m.log.Debugf("sending to %s from %s of len %d", to, from, len(body))
	// parse the url
	parsed, err := ParseURL(to)
	if err != nil {
		m.log.Warnf("sending message, but didn't parse %#v", err)
		return err
	}

	// make the envelope
	im := &interiorMessage{From: from, Body: body}
	interiorBytes, err := bencode.Serialize(im)
	if err != nil {
		return err
	}
	publicKey, privateKey, err := box.GenerateKey(crypto_rand.Reader)
	if err != nil {
		return err
	}
	encryptedInteriorBytes, err := crypto.EncryptWithDH(parsed.PublicBytes[:], privateKey[:], interiorBytes, nil)
	if err != nil {
		return err
	}
	env := &envelope{*publicKey, encryptedInteriorBytes}
	envBytes, err := bencode.Serialize(env)
	if err != nil {
		return err
	}

	hostPort := fmt.Sprintf("%s:%d", parsed.Host, parsed.Port)
	t := m.transportForHostPort(hostPort)
	if t == nil {
		return fmt.Errorf("cannot find transport for %s", hostPort)
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()
	return t.client.Send(ctx, parsed.SendToken[:], envBytes)
}

func (m *Manager) Ping(_ context.Context, host string, port int) error {
	hostPort := fmt.Sprintf("%s:%d", host, port)
	t := m.transportForHostPort(hostPort)
	if t == nil {
		return fmt.Errorf("couldn't find transport for %s", hostPort)
	}
	if t.client.State != heya_client.Open {
		return fmt.Errorf("not open, currently %d", t.client.State)
	}
	return nil
}

func (m *Manager) WaitForPending() {
	m.processedIntro.Wait()
}

func (m *Manager) SetIOSPushTokens(tokens []string) error {
	m.log.Debugf("setting push tokens to %#v", tokens)
	m.transportLock.RLock()
	defer m.transportLock.RUnlock()
	for _, t := range m.transportMap {
		// TODO this should skip transports not being used for incoming
		tokenMap := make(map[string]bool)
		for _, t := range tokens {
			tokenMap[t] = true
		}
		ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
		defer cancelFn()
		pushTokens, err := t.client.ListIOSPushTokens(ctx)
		if err != nil {
			return err
		}
		for _, existingPushToken := range pushTokens {
			if _, ok := tokenMap[existingPushToken.Value]; ok {
				delete(tokenMap, existingPushToken.Value)
			} else {
				m.log.Debugf("removing token %s", existingPushToken)
				if err := t.client.DeleteIOSPushToken(ctx, existingPushToken.Value); err != nil {
					return err
				}
			}
		}
		for token := range tokenMap {
			m.log.Debugf("adding token %s", token)
			if err := t.client.AddIOSPushToken(ctx, token); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) reportAssignmentChange(groupID ids.ID, url string, added bool) error {
	return m.assignmentChangeReporter(groupID, url, added)
}

func (m *Manager) transports() ([]*transport, error) {
	var ts []*transport
	if err := m.db.Tx.Select(&ts, "SELECT * FROM _heya_transports"); err != nil {
		return nil, fmt.Errorf("messaging: error getting transports: %w", err)
	}
	return ts, nil
}

func (m *Manager) insertTransport(lt *transport) error {
	if _, err := m.db.Tx.NamedExec("INSERT INTO _heya_transports (id, private_key_pkcs1, certificate, host, port) VALUES (:id, :private_key_pkcs1, :certificate, :host, :port)", lt); err != nil {
		return fmt.Errorf("messaging: error upserting local transport: %w", err)
	}
	return nil
}

func (m *Manager) sendTokensForGroup(transportID, groupID []byte) ([]*sendToken, error) {
	var st []*sendToken
	if err := m.db.Tx.Select(&st, "SELECT * FROM _heya_send_tokens where transport_id = $1 AND group_id = $2", transportID, groupID); err != nil {
		return nil, fmt.Errorf("messaging: error getting send tokens: %w", err)
	}
	return st, nil
}

func (m *Manager) allSendTokens() ([]*sendToken, error) {
	var st []*sendToken
	if err := m.db.Tx.Select(&st, "SELECT * FROM _heya_send_tokens"); err != nil {
		return nil, fmt.Errorf("messaging: error getting send tokens: %w", err)
	}
	return st, nil
}

func (m *Manager) deleteSendToken(id []byte) error {
	if _, err := m.db.Tx.Exec("DELETE FROM _heya_send_tokens where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting send token: %w", err)
	}
	return nil
}

func (m *Manager) sendTokensState(state int) ([]*sendToken, error) {
	var st []*sendToken
	if err := m.db.Tx.Select(&st, "SELECT * FROM _heya_send_tokens where state = $1", state); err != nil {
		return nil, fmt.Errorf("messaging: error getting send tokens: %w", err)
	}
	return st, nil
}

func (m *Manager) unassignedSendToken(transportID []byte) ([]*sendToken, error) {
	var st []*sendToken
	if err := m.db.Tx.Select(&st, "SELECT * FROM _heya_send_tokens where transport_id = $1 AND state = $2", transportID, StateUnassigned); err != nil {
		return nil, fmt.Errorf("messaging: error getting send tokens: %w", err)
	}
	return st, nil
}

func (m *Manager) upsertSendToken(st *sendToken) error {
	if _, err := m.db.Tx.NamedExec("INSERT INTO _heya_send_tokens (id, transport_id, state, send_token, private_key_nacl, next_seq, group_id, expires_at) VALUES (:id, :transport_id, :state, :send_token, :private_key_nacl, :next_seq, :group_id, :expires_at) ON CONFLICT(id) DO UPDATE SET transport_id = :transport_id, state = :state, send_token = :send_token, private_key_nacl = :private_key_nacl, next_seq = :next_seq, group_id = :group_id, expires_at = :expires_at", st); err != nil {
		return fmt.Errorf("messaging: error upserting send token: %w", err)
	}
	if len(st.SendToken) != 0 {
		m.sendTokensLock.Lock()
		if _, ok := m.sendTokens[[32]byte(st.SendToken)]; !ok {
			m.sendTokens[[32]byte(st.SendToken)] = st
		}
		m.sendTokensLock.Unlock()
	}
	return nil
}

func (m *Manager) unassignedSendTokenCount(transportID []byte) (int64, error) {
	var c int64
	if err := m.db.Tx.Get(&c, "SELECT count(*) FROM _heya_send_tokens where transport_id = $1 AND state = $2", transportID, StateUnassigned); err != nil {
		return 0, fmt.Errorf("messaging: error getting send tokens: %w", err)
	}
	return c, nil
}

func (m *Manager) transportForHostPort(hostPort string) *transport {
	m.transportLock.RLock()
	defer m.transportLock.RUnlock()
	for _, transport := range m.transportMap {
		if transport.hostPort() == hostPort {
			return transport
		}
	}
	return nil
}

func (m *Manager) startSendTokenProcessor(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		defer m.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.processSendTokens:
				processAgain := false
				// get new tokens and get a real token for them, then write it back into the table
				var sendTokens []*sendToken
				if err := m.db.Run("get new tokens", func() error {
					var err error
					sendTokens, err = m.sendTokensState(StateNew)
					return err
				}); err != nil {
					m.log.Warnf("error getting token %#v", err)
					processAgain = true
				}
				for _, st := range sendTokens {
					m.transportLock.RLock()
					t, ok := m.transportMap[ids.ID(st.TransportID)]
					m.transportLock.RUnlock()
					if !ok {
						m.log.Fatalf("couldn't find transport for id %x", st.TransportID)
					}
					startTime := time.Now()
					endTime := time.Now().Add(time.Hour * 24 * 365)
					reqCtx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)

					token, err := t.client.MakeSendToken(reqCtx, startTime, endTime)
					cancelFn()
					if err != nil {
						m.log.Warnf("error making send token %#v", err)
						processAgain = true
					}
					st.SendToken = token
					st.State = StateUnassigned
					st.ExpiresAt = uint64(endTime.Unix())
					if err := m.db.Run("upsert new tokens", func() error {
						return m.upsertSendToken(st)
					}); err != nil {
						m.log.Fatalf("error processing send token %#v", err)
					}
				}

				// get unassigned tokens with a group id and assign them
				if err := m.db.Run("get unassigned tokens", func() error {
					var err error
					sendTokens, err = m.sendTokensState(StateUnassigned)
					return err
				}); err != nil {
					m.log.Fatalf("error processing send token %#v", err)
				}
				for _, st := range sendTokens {
					if len(st.GroupID) == 0 {
						continue
					}

					m.transportLock.RLock()
					t, ok := m.transportMap[ids.ID(st.TransportID)]
					m.transportLock.RUnlock()
					if !ok {
						m.log.Fatalf("couldn't find transport for id %x", st.TransportID)
					}

					st.State = StateAssigned
					st.transport = t
					if err := m.db.Run("upsert new tokens", func() error {
						func(groupID ids.ID, url string) {
							m.db.AfterCommit(func() {
								if err := m.reportAssignmentChange(groupID, url, true); err != nil {
									m.log.Fatalf("error while reporting assignment change: %#v", err)
								}
							})
						}(ids.ID(st.GroupID), st.url())
						return m.upsertSendToken(st)
					}); err != nil {
						m.log.Fatalf("error processing send token %#v", err)
					}
				}

				m.transportLock.RLock()
				transportIDs := make([]ids.ID, 0, len(m.transportMap))
				for id := range m.transportMap {
					transportIDs = append(transportIDs, id)
				}
				m.transportLock.RUnlock()
				unassignedTokens := make(map[ids.ID]int64)

				// get token counts needed
				if err := m.db.Run("count unassigned send tokens", func() error {
					var err error
					for _, id := range transportIDs {
						count, err := m.unassignedSendTokenCount(id[:])
						if err != nil {
							return err
						}
						if count < 5 {
							unassignedTokens[id] = 5 - count
						}
					}
					sendTokens, err = m.sendTokensState(StateUnassigned)
					return err
				}); err != nil {
					m.log.Fatalf("error processing send token %#v", err)
				}

				for transportID, maxTokens := range unassignedTokens {
					m.transportLock.RLock()
					t := m.transportMap[transportID]
					m.transportLock.RUnlock()
					for i := 0; i != int(maxTokens); i++ {
						startTime := time.Now()
						endTime := time.Now().Add(time.Hour * 24 * 365)
						reqCtx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)

						token, err := t.client.MakeSendToken(reqCtx, startTime, endTime)
						cancelFn()
						if err != nil {
							m.log.Warnf("error getting send token %#v", err)
							processAgain = true
						}
						stID := ids.NewID()
						key := nacl.NewKey()
						st := &sendToken{
							ID:             stID[:],
							TransportID:    transportID[:],
							State:          StateUnassigned,
							ExpiresAt:      uint64(endTime.Unix()),
							SendToken:      token,
							PrivateKeyNacl: key[:],
							NextSeq:        0,
							GroupID:        []byte{},
							transport:      t,
						}
						if err := m.db.Run("upsert new tokens", func() error {
							return m.upsertSendToken(st)
						}); err != nil {
							m.log.Fatalf("error processing send token %#v", err)
						}
					}
				}

				if err := m.db.Run("reconcile send tokens", func() error {
					return m.reconcileSendTokens()
				}); err != nil {
					m.log.Fatalf("error while reconciling send tokens: %#v", err)
				}

				if processAgain {
					go func() {
						time.Sleep(15 * time.Second)
						m.processSendTokens <- true
					}()
				}

			}
		}
	}()
}

func (m *Manager) startTransportStarter(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		defer m.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-m.readyTransports:
				m.processedIntro.Add(1)
				m.finished.Add(1)
				go func() {
					defer m.finished.Done()
					m.log.Debugf("got a ready transport for %s", t.hostPort())
					reqCtx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
					defer cancelFn()
					if err := t.client.Connect(reqCtx); err != nil {
						m.log.Warnf("error while connecting from client %s: %#v", t.hostPort(), err)
					}
					m.processSendTokens <- true
					for {
						select {
						case <-ctx.Done():
							m.log.Debugf("done receiving notifications %s", t.hostPort())
							return
						case notification := <-t.client.Notifications():
							switch v := notification.(type) {
							case *heya_client.Notification:
								m.sendTokensLock.RLock()
								st, ok := m.sendTokens[[32]byte(v.Token)]
								m.sendTokensLock.RUnlock()
								if !ok {
									continue
								}
								if v.Seq < st.NextSeq {
									continue
								}

								startSeq := st.NextSeq

								for startSeq < v.Seq {
									messages := []*MessageImpl{}
									m.log.Debugf("getting messages from %d to %d", startSeq, v.Seq)
									for i := startSeq; i < v.Seq; i++ {
										m.log.Debugf("getting message %d", i)
										reqCtx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
										defer cancelFn()
										message, err := t.client.Want(reqCtx, st.SendToken, i)
										if err != nil {
											m.log.Fatalf("want command, %#v", err)
											continue
										}

										startSeq = i + 1

										if message == nil {
											m.log.Debugf("unable to get message %d", i)
											continue
										}

										env := &envelope{}
										if err := bencode.Deserialize(message.Body, env); err != nil {
											m.log.Warnf("unable to deserialize message: %#v", err)
											continue
										}

										interiorBytes, err := crypto.DecryptWithDH(env.PublicKey[:], st.privateKeyNacl()[:], env.Body, nil)
										if err != nil {
											m.log.Warnf("unable to decrypt message: %#v", err)
											continue
										}

										interior := &interiorMessage{}
										if err := bencode.Deserialize(interiorBytes, interior); err != nil {
											m.log.Warnf("unable to deserialize message: %#v", err)
											continue
										}
										messages = append(messages, &MessageImpl{from: interior.From, body: interior.Body})
										if len(messages) == 100 {
											break
										}
									}
									if err := m.db.Run("update send_token with new seq", func() error {
										m.finished.Add(1)
										defer m.finished.Done()
										if err := m.processor(messages); err != nil {
											return err
										}
										st.NextSeq = startSeq - 1
										return m.upsertSendToken(st)
									}); err != nil {
										m.log.Fatal(err)
									}
									reqCtx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
									defer cancelFn()
									_, err := t.client.Trim(reqCtx, st.SendToken, st.NextSeq)
									if err != nil {
										m.log.Debugf("error while running TRIM %#v", err)
									}
								}
							case *heya_client.DoneIntro:
								if m.processOnce {
									m.processedIntro.Done()
									return
								}
							}
						}
					}
				}()
			}
		}
	}()
}

func (m *Manager) stateUpdater(host string, port int) func(int) {
	return func(state int) {
		var s string
		switch state {
		case heya_client.Closed:
			s = "closed"
		case heya_client.Closing:
			s = "closing"
		case heya_client.Open:
			s = "open"
		case heya_client.Reconnecting:
			s = "reconnecting"
		}
		m.updates <- &StateUpdate{host, port, s}
	}
}

func (m *Manager) reconcileSendTokens() error {
	// two way, if a token is here and not in the transport, delete it
	// if a token is in the transport and not here, delete it from the transport

	m.transportLock.RLock()
	transports := make([]*transport, 0, len(m.transportMap))
	for _, t := range m.transportMap {
		transports = append(transports, t)
	}
	m.transportLock.RUnlock()

	for _, t := range transports {
		ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)
		tokens, err := t.client.ListTokens(ctx)
		cancelFn()
		if err != nil {
			m.log.Warnf("error getting tokens %#v", err)
			continue
		}

		tokenMap := make(map[[32]byte]bool)
		for _, token := range tokens {
			tokenMap[[32]byte(token.Value)] = true
			m.sendTokensLock.RLock()
			localToken, ok := m.sendTokens[[32]byte(token.Value)]
			m.sendTokensLock.RUnlock()
			if !ok {
				ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)
				err := t.client.RevokeSendToken(ctx, token.Value)
				cancelFn()
				if err != nil {
					m.log.Warnf("error revoking token %#v", err)
				}
				continue
			}

			// extend token
			if time.Until(time.Unix(int64(localToken.ExpiresAt), 0)) > time.Hour*24*30*6 {
				continue
			}

			ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)
			newExpires, err := t.client.ExtendSendToken(ctx, token.Value, 60*24*30*6)
			cancelFn()
			if err != nil {
				m.log.Warnf("error extending token %#v", err)
				continue
			}
			localToken.ExpiresAt = uint64(newExpires.Unix())
			if err := m.db.Run("delete send tokens", func() error {
				return m.upsertSendToken(localToken)
			}); err != nil {
				m.log.Warnf("error upserting token %#v", err)
				continue
			}
		}

		tokenDeleteIDs := make([][]byte, 0)
		m.sendTokensLock.RLock()
		for _, token := range m.sendTokens {
			if bytes.Equal(token.TransportID, t.ID) && !tokenMap[[32]byte(token.SendToken)] {
				tokenDeleteIDs = append(tokenDeleteIDs, token.SendToken)
			}
		}
		m.sendTokensLock.RUnlock()
		if len(tokenDeleteIDs) != 0 {
			if err := m.db.Run("delete send tokens", func() error {
				for _, id := range tokenDeleteIDs {
					if err := m.deleteSendToken(id); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				m.log.Warnf("error deleting send tokens: %#v", err)
			}
		}
	}
	return nil
}
