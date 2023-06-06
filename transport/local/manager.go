package local

import (
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/migration"
	"go.uber.org/zap"
)

const (
	DigestScheme     = "id"
	serviceTypeProto = "_slick._tcp"
)

func NewURL(digest [32]byte) string {
	return fmt.Sprintf("%s:sha-256;%s", DigestScheme, base64.URLEncoding.EncodeToString(digest[:]))
}

func ParseURL(u string) (digest [32]byte, err error) {
	parsedURL, err := url.Parse(u)
	if err != nil {
		return
	}

	if parsedURL.Scheme != DigestScheme {
		err = fmt.Errorf("expected scheme %s, got %s", DigestScheme, parsedURL.Scheme)
		return
	}

	if !strings.HasPrefix(parsedURL.Opaque, "sha-256;") {
		err = fmt.Errorf("expected opaque to start with sha-256;, got %s", parsedURL.Opaque)
		return
	}

	data, err := base64.URLEncoding.DecodeString(parsedURL.Opaque[8:])
	if err != nil {
		return
	}
	if len(data) != 32 {
		err = fmt.Errorf("expected length 32, got %d", len(data))
		return
	}
	copy(digest[:], data)
	return
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

type localService struct {
	httpService     *http.Server
	zeroconfService *zeroconf.Server
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlivePeriod(3 * time.Minute); err != nil {
		return nil, err
	}
	return tc, nil
}

type MessageProcessor func([]*MessageImpl) error
type MessageImpl struct {
	to   string
	from string
	body []byte
}

func (m *MessageImpl) To() string {
	return m.to
}

func (m *MessageImpl) From() string {
	return m.from
}

func (m *MessageImpl) Body() []byte {
	return m.body
}

type transport struct {
	CertDigest []byte `db:"cert_digest"`
	PrivateDer []byte `db:"private_der"`
	PublicDer  []byte `db:"public_der"`
}

func (t *transport) URL() string {
	return NewURL([32]byte(t.CertDigest))
}

type report struct {
	groupID ids.ID
}

type Manager struct {
	config                   *config.Config
	db                       *db.Database
	log                      *zap.SugaredLogger
	servers                  map[string]*localService
	cancelFunc               context.CancelFunc
	finished                 sync.WaitGroup
	listenLock               sync.Mutex
	readyTransports          chan *transport
	transportMap             map[string]*transport
	processor                MessageProcessor
	assignmentChangeReporter func(ids.ID, string, bool) error
	reportedGroups           chan *report
}

func NewManager(config *config.Config, d *db.Database, processor MessageProcessor, assignmentChangeReporter func(ids.ID, string, bool) error) (*Manager, error) {
	log := config.Logger("transport/local/manager")

	if err := d.MigrateNoLock("_transport_local", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
	CREATE TABLE _local_transports (
		cert_digest BLOB PRIMARY KEY,
		private_der BLOB NOT NULL,
		public_der BLOB NOT NULL
	);
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
		servers:                  make(map[string]*localService),
		readyTransports:          make(chan *transport, 100),
		transportMap:             make(map[string]*transport),
		assignmentChangeReporter: assignmentChangeReporter,
		reportedGroups:           make(chan *report, 100),
	}

	return m, nil
}

func (m *Manager) ReportGroup(id ids.ID) error {
	m.reportedGroups <- &report{groupID: id}
	return nil
}

func (m *Manager) Start() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancelFunc = cancelFunc
	if err := m.db.Run("ensure a transport", func() error {
		locals, err := m.transports()
		if err != nil {
			return err
		}

		if len(locals) == 0 {
			_, err := m.createTransport()
			if err != nil {
				return err
			}
			return nil
		}
		for _, t := range locals {
			m.transportMap[t.URL()] = t
		}

		return nil
	}); err != nil {
		return err
	}

	if err := m.pumpTransports(); err != nil {
		return err
	}

	m.startTransportStarter(ctx)
	m.startGroupReporter(ctx)
	return nil
}

func (m *Manager) Send(to string, body []byte) error {
	m.log.Debugf("sending to %s of len %d", to, len(body))
	digest, err := ParseURL(to)
	if err != nil {
		return err
	}
	id := fmt.Sprintf("%x", digest[0:8])

	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return err
	}

	entries := make(chan *zeroconf.ServiceEntry)
	timeout := time.Duration(m.config.LookupTimeoutMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	complete := make(chan error)

	if err := resolver.Lookup(ctx, id, serviceTypeProto, "local.", entries); err != nil {
		return err
	}

	go func() {
		for entry := range entries {
			m.log.Debugf("got an entry %s", entry.Text[0])
			if entry.Text[0] == to {
				var err error
				for _, ip := range entry.AddrIPv4 {
					addr := fmt.Sprintf("https://%s:%d", ip.String(), entry.Port)
					if err = m.request(addr, body); err == nil {
						break
					}
				}
				if err == nil {
					complete <- nil
					return
				}
				for _, ip := range entry.AddrIPv6 {
					addr := fmt.Sprintf("https://[%s]:%d", ip.String(), entry.Port)
					if err = m.request(addr, body); err == nil {
						break
					}
				}
				complete <- err
			}
		}
	}()

	select {
	case <-ctx.Done():
		m.log.Debugf("timed out sending from to %s", to)
		return errors.New("timed out locating entry")
	case err := <-complete:
		if err != nil {
			m.log.Debugf("sending to %s got error %#v", to, err)
		} else {
			m.log.Debugf("sent to %s", to)
		}
		return err
	}
}

func (m *Manager) Scan(ctx context.Context) ([]string, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}

	entries := make(chan *zeroconf.ServiceEntry)
	complete := make(chan error)

	if err := resolver.Browse(ctx, serviceTypeProto, "local.", entries); err != nil {
		return nil, err
	}

	scanned := make([]string, 0)
	go func() {
		for entry := range entries {
			m.log.Debugf("got an entry while scanning %s", entry.Text[0])
			scanned = append(scanned, entry.Text[0])
		}
		m.log.Debugf("finished scanning")
		complete <- nil
	}()
	<-ctx.Done()
	return scanned, nil
}

func (m *Manager) TransportURLs() []string {
	urls := make([]string, 0, len(m.transportMap))
	for u := range m.transportMap {
		urls = append(urls, u)
	}
	return urls
}

func (m *Manager) createTransport() (*transport, error) {
	priv, err := rsa.GenerateKey(crypto_rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	publicDigest := sha256.Sum256(priv.Public().(*rsa.PublicKey).N.Bytes())
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.Unix()),
		NotBefore:             now,
		NotAfter:              now.AddDate(100, 0, 0), // Valid for one hundred years
		SubjectKeyId:          publicDigest[:],
		BasicConstraintsValid: true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	cert, err := x509.CreateCertificate(crypto_rand.Reader, template, template,
		priv.Public(), priv)
	if err != nil {
		return nil, err
	}

	privBytes := x509.MarshalPKCS1PrivateKey(priv)

	digest := sha256.Sum256(cert)
	t := &transport{
		CertDigest: digest[:],
		PublicDer:  cert,
		PrivateDer: privBytes,
	}

	m.db.AfterCommit(func() {
		m.readyTransports <- t
	})

	if err := m.insertTransport(t); err != nil {
		return nil, err
	}
	m.transportMap[t.URL()] = t
	return t, nil
}

func (m *Manager) transports() ([]*transport, error) {
	var ts []*transport
	if err := m.db.Tx.Select(&ts, "SELECT * FROM _local_transports"); err != nil {
		return nil, fmt.Errorf("messaging: error getting transports: %w", err)
	}
	return ts, nil
}

func (m *Manager) insertTransport(lt *transport) error {
	if _, err := m.db.Tx.NamedExec("INSERT INTO _local_transports (private_der, public_der, cert_digest) VALUES (:private_der, :public_der, :cert_digest)", lt); err != nil {
		return fmt.Errorf("messaging: error upserting local transport: %w", err)
	}
	return nil
}

func (m *Manager) createURL(d [32]byte) string {
	return fmt.Sprintf("%s:sha-256;%s", DigestScheme, base64.URLEncoding.EncodeToString(d[:]))
}

type HTTPHandler struct {
	m  *Manager
	lt *transport
}

func (h HTTPHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) { // create response binary data
	cert := req.TLS.PeerCertificates[0]

	body, err := io.ReadAll(req.Body)
	if err != nil {
		h.m.log.Warnf("error reading body: %#v", err)
		return
	}
	msg := &MessageImpl{from: h.m.createURL(sha256.Sum256(cert.Raw)), to: h.lt.URL(), body: body}
	if err := h.m.db.Run("process message", func() error {
		return h.m.processor([]*MessageImpl{msg})
	}); err != nil {
		h.m.log.Warnf("error while processing message %#v", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	res.WriteHeader(http.StatusOK)
}

func (m *Manager) listen(lt *transport) error {
	m.listenLock.Lock()
	defer m.listenLock.Unlock()

	if _, ok := m.servers[lt.URL()]; ok {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/", HTTPHandler{m, lt})

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 500 * time.Millisecond,
		ReadTimeout:       1 * time.Second,
		WriteTimeout:      1 * time.Second,
	}

	publicCert, err := x509.ParseCertificate(lt.PublicDer)
	if err != nil {
		return err
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(lt.PrivateDer)
	if err != nil {
		return err
	}
	var outCert tls.Certificate
	outCert.Certificate = append(outCert.Certificate, publicCert.Raw)
	outCert.PrivateKey = privateKey

	config := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAnyClientCert,
		CipherSuites: []uint16{tls.TLS_CHACHA20_POLY1305_SHA256},
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0] = outCert
	config.ClientAuth = tls.RequireAnyClientCert

	ln, err := net.Listen("tcp", ":0") // #nosec G102
	if err != nil {
		return err
	}

	port := ln.Addr().(*net.TCPAddr).Port
	tlsListener := tls.NewListener(tcpKeepAliveListener{ln.(*net.TCPListener)},
		config)

	meta := []string{
		lt.URL(),
	}

	serviceID := fmt.Sprintf("%x", lt.CertDigest[0:8])
	service, err := zeroconf.Register(
		serviceID,        // service instance name
		serviceTypeProto, // service type and protocl
		"local.",         // service domain
		port,             // service port
		meta,             // service metadata
		nil,              // register on all network interfaces
	)

	m.log.Debugf("registered %s on port %d, got err %#v", serviceID, port, err)

	if err != nil {
		return err
	}

	m.servers[lt.URL()] = &localService{server, service}

	go func() {
		for {
			if err := server.Serve(tlsListener); err != http.ErrServerClosed {
				m.log.Warnf("error serving %#v", err)
			}
			time.Sleep(5 * time.Second)
		}
	}()

	m.log.Debugf("starting a listener on port %d", port)
	return nil
}

func (m *Manager) Shutdown() error {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.finished.Wait()
	}

	c := context.Background()
	for _, s := range m.servers {
		s.zeroconfService.Shutdown()
		if err := s.httpService.Shutdown(c); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) request(to string, body []byte) error {
	var lt *transport
	for _, t := range m.transportMap {
		lt = t
	}
	if lt == nil {
		return fmt.Errorf("couldn't find local transport")
	}

	publicCert, err := x509.ParseCertificate(lt.PublicDer)
	if err != nil {
		return err
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(lt.PrivateDer)
	if err != nil {
		return err
	}
	var outCert tls.Certificate
	outCert.Certificate = append(outCert.Certificate, publicCert.Raw)
	outCert.PrivateKey = privateKey

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS13,
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{outCert},
				ClientAuth:         tls.RequireAnyClientCert,
				CipherSuites:       []uint16{tls.TLS_CHACHA20_POLY1305_SHA256},
			},
		},
	} // #nosec G402

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.config.RequestTimeoutMs)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", to, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-type", "application/x-slick")
	if _, err := client.Do(req); err != nil {
		return err
	}

	return nil
}

func (m *Manager) startTransportStarter(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case lt := <-m.readyTransports:
				if err := m.listen(lt); err != nil {
					m.log.Fatal("error starting transport", err)
				}
			}
		}

	}()
}

func (m *Manager) startGroupReporter(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		defer m.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case report := <-m.reportedGroups:
				for url := range m.transportMap {
					if err := m.assignmentChangeReporter(report.groupID, url, true); err != nil {
						m.log.Fatalf("error reporting assignment change %#v", err)
					}
				}
			}
		}

	}()
}

func (m *Manager) pumpTransports() error {
	return m.db.Run("pumping transports", func() error {
		ts, err := m.transports()
		if err != nil {
			return err
		}

		for _, t := range ts {
			m.readyTransports <- t
		}
		return nil
	})
}
