package transport

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/migration"
	"github.com/meow-io/go-slick/transport/heya"
	"github.com/meow-io/go-slick/transport/local"
	"go.uber.org/zap"
)

const preflightInterval = 15

type hostPort struct {
	host string
	port int
}

type Message interface {
	From() string
	Body() []byte
}

type Manager struct {
	db              *db.Database
	clock           clock.Clock
	config          *config.Config
	log             *zap.SugaredLogger
	local           *local.Manager
	heya            *heya.Manager
	finished        sync.WaitGroup
	cancelFunc      context.CancelFunc
	updates         chan interface{}
	preflightStatus map[string]time.Time
	preflightLock   sync.RWMutex
	statusUpdater   func(string, bool)
}

type Transport struct {
	URL      string
	ETTR     int
	Priority int
}

type MessageProcessor func([]Message) error
type AssignmentReporter func(ids.ID, string, bool) error

func NewManager(config *config.Config, d *db.Database, clock clock.Clock, processOnce bool, processor MessageProcessor, assignmentReporter AssignmentReporter) (*Manager, error) {
	log := config.Logger("transport/manager")

	if err := d.MigrateNoLock("_transport", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
	CREATE TABLE _ios_push_tokens (
		token STRING PRIMARY KEY
	);
						`)
				return err
			},
		},
	}); err != nil {
		return nil, err
	}

	localManager, err := local.NewManager(config, d, func(messageImpls []*local.MessageImpl) error {
		messages := make([]Message, len(messageImpls))
		for i, m := range messageImpls {
			messages[i] = Message(m)
		}
		return processor(messages)
	}, assignmentReporter)
	if err != nil {
		return nil, err
	}

	heyaManager, err := heya.NewManager(config, d, processOnce, func(messageImpls []*heya.MessageImpl) error {
		messages := make([]Message, len(messageImpls))
		for i, m := range messageImpls {
			messages[i] = Message(m)
		}
		return processor(messages)
	}, assignmentReporter)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		db:              d,
		clock:           clock,
		config:          config,
		log:             log,
		local:           localManager,
		heya:            heyaManager,
		finished:        sync.WaitGroup{},
		updates:         make(chan interface{}, 100),
		preflightStatus: make(map[string]time.Time),
	}

	return m, nil
}

func (m *Manager) Start() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancelFunc = cancelFunc

	if err := m.local.Start(); err != nil {
		return err
	}
	if err := m.heya.Start(); err != nil {
		return err
	}
	m.startUpdatePassing(ctx)
	m.startPreflightChecker(ctx)
	return nil
}

func (m *Manager) Updates() chan interface{} {
	return m.updates
}

func (m *Manager) WaitForPending() {
	m.heya.WaitForPending()
}

func (m *Manager) Preflight(urls []string) []bool {
	m.preflightLock.RLock()
	defer m.preflightLock.RUnlock()
	statuses := make([]bool, len(urls))
	for i := range statuses {
		url := urls[i]
		active, ok := m.preflightStatus[url]
		if !ok {
			m.preflightLock.RUnlock()
			m.preflightLock.Lock()
			// if local is present but there are multiple urls, assume its not currently working
			if len(urls) > 1 && strings.HasPrefix(url, fmt.Sprintf("%s:", local.DigestScheme)) {
				statuses[i] = false
				m.preflightStatus[url] = m.clock.Now()
			} else {
				statuses[i] = true
				m.preflightStatus[url] = m.clock.Now().Add(preflightInterval * 2 * time.Second)
			}
			m.preflightLock.Unlock()
			m.preflightLock.RLock()

		} else {
			statuses[i] = m.checkActive(active)
		}
	}
	return statuses
}

func (m *Manager) StatusChanged(f func(string, bool)) {
	m.statusUpdater = f
}

func (m *Manager) Shutdown() error {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.finished.Wait()
	}

	errors := make([]error, 0)
	if err := m.heya.Shutdown(); err != nil {
		errors = append(errors, err)
	}

	if err := m.local.Shutdown(); err != nil {
		errors = append(errors, err)
	}

	if len(errors) != 0 {
		return fmt.Errorf("errors encountered during shutdown: %#v", errors)
	}
	return nil
}

func (m *Manager) Send(from string, tos []string, body []byte) []error {
	errs := make([]error, len(tos))
	for i, to := range tos {
		u, err := url.Parse(to)
		if err != nil {
			errs[i] = err
			continue
		}
		switch u.Scheme {
		case local.DigestScheme:
			errs[i] = m.local.Send(to, body)
		case heya.HeyaScheme:
			errs[i] = m.heya.Send(from, to, body)
		}
	}
	return errs
}

func (m *Manager) URLsForGroup(id ids.ID) ([]string, error) {
	localURLs := m.local.TransportURLs()
	heyaURLs, err := m.heya.URLsForGroup(id)
	if err != nil {
		return nil, err
	}
	m.log.Infof("urls for group %x local=%#v heya=%#v", id, localURLs, heyaURLs)
	return append(localURLs, heyaURLs...), nil
}

func (m *Manager) ReportGroup(id ids.ID) error {
	if err := m.heya.ReportGroup(id); err != nil {
		return err
	}
	return m.local.ReportGroup((id))
}

func (m *Manager) RegisterHeyaTransport(authToken, host string, port int) error {
	if err := m.heya.CreateTransport(authToken, host, port); err != nil {
		return err
	}
	return m.reconcileIOSPushTokens()
}

func (m *Manager) AddPushNotificationToken(token string) error {
	if _, err := m.db.Tx.Exec("INSERT INTO _ios_push_tokens (token) VALUES ($1) ON CONFLICT(token) DO NOTHING", token); err != nil {
		return fmt.Errorf("messaging: error inserting local transport: %w", err)
	}
	return m.reconcileIOSPushTokens()
}

func (m *Manager) DeletePushNotificationToken(token string) error {
	if _, err := m.db.Tx.Exec("DELETE FROM _ios_push_tokens (token) VALUES ($1) ON CONFLICT(token) DO NOTHING", token); err != nil {
		return fmt.Errorf("messaging: error inserting local transport: %w", err)
	}
	return m.reconcileIOSPushTokens()
}

func (m *Manager) reconcileIOSPushTokens() error {
	var tokens []string
	if err := m.db.Tx.Select(&tokens, "select token from _ios_push_tokens"); err != nil {
		return fmt.Errorf("error getting push tokens: %w", err)
	}

	return m.heya.SetIOSPushTokens(tokens)
}

func (m *Manager) startUpdatePassing(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case update := <-m.heya.Updates():
				m.updates <- update
			}
		}
	}()
}

func (m *Manager) startPreflightChecker(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		defer m.finished.Done()
		for {
			m.log.Debugf("performing preflight updates")
			start := m.clock.Now()

			errors := make(chan error)
			reqCtx, cancelFn := context.WithDeadline(ctx, start.Add((preflightInterval-1)*time.Second))

			m.performPreflightUpdates(reqCtx, errors)

			for i := 0; i != 2; i++ {
				if err := <-errors; err != nil {
					m.log.Debugf("error in preflight loop %#v", err)
				}
			}

			// collect all the host:port, heyas
			// wait until whatev
			cancelFn()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(start.Add(preflightInterval * time.Second))):
				// do nothing
			}
		}
	}()
}

func (m *Manager) performPreflightUpdates(ctx context.Context, errors chan error) {
	go func() {
		m.log.Debugf("scanning")
		entries, err := m.local.Scan(ctx)
		if err != nil {
			m.log.Debugf("scanning err %#v", err)
			errors <- err
		}
		for i := range entries {
			e := entries[i]
			m.preflightLock.Lock()
			if _, ok := m.preflightStatus[e]; ok {
				m.log.Debugf("preflight scanned %s", e)
				wasActive := m.checkActive(m.preflightStatus[e])
				m.preflightStatus[e] = m.clock.Now().Add(preflightInterval * time.Second)
				if m.statusUpdater != nil && !wasActive {
					go m.statusUpdater(e, true)
				}
			}
			m.preflightLock.Unlock()
		}
		errors <- nil
	}()
	go func() {
		heyaHostPorts := make(map[hostPort][]string)
		m.preflightLock.RLock()
		for h := range m.preflightStatus {
			p, err := url.Parse(h)
			if err != nil {
				m.log.Debugf("error parsing %s %#v", h, err)
				continue
			}
			if p.Scheme != heya.HeyaScheme {
				continue
			}
			port := p.Port()
			var portInt int
			if port == "" {
				portInt = heya.DefaultPort
			} else {
				pi, err := strconv.ParseInt(port, 10, 64)
				if err != nil {
					m.log.Debugf("error parsing %s %#v", h, err)
					continue
				}
				portInt = int(pi)
			}
			hp := hostPort{p.Hostname(), portInt}
			heyaHostPorts[hp] = append(heyaHostPorts[hp], h)
		}
		m.preflightLock.RUnlock()

		for h, entries := range heyaHostPorts {
			if err := m.heya.Ping(ctx, h.host, h.port); err != nil {
				m.log.Debugf("error while pinging %#v %#v", h, err)
			} else {
				m.preflightLock.Lock()
				for i := range entries {
					e := entries[i]
					wasActive := m.checkActive(m.preflightStatus[e])
					m.preflightStatus[e] = m.clock.Now().Add(preflightInterval * time.Second)
					if m.statusUpdater != nil && !wasActive {
						go m.statusUpdater(e, true)
					}
				}
				m.preflightLock.Unlock()
			}
		}
		errors <- nil
	}()
}

func (m *Manager) checkActive(t time.Time) bool {
	return m.clock.Now().Before(t)
}
