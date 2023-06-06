// Handles routing application messages to the correct database handler. At present, only one
// handler exists, that is, for "eav".
package data

import (
	"context"
	"fmt"
	"sync"

	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/data/eav"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"go.uber.org/zap"
)

const EAVDatabase = "eav"

type Manager struct {
	EAV *eav.EAV

	log        *zap.SugaredLogger
	config     *config.Config
	db         *db.Database
	finished   sync.WaitGroup
	cancelFunc context.CancelFunc
	updates    chan interface{}
	eavUpdates chan interface{}
}

func NewManager(c *config.Config, d *db.Database, clock clock.Clock) (*Manager, error) {
	log := c.Logger("transport/local/manager")

	eavUpdates := make(chan interface{}, 100)

	eavDB, err := eav.NewEAV(c, d, clock, eavUpdates)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		EAV: eavDB,

		cancelFunc: nil,
		log:        log,
		config:     c,
		db:         d,
		updates:    make(chan interface{}, 100),
		eavUpdates: eavUpdates,
	}

	return m, nil
}

func (m *Manager) Start() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancelFunc = cancelFunc
	m.startUpdatePassing(ctx)
	return nil
}

func (m *Manager) Updates() chan interface{} {
	return m.updates
}

func (m *Manager) Shutdown() error {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.finished.Wait()
	}
	return nil
}

func (m *Manager) AddMessage(id ids.ID, name string, body []byte, fromSelf bool) error {
	switch name {
	case "eav":
		ops, err := eav.DeserializeOps(body)
		if err != nil {
			m.log.Warnf("error deserializing ops %#v", err)
			return nil
		}
		applier := eav.Other
		if fromSelf {
			applier = eav.Self
		}
		if _, _, err := m.EAV.Apply(id, applier, ops); err != nil {
			return err
		}
		return nil
	default:
		m.log.Warnf("unknown database name %s", name)
		return nil
	}
}

func (m *Manager) AddBackfill(id ids.ID, name string, body []byte, fromSelf bool) error {
	if name != EAVDatabase {
		return fmt.Errorf("expected name %s, got %s", EAVDatabase, name)
	}
	applier := eav.Other
	if fromSelf {
		applier = eav.Self
	}
	return m.EAV.ProcessBackfill(id, applier, body)
}

func (m *Manager) startUpdatePassing(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case update := <-m.eavUpdates:
				m.updates <- update
			}
		}
	}()
}
