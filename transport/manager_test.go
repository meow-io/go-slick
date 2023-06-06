package transport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/internal/test"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	os.Exit(test.DBCleanup(m.Run))
}

type testClock struct {
	offsetMicro uint64
}

func (tc *testClock) CurrentTimeMicro() uint64 {
	return uint64(time.Now().UnixMicro()) + tc.offsetMicro
}

func (tc *testClock) CurrentTimeMs() uint64 {
	return uint64(time.Now().UnixMilli()) + tc.offsetMicro/1000
}

func (tc *testClock) CurrentTimeSec() uint64 {
	return tc.CurrentTimeMs() / 1000
}

func (tc *testClock) Now() time.Time {
	return time.Now().Add(time.Duration(tc.offsetMicro * uint64(time.Microsecond)))
}

func (tc *testClock) AdvanceMs(a uint64) {
	tc.offsetMicro += a * 1000
}

type report struct {
	groupID ids.ID
	URL     string
	Added   bool
}

type testManager struct {
	manager  *Manager
	db       *db.Database
	messages chan Message
	reports  chan *report
}

type testManagerMaker struct {
	managers []*testManager
	clock    *testClock
}

func newTestManagerMaker() *testManagerMaker {
	c := &testClock{}
	return &testManagerMaker{
		managers: make([]*testManager, 0),
		clock:    c,
	}
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
}

func (tmm *testManagerMaker) AddManager(prefix string) *testManager {
	messages := make(chan Message, 100)
	config := config.NewConfig(config.WithLoggingPrefix(prefix))
	db := test.NewTestDatabase(config)
	reports := make(chan *report, 100)
	manager, err := NewManager(config, db, tmm.clock, false, func(ms []Message) error {
		for _, m := range ms {
			messages <- m
		}
		return nil
	}, func(i ids.ID, s string, b bool) error {
		reports <- &report{i, s, b}
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

func TestCreateTransportsForGroup(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	require.Nil(m1.manager.Shutdown())
}

func TestPreflightWithInvalidURL(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	errors := make(chan error)
	m1 := tmm.AddManager("manager1")
	ctx, cancelFn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	m1.manager.performPreflightUpdates(ctx, errors)
	require.Nil(<-errors)
	require.Nil(<-errors)
	cancelFn()
	status := m1.manager.Preflight([]string{"id://qweasdzxc"})
	require.Equal([]bool{true}, status)

	tmm.clock.AdvanceMs(60000)
	ctx, cancelFn = context.WithTimeout(context.Background(), 100*time.Millisecond)
	m1.manager.performPreflightUpdates(ctx, errors)
	require.Nil(<-errors)
	require.Nil(<-errors)
	cancelFn()
	status = m1.manager.Preflight([]string{"id://qweasdzxc"})
	require.Equal([]bool{false}, status)
}

func TestPreflightWithValidURL(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	errors := make(chan error)
	m1 := tmm.AddManager("manager1")
	groupID := ids.NewID()
	require.Nil(m1.manager.ReportGroup(groupID))
	report := <-m1.reports

	ctx, cancelFn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	m1.manager.performPreflightUpdates(ctx, errors)
	require.Nil(<-errors)
	require.Nil(<-errors)
	cancelFn()
	status := m1.manager.Preflight([]string{report.URL})
	require.Equal([]bool{true}, status)

	tmm.clock.AdvanceMs(60000)
	ctx, cancelFn = context.WithTimeout(context.Background(), 100*time.Millisecond)
	m1.manager.performPreflightUpdates(ctx, errors)
	require.Nil(<-errors)
	require.Nil(<-errors)
	cancelFn()
	status = m1.manager.Preflight([]string{report.URL})
	require.Equal([]bool{true}, status)
}
