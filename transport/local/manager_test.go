package local

import (
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

type testReport struct {
	groupID ids.ID
	url     string
	added   bool
}

type testManager struct {
	manager  *Manager
	db       *db.Database
	messages chan *MessageImpl
	reports  chan *testReport
}

type testManagerMaker struct {
	managers []*testManager
}

func makeTestManagerMaker() *testManagerMaker {
	return &testManagerMaker{
		managers: make([]*testManager, 0),
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
	messages := make(chan *MessageImpl, 100)
	reports := make(chan *testReport, 100)
	config := config.NewConfig(config.WithLoggingPrefix(prefix))
	db := test.NewTestDatabase(config)
	manager, err := NewManager(config, db, func(mi []*MessageImpl) error {
		for _, m := range mi {
			messages <- m
		}
		return nil
	}, func(i ids.ID, s string, b bool) error {
		reports <- &testReport{i, s, b}
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
	require := require.New(t)

	tmm := makeTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1")
	m2 := tmm.AddManager("manager2")

	var url1, url2 string
	require.Eventually(func() bool {
		urls := m1.manager.TransportURLs()
		if len(urls) == 1 {
			url1 = urls[0]
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
	require.Eventually(func() bool {
		urls := m2.manager.TransportURLs()
		if len(urls) == 1 {
			url2 = urls[0]
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)

	require.Nil(m1.manager.Send(url2, []byte("hello")))
	m := <-m2.messages
	require.Equal(url1, m.From())
	require.Equal(url2, m.To())
	require.Equal([]byte("hello"), m.Body())
}
