package messaging

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/db"

	"github.com/meow-io/go-slick/internal/test"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	os.Exit(test.DBCleanup(m.Run))
}

func waitFor(m *testManager, tester func(interface{}) bool) {
	for e := range m.manager.Updates() {
		if tester(e) {
			return
		}
	}
	panic("couldn't get id")
}

func wait(m *testManager) ids.ID {
	var groupID ids.ID
	waitFor(m, func(i interface{}) bool {
		gu, ok := i.(*GroupUpdate)
		if !ok {
			return false
		}
		if gu.GroupID == m.manager.DeviceGroupID() {
			return false
		}
		if gu.GroupState != GroupStateSynced {
			return false
		}
		groupID = gu.GroupID
		return true
	})
	return groupID
}

type incomingMessages chan string

func noWaitConfig(prefix string) *config.Config {
	return config.NewConfig(
		config.WithAckWaitTimeMs(10),
		config.WithGroupMessageWaitTimeMs(10),
		config.WithPrivateMessageWaitTimeMs(10),
		config.WithLoggingPrefix(prefix),
	)
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

type testManager struct {
	manager    *Manager
	db         *db.Database
	incoming   incomingMessages
	backfill   incomingMessages
	control    *senderController
	transports *testTransports
}

type testManagerMaker struct {
	managers map[string]*testManager
	clock    *testClock
	running  bool
}

func newTestManagerMaker() *testManagerMaker {
	return &testManagerMaker{
		managers: make(map[string]*testManager),
		clock:    &testClock{0},
		running:  true,
	}
}

func (tmm *testManagerMaker) teardown() {
	shutdownErrors := make(chan error, len(tmm.managers))

	tmm.running = false

	for _, tm := range tmm.managers {
		if err := tm.manager.WaitForDelivery(); err != nil {
			panic(err)
		}
	}
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

type testTransports struct {
	urls    []string
	m       *testManager
	reports chan ids.ID
}

func (t testTransports) ReportGroup(groupID ids.ID) error {
	t.reports <- groupID
	for _, u := range t.urls {
		if err := t.m.manager.ReportChange(groupID, u, true); err != nil {
			return err
		}
	}
	return nil
}

func (t testTransports) URLsForGroup(_ ids.ID) ([]string, error) {
	return t.urls, nil
}

func (t testTransports) Preflight(urls []string) []bool {
	ret := make([]bool, len(urls))
	for i := range urls {
		ret[i] = true
	}
	return ret
}

func (t testTransports) StatusChanged(_ func(string, bool)) {
}

func (tmm *testManagerMaker) AddManager(prefix string, urls []string) *testManager {
	incoming := make(incomingMessages, 100)
	backfill := make(incomingMessages, 100)

	dc := &senderController{state: deliveryControllerNormal, perState: make(map[string]int)}

	sender := dc.makeSender(urls, tmm)
	incomingProcessor := makeChannelProcessor(incoming, backfill)
	config := noWaitConfig(prefix)
	db := test.NewTestDatabase(config)
	tm := &testManager{nil, nil, incoming, backfill, dc, nil}
	tt := &testTransports{urls, tm, make(chan ids.ID, 100)}
	manager, err := NewManager(config, db, tt, sender, incomingProcessor, tmm.clock, nil)
	if err != nil {
		panic(err)
	}
	tm.transports = tt
	tm.manager = manager
	tm.db = db
	if err := manager.Start(); err != nil {
		panic(err)
	}

	for _, url := range urls {
		tmm.managers[url] = tm
	}
	return tm
}

type testProcessor struct {
	out      chan string
	backfill chan string
}

func (tp *testProcessor) Incoming(message *IncomingGroupMessage) error {
	tp.out <- string(message.Body)
	return nil
}

func (tp *testProcessor) Backfill(_, _ ids.ID, _, _ bool) ([]byte, ids.ID, bool, error) {
	return []byte("hello"), [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, true, nil
}

func (tp *testProcessor) ProcessBackfill(_ ids.ID, b []byte, _ bool) error {
	tp.backfill <- string(b)
	return nil
}

func makeChannelProcessor(out chan string, backfill chan string) processor {
	return &testProcessor{out, backfill}
}

const (
	deliveryControllerNormal = iota
	deliveryControllerIntermittent
	deliveryControllerBlackhole
	deliveryControllerError
)

type senderController struct {
	state    int
	perState map[string]int
}

func (dc *senderController) makeSender(fromURLs []string, tmm *testManagerMaker) sender {
	deliveryCount := 0
	return func(am *AddressedMessage) []error {
		errs := make([]error, len(am.URLs))
		for i := range am.URLs {
			to := am.URLs[i]
			switch dc.deliveryState(to) {
			case deliveryControllerNormal:
				errs[i] = nil
				go func(to string) {
					target := tmm.managers[to]
					if err := target.db.Run("Processing incoming message", func() error {
						return target.manager.ProcessIncomingMessage(fromURLs[0], am.Body)
					}); err != nil {
						if tmm.running {
							panic(err)
						}
					}
				}(to)
			case deliveryControllerIntermittent:
				if deliveryCount%2 == 0 {
					errs[i] = nil
					go func(to string) {
						target := tmm.managers[to]
						if err := target.db.Run("Processing incoming message", func() error {
							return target.manager.ProcessIncomingMessage(fromURLs[0], am.Body)
						}); err != nil {
							if tmm.running {
								panic(err)
							}
						}
					}(to)
				} else {
					errs[i] = errors.New("intermittent error")
				}
				deliveryCount++
			case deliveryControllerBlackhole:
				errs[i] = nil
			case deliveryControllerError:
				errs[i] = errors.New("error")
			}
		}
		return errs
	}
}

func (dc *senderController) deliveryState(to string) int {
	state, ok := dc.perState[to]
	if ok {
		return state
	}
	return dc.state
}

func (dc *senderController) clearMap() {
	dc.perState = make(map[string]int)
}

func (dc *senderController) BlackholeDelivery() {
	dc.clearMap()
	dc.state = deliveryControllerBlackhole
}

func (dc *senderController) BlackholeDeliveryFor(to string) {
	dc.perState[to] = deliveryControllerBlackhole
}

func (dc *senderController) ErrorDeliveryFor(to string) {
	dc.perState[to] = deliveryControllerError
}

func (dc *senderController) NormalDelivery() {
	dc.clearMap()
	dc.state = deliveryControllerNormal
}

func (dc *senderController) IntermittentDelivery() {
	dc.clearMap()
	dc.state = deliveryControllerIntermittent
}

func TestJpakeGroupSetup(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		return err
	}))

	var invite *Jpake1
	require.Nil(m1.db.Run("invite member", func() error {
		var err error
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve member", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	groupID1 := wait(m1)
	groupID2 := wait(m2)

	require.Nil(m2.db.Run("send message", func() error {
		return m2.manager.SendGroupMessage(groupID2, []byte("what up"))
	}))
	require.Equal(<-m1.incoming, "what up")

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(groupID1, []byte("hey there"))
	}))
	require.Equal(<-m2.incoming, "hey there")
}

func TestJpakeGroupSetupWithIntermittentDelivery(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	m1.control.IntermittentDelivery()
	m2.control.IntermittentDelivery()

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		return err
	}))

	var invite *Jpake1
	require.Nil(m1.db.Run("invite member", func() error {
		var err error
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve member", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	groupID1 := wait(m1)
	groupID2 := wait(m2)

	require.Nil(m2.db.Run("send message", func() error {
		return m2.manager.SendGroupMessage(groupID2, []byte("what up"))
	}))
	require.Equal(<-m1.incoming, "what up")

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(groupID1, []byte("hey there"))
	}))
	require.Equal(<-m2.incoming, "hey there")
}

func TestPushNotification(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		return err
	}))

	var invite *Jpake1
	require.Nil(m1.db.Run("invite member", func() error {
		var err error
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve member", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	groupID1 := wait(m1)
	groupID2 := wait(m2)

	require.Nil(m2.db.Run("send message", func() error {
		return m2.manager.SendGroupMessage(groupID2, []byte("what up"))
	}))
	require.Equal(<-m1.incoming, "what up")

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(groupID1, []byte("hey there"))
	}))
	require.Equal(<-m2.incoming, "hey there")
}

func TestDifferentPasswords(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		return err
	}))

	var invite *Jpake1
	require.Nil(m1.db.Run("invite member", func() error {
		var err error
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "wrong-password")
		return err
	}))

	for groupUpdate := range m1.manager.Updates() {
		if _, ok := groupUpdate.(*IntroFailed); ok {
			break
		}
	}
}

func TestPrekeyGroupSetup(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
	managers := []*Manager{m1.manager, m2.manager}

	groupID := ids.NewID()
	err := m1.db.Run("manager 1 setup", func() error {
		return m2.db.Run("manager 2 setup", func() error {
			groupIdentities := make(IdentityMap)
			identIDs := [][]ids.ID{}
			keys := make([]ed25519.PrivateKey, 2)
			for i, m := range managers {
				identityID := ids.NewID()
				membershipID := ids.NewID()
				membership, key, err := m.generateGroupMembership(identityID, membershipID, groupID)
				require.Nil(err)
				identIDs = append(identIDs, []ids.ID{identityID, membershipID})
				keys[i] = key
				groupIdentities[identIDs[i][0]] = make(MembershipMap)
				groupIdentities[identIDs[i][0]][identIDs[i][1]] = membership
			}

			gd := &GroupDescription{
				Name:        &LWWString{},
				Description: &LWWString{},
				Icon:        &LWWBlob{},
				IconType:    &LWWString{},
				Identities:  groupIdentities,
			}

			gdb, err := bencode.Serialize(gd)
			if err != nil {
				return fmt.Errorf("messaging: error creating group %w", err)
			}

			digest := sha256.Sum256(gdb)
			groupd := groupDescription{
				Digest: digest[:],
				Desc:   gdb,
			}

			for i, m := range managers {
				g := &group{
					ID:                 groupID[:],
					DescDigest:         digest[:],
					SelfIdentityID:     identIDs[i][0][:],
					SelfMembershipID:   identIDs[i][1][:],
					SourceIdentityID:   identIDs[i][0][:],
					SourceMembershipID: identIDs[i][1][:],
					State:              GroupStateSynced,
					IntroKeyPriv:       keys[i][:],
				}
				if err := m.db.insertGroupDescription(&groupd); err != nil {
					return err
				}

				if err := m.db.upsertGroup(g); err != nil {
					return err
				}

				if _, err := m.processGroupDescription(groupID, identIDs[i][0], identIDs[i][1], digest, gd, nil); err != nil {
					return err
				}
			}
			return nil
		})
	})
	require.Nil(err)

	for groupUpdate := range m1.manager.Updates() {
		if up, ok := groupUpdate.(*GroupUpdate); ok {
			if up.ConnectedMemberCount == 1 {
				break
			}
		}
	}
	for groupUpdate := range m2.manager.Updates() {
		if up, ok := groupUpdate.(*GroupUpdate); ok {
			if up.ConnectedMemberCount == 1 {
				break
			}
		}
	}

	require.Nil(m1.db.Run("manager 1 setup", func() error {
		return m1.manager.SendGroupMessage(groupID, []byte("hey there"))
	}))
	require.Equal("hey there", <-m2.incoming)
}

func TestJpakeAutoAck(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		return err
	}))

	var invite *Jpake1
	require.Nil(m1.db.Run("invite member", func() error {
		var err error
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	wait(m1)
	wait(m2)

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(id, []byte("hey there"))
	}))
	require.Equal("hey there", <-m2.incoming)

	waitFor(m1, func(i interface{}) bool {
		if up, ok := i.(*GroupUpdate); ok {
			return up.GroupID == id && up.Seq == 1 && up.AckedMemberCount == 1
		}
		return false
	})
}

func TestThreePartyPrekey(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
	m3 := tmm.AddManager("manager3", []string{"id:sha-256;cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"})

	var id ids.ID
	require.Nil(m2.db.Run("create group", func() error {
		var err error
		id, err = m2.manager.CreateGroup("group1")
		return err
	}))

	var invite1, invite2 *Jpake1
	require.Nil(m2.db.Run("invite member", func() error {
		var err error
		invite1, err = m2.manager.InviteMember(id, "password")
		return err
	}))
	require.Nil(m1.db.Run("approve invite", func() error {
		_, err := m1.manager.ApproveJpakeInvite(invite1, "password")
		return err
	}))

	require.Nil(m2.db.Run("invite member", func() error {
		var err error
		invite2, err = m2.manager.InviteMember(id, "password")
		return err
	}))
	require.Nil(m3.db.Run("approve invite", func() error {
		_, err := m3.manager.ApproveJpakeInvite(invite2, "password")
		return err
	}))

	waitFor(m2, func(i interface{}) bool {
		if up, ok := i.(*GroupUpdate); ok {
			return up.GroupID == id && up.MemberCount == 2
		}
		return false
	})

	require.Nil(m2.db.Run("send message", func() error {
		return m2.manager.SendGroupMessage(id, []byte("what up"))
	}))

	require.Equal("what up", <-m1.incoming)
	require.Equal("what up", <-m3.incoming)

	waitFor(m2, func(i interface{}) bool {
		if up, ok := i.(*GroupUpdate); ok {
			return up.GroupID == id && up.Seq == 1 && up.AckedMemberCount == 2
		}
		return false
	})
}

func TestLostGroupMessage(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	var invite *Jpake1
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		require.Nil(err)
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	wait(m1)
	wait(m2)

	m1.control.BlackholeDelivery()

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(id, []byte("hey there1"))
	}))

	tmm.clock.AdvanceMs(1000)

	m1.control.NormalDelivery()
	require.Equal(0, len(m2.incoming))

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(id, []byte("hey there2"))
	}))
	messages := make([]string, 2)
	messages[0] = <-m2.incoming
	messages[1] = <-m2.incoming
	sort.Strings(messages)

	require.Equal([]string{"hey there1", "hey there2"}, messages)
}

func TestLostPrivateMessage(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var id ids.ID
	var invite *Jpake1
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("group1")
		require.Nil(err)
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	wait(m1)
	wait(m2)

	m1.control.BlackholeDelivery()

	require.Nil(m1.db.Run("send private message", func() error {
		return m1.manager.SendGroupMessage(id, []byte("hey there1"))
	}))

	tmm.clock.AdvanceMs(1000)

	require.Equal(0, len(m2.incoming))

	m1.control.NormalDelivery()

	require.Nil(m1.db.Run("send message", func() error {
		return m1.manager.SendGroupMessage(id, []byte("hey there2"))
	}))

	messages := make([]string, 2)
	messages[0] = <-m2.incoming
	messages[1] = <-m2.incoming
	sort.Strings(messages)

	require.Equal([]string{"hey there1", "hey there2"}, messages)
}

func TestThreePartyFanout(t *testing.T) {
	require := require.New(t)
	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
	m3 := tmm.AddManager("manager3", []string{"id:sha-256;cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"})

	// manager 1 and manager 3 are prevented from prekeying to each other
	m1.control.BlackholeDeliveryFor("id:sha-256;cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	var id ids.ID
	var invite1, invite2 *Jpake1
	require.Nil(m2.db.Run("create group & invite", func() error {
		var err error
		id, err = m2.manager.CreateGroup("group1")
		require.Nil(err)
		invite1, err = m2.manager.InviteMember(id, "password")
		require.Nil(err)
		invite2, err = m2.manager.InviteMember(id, "password")
		return err
	}))

	require.Nil(m1.db.Run("approve invite", func() error {
		_, err := m1.manager.ApproveJpakeInvite(invite1, "password")
		return err
	}))

	require.Nil(m3.db.Run("approve invite", func() error {
		_, err := m3.manager.ApproveJpakeInvite(invite2, "password")
		return err
	}))

	waitFor(m2, func(i interface{}) bool {
		up, ok := i.(*GroupUpdate)
		if !ok {
			return false
		}
		return up.GroupID == id && up.ConnectedMemberCount == 2
	})

	require.Nil(m1.db.Run("send message", func() error {
		groups, err := m1.manager.Groups()
		if err != nil {
			return err
		}
		return m1.manager.SendGroupMessage(groups[1].ID, []byte("what up"))
	}))
	require.Equal("what up", <-m3.incoming)
}

func TestBackfill(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})

	var invite *Jpake1
	require.Nil(m1.db.Run("create group & invite", func() error {
		var err error
		id, err := m1.manager.CreateGroup("group1")
		require.Nil(err)
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))
	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	require.Equal(<-m2.backfill, "hello")

	waitFor(m2, func(i interface{}) bool {
		up, ok := i.(*GroupUpdate)
		return ok && up.GroupState == GroupStateSynced
	})
}

func TestMultiTransport(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{"id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "heya:/a/a"})
	m2 := tmm.AddManager("manager2", []string{"id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "heya:/b/b"})

	m1.control.ErrorDeliveryFor("id:sha-256;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	m2.control.ErrorDeliveryFor("id:sha-256;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	var invite *Jpake1
	require.Nil(m1.db.Run("create group & invite", func() error {
		var err error
		id, err := m1.manager.CreateGroup("group1")
		require.Nil(err)
		invite, err = m1.manager.InviteMember(id, "password")
		return err
	}))
	require.Nil(m2.db.Run("approve invite", func() error {
		_, err := m2.manager.ApproveJpakeInvite(invite, "password")
		return err
	}))

	wait(m2)

	require.Nil(m1.db.Run("send message", func() error {
		groups, err := m1.manager.Groups()
		if err != nil {
			return err
		}
		return m1.manager.SendGroupMessage(groups[1].ID, []byte("what up"))
	}))
	require.Equal("what up", <-m2.incoming)
}

func TestGroupReportAdding(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("test")
		return err
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have one endpoint", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 0 && mem.Description.Version == 1
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))

	require.Nil(m1.db.Run("report a new endpoint", func() error {
		return m1.manager.ReportChange(id, "heya:/a/b", true)
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have one endpoint", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 1 && mem.Description.Version == 2
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))
}

func TestGroupReportRemoving(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("test")
		require.Nil(err)
		require.Nil(m1.manager.ReportChange(id, "id:blahblah", true))
		require.Nil(m1.manager.ReportChange(id, "heya:/a/b", true))
		return nil
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have two endpoints", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 2 && mem.Description.Version == 3
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))

	require.Nil(m1.db.Run("report a removed endpoint", func() error {
		return m1.manager.ReportChange(id, "heya:/a/b", false)
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have only one endpoint now", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 1 && mem.Description.Version == 4
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))
}

func TestGroupDoubleReporting(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{})

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("test")
		return err
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have one endpoint", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 0 && mem.Description.Version == 1
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))

	require.Nil(m1.db.Run("report a new endpoint", func() error {
		return m1.manager.ReportChange(id, "heya:/a/b", true)
	}))
	require.Nil(m1.db.Run("report a new endpoint", func() error {
		return m1.manager.ReportChange(id, "heya:/a/b", true)
	}))

	require.Nil(m1.db.Run("get group desc and ensure we have one endpoint", func() error {
		require.Eventually(func() bool {
			group, err := m1.manager.db.group(id[:])
			require.Nil(err)
			desc, err := m1.manager.db.groupDescription(group.DescDigest)
			require.Nil(err)
			decoded, err := desc.decodeDescription()
			require.Nil(err)
			mem := decoded.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)]
			return len(mem.Description.Endpoints) == 1 && mem.Description.Version == 2
		}, 3*time.Second, 100*time.Millisecond)
		return nil
	}))
}

func TestGroupReportingOnStartup(t *testing.T) {
	require := require.New(t)

	tmm := newTestManagerMaker()
	defer tmm.teardown()

	m1 := tmm.AddManager("manager1", []string{})
	report1 := <-m1.transports.reports
	require.Equal(m1.manager.DeviceGroupID(), report1)

	var id ids.ID
	require.Nil(m1.db.Run("create group", func() error {
		var err error
		id, err = m1.manager.CreateGroup("test")
		return err
	}))
	report2 := <-m1.transports.reports
	require.Equal(id, report2)
	require.Nil(m1.manager.Shutdown())
	require.Nil(m1.manager.Start())
	reports := make([]ids.ID, 2)
	reports[0] = <-m1.transports.reports
	reports[1] = <-m1.transports.reports
	sort.Sort(ids.ByLexicographical(reports))

	require.Equal(m1.manager.DeviceGroupID(), reports[0])
	require.Equal(id, reports[1])
}
