// This package provides a high-level interface to the Slick implementation.
// It provides functions for defining schemas, writing, and querying data. As well
// it provides functions for joining, leaving and creating groups as well as
// the device group.
package slick

import (
	"context"
	crypto_rand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/data"
	"github.com/meow-io/go-slick/data/eav"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/messaging"
	"github.com/meow-io/go-slick/migration"
	"github.com/meow-io/go-slick/transport"
	"github.com/meow-io/go-slick/transport/heya"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// Useful for debugging
// "github.com/davecgh/go-spew/spew"

const (
	// Constants for application state.
	StateNew = iota
	StateInitialized
	StateRunning
	StateClosing
	StateClosed
	// Meta-intro states useful to developers. INTRO_SUCCEEDED indicates a completely backfilled group. IntroFailed indicates something went
	// wrong during the invite process.
	IntroFailed    = 1000
	IntroSucceeded = 1001
)

type applicationMessage struct {
	Name string `bencode:"n"`
	Body []byte `bencode:"b"`
}

type deviceGroupApplicationMessage struct {
	GroupID ids.ID `bencode:"i"`
	Name    string `bencode:"n"`
	Body    []byte `bencode:"b"`
}

// An event indicating an update to a table.
type TableUpdate struct {
	ID        ids.ID
	Name      string
	Tablename string
}

// An event indicating a change in the state of Slick.
type AppState struct {
	State int
}

// An event indicating a change in a group.
type GroupUpdate struct {
	ID                   ids.ID
	AckedMemberCount     uint
	GroupState           int
	MemberCount          uint
	ConnectedMemberCount uint
	Seq                  uint64
	PendingMessageCount  uint
}

// An event indicating a change in an intro.
type IntroUpdate struct {
	GroupID   ids.ID
	Initiator bool
	Stage     uint32
	Type      uint32
}

type TransportStateUpdate struct {
	URL   string
	State string
}

type MessagesFetchedUpdate struct {
}

// A group.
type Group struct {
	ID            ids.ID
	AuthorTag     [7]byte
	IdentityTag   [4]byte
	MembershipTag [3]byte
	State         int
	Name          string
	group         *messaging.Group
}

func newGroup(g *messaging.Group) *Group {
	identityTag := [4]byte(g.AuthorTag[0:4])
	membershipTag := [3]byte(g.AuthorTag[4:])

	return &Group{
		ID:            g.ID,
		AuthorTag:     g.AuthorTag,
		IdentityTag:   identityTag,
		MembershipTag: membershipTag,
		State:         g.State,
		Name:          g.Name,
		group:         g,
	}
}

// Make a random 6-digit pincode for use in invites
func NewPin() (string, error) {
	nBig, err := crypto_rand.Int(crypto_rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", nBig.Uint64()), nil
}

type Slick struct {
	DeviceGroup        *deviceGroup
	DB                 *db.Database
	lastTimestamp      uint64
	timeVersion        uint8
	config             *config.Config
	log                *zap.SugaredLogger
	state              int
	clock              clock.Clock
	messaging          *messaging.Manager
	transport          *transport.Manager
	data               *data.Manager
	updates            chan interface{}
	cancelFunc         context.CancelFunc
	finished           sync.WaitGroup
	transportStates    map[string]string
	transportStateLock sync.Mutex
	applicationInit    func() error
}

// Create a slick instance
func NewSlick(c *config.Config, applicationInit func() error) (*Slick, error) {
	log := c.Logger("")
	absRootPath, err := filepath.Abs(c.RootDir)
	if err != nil {
		return nil, err
	}
	c.RootDir = absRootPath
	log.Debugf("making slick, using root path of %s", c.RootDir)

	if err := os.MkdirAll(c.RootDir, 0o700); err != nil {
		return nil, err
	}
	cl := clock.NewSystemClock()
	db, err := db.NewDatabase(c, cl, path.Join(c.RootDir, "data"))
	if err != nil {
		return nil, err
	}

	state := StateNew
	if db.Initialized() {
		state = StateInitialized
	}
	clock := clock.NewSystemClock()

	slick := &Slick{
		lastTimestamp:   0,
		timeVersion:     0,
		log:             log,
		config:          c,
		clock:           clock,
		state:           state,
		messaging:       nil,
		transport:       nil,
		data:            nil,
		updates:         make(chan interface{}, 100),
		DB:              db,
		transportStates: make(map[string]string),
		applicationInit: applicationInit,
	}

	return slick, nil
}

// Get current transport states
func (s *Slick) TransportStates() map[string]string {
	s.transportStateLock.Lock()
	defer s.transportStateLock.Unlock()
	return maps.Clone(s.transportStates)
}

// Makes a key from a password
func (s *Slick) NewKey(password string) ([]byte, error) {
	return newKey(password, s.config.RootDir, "salt")
}

// Makes a new id for use in a write to EAV.
func (s *Slick) NewID(authorTag [7]byte) (ids.ID, error) {
	var id [16]byte
	currentMicro := s.clock.CurrentTimeMicro()
	if currentMicro <= s.lastTimestamp {
		if err := s.loadTimeVersion(); err != nil {
			return id, err
		}
	}
	s.lastTimestamp = currentMicro
	binary.BigEndian.PutUint64(id[0:8], currentMicro)
	id[8] = s.timeVersion
	copy(id[9:], authorTag[0:7])
	return id, nil
}

// Gets various updates which must be dealt with.
// This will either produce *GroupUpdate, *eav.TableRowUpdate, *eav.TableRowInsert or *eav.TableUpdate
func (s *Slick) Updates() chan interface{} {
	return s.updates
}

// Returns true is slick is in NEW state.
func (s *Slick) New() bool {
	return s.state == StateNew
}

// Returns true is slick is in INITIALIZED state.
func (s *Slick) Initialized() bool {
	return s.state == StateInitialized
}

// Returns true is slick is in RUNNING state.
func (s *Slick) Running() bool {
	return s.state == StateRunning
}

// Initialize slick with a given key.
func (s *Slick) Initialize(key []byte) error {
	if err := s.initialize(key); err != nil {
		return err
	}
	return s.open(key, false)
}

func (s *Slick) initialize(key []byte) error {
	if s.state != StateNew {
		return errors.New("cannot initialize unless in state new")
	}

	if err := s.DB.Initialize(key); err != nil {
		return err
	}

	s.setState(StateInitialized)
	return nil
}

// Open an existing slick with a given key.
func (s *Slick) Open(key []byte) error {
	return s.open(key, false)
}

// Open an existing slick with a given key, get all messages from it and wait till it's done.
func (s *Slick) GetMessages(key []byte) error {
	done := make(chan bool)
	defer func() {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
		s.updates <- &MessagesFetchedUpdate{}
	}()

	// open
	if err := s.open(key, true); err != nil {
		done <- true
		return err
	}

	time.Sleep(1 * time.Second)

	// wait for initial messages to be processed
	s.transport.WaitForPending()

	// wait for everything to be delivered
	if err := s.messaging.WaitForDelivery(); err != nil {
		return err
	}

	done <- true

	return nil
}

func (s *Slick) open(key []byte, processOnce bool) error {
	if s.state != StateInitialized {
		return errors.New("cannot open unless in state initialized")
	}

	if err := s.DB.Open(key); err != nil {
		return err
	}

	if err := s.DB.Migrate("_slick", []*migration.Migration{
		{
			Name: "create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
CREATE TABLE _time_id_gen (
	id INTEGER PRIMARY KEY,
	time_version INTEGER NOT NULL
);

INSERT INTO _time_id_gen (id, time_version) values(1, 0);
					`)
				return err
			},
		},
	}); err != nil {
		return err
	}

	if err := s.loadTimeVersion(); err != nil {
		return err
	}

	if err := s.DB.Lock("initializing subsystems", func() error {
		transport, err := transport.NewManager(s.config, s.DB, s.clock, processOnce, func(messages []transport.Message) error {
			for _, message := range messages {
				if err := s.messaging.ProcessIncomingMessage(message.From(), message.Body()); err != nil {
					return err
				}
			}
			return nil
		}, func(groupID ids.ID, url string, added bool) error {
			return s.DB.Run(fmt.Sprintf("report change for %x", groupID), func() error {
				return s.messaging.ReportChange(groupID, url, added)
			})
		})
		if err != nil {
			return err
		}
		s.transport = transport
		data, err := data.NewManager(s.config, s.DB, s.clock)
		if err != nil {
			return err
		}
		s.data = data
		messaging, err := messaging.NewManager(s.config, s.DB, s.Transports(), s.sender(), s.processor(), s.clock, s.membershipUpdater())
		if err != nil {
			return err
		}
		s.messaging = messaging
		return nil
	}); err != nil {
		return err
	}

	if err := s.applicationInit(); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.cancelFunc = cancelFunc
	if err := s.transport.Start(); err != nil {
		return err
	}
	if err := s.data.Start(); err != nil {
		return err
	}
	if err := s.messaging.Start(); err != nil {
		return err
	}

	// device group init isn't low level, uses normal api
	deviceGroup, err := newDeviceGroup(s)
	if err != nil {
		return err
	}

	s.DeviceGroup = deviceGroup
	s.setState(StateRunning)
	s.startUpdatePassing(ctx)
	return nil
}

// Create a password-protected invite for a specific group.
func (s *Slick) Invite(groupID ids.ID, password string) (*messaging.Jpake1, error) {
	if groupID == s.DeviceGroup.ID {
		return nil, errors.New("cannot invite to device group with this method")
	}
	var invite *messaging.Jpake1
	return invite, s.DB.Run("invite member", func() error {
		var err error
		invite, err = s.messaging.InviteMember(groupID, password)
		return err
	})
}

// Accept a password-protected invite.
func (s *Slick) AcceptInvite(invite *messaging.Jpake1, password string) (ids.ID, error) {
	var id ids.ID
	return id, s.DB.Run("accept invite", func() error {
		var err error
		id, err = s.messaging.ApproveJpakeInvite(invite, password)
		return err
	})
}

// Cancel all invites for a specific group.
func (s *Slick) CancelInvites(groupID ids.ID) error {
	return s.DB.Run("canceling invites", func() error {
		return s.messaging.CancelInvites(groupID)
	})
}

// Gracefully stop an existing slick instance.
func (s *Slick) Shutdown() error {
	if s.state != StateRunning {
		return nil
	}
	// try to clean up memory after a shutdown
	defer runtime.GC()

	errs := make([]string, 0)
	s.cancelFunc()
	s.finished.Wait()

	if err := s.messaging.Shutdown(); err != nil {
		errs = append(errs, err.Error())
	}
	if err := s.data.Shutdown(); err != nil {
		errs = append(errs, err.Error())
	}
	if err := s.transport.Shutdown(); err != nil {
		errs = append(errs, err.Error())
	}
	if err := s.DB.Shutdown(); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) != 0 {
		return fmt.Errorf("error during shutdown: %s", strings.Join(errs, ", "))
	}

	s.cancelFunc = nil
	s.data = nil
	s.messaging = nil
	s.transport = nil

	s.setState(StateInitialized)

	close(s.updates)
	s.updates = make(chan interface{}, 100)

	return nil
}

// Create a new group.
func (s *Slick) CreateGroup(name string) (ids.ID, error) {
	if s.state != StateRunning {
		return ids.ID{}, fmt.Errorf("expected state %d, was %d", StateRunning, s.state)
	}

	var groupID ids.ID
	err := s.DB.Run("create group", func() error {
		var err error
		groupID, err = s.messaging.CreateGroup(name)
		if err != nil {
			return err
		}
		return s.transport.ReportGroup(groupID)
	})
	return groupID, err
}

// Register a heya transport.
func (s *Slick) RegisterHeyaTransport(authToken, host string, port int) error {
	return s.DB.Run("registering heya transport", func() error {
		err := s.transport.RegisterHeyaTransport(authToken, host, port)
		if err != nil {
			return err
		}

		groups, err := s.messaging.Groups()
		if err != nil {
			return err
		}
		for _, group := range groups {
			if err := s.transport.ReportGroup(group.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// Add a push notification token.
func (s *Slick) AddPushNotificationToken(token string) error {
	return s.DB.Run("add push notification token", func() error {
		return s.transport.AddPushNotificationToken(token)
	})
}

// Remove a push notification token.
func (s *Slick) DeletePushNotificationToken(token string) error {
	return s.DB.Run("remove push notification token", func() error {
		return s.transport.DeletePushNotificationToken(token)
	})
}

// Get all groups.
func (s *Slick) Groups() ([]*Group, error) {
	outGroups := make([]*Group, 0)
	if err := s.DB.Run("get groups", func() error {
		groups, err := s.messaging.Groups()
		if err != nil {
			return err
		}
		for _, g := range groups {
			if g.ID == s.messaging.DeviceGroupID() {
				continue
			}
			if g.State == messaging.GroupStateProposed {
				continue
			}
			outGroups = append(outGroups, newGroup(g))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return outGroups, nil
}

// Get a specific group.
func (s *Slick) Group(groupID ids.ID) (*Group, error) {
	outGroup := &Group{}
	if err := s.DB.Run("get group", func() error {
		g, err := s.messaging.Group(groupID)
		if err != nil {
			return err
		}
		outGroup = newGroup(g)
		return nil
	}); err != nil {
		return nil, err
	}
	return outGroup, nil
}

// Get the current group state for a specific group.
func (s *Slick) GroupState(groupID ids.ID) (*GroupUpdate, error) {
	var update *GroupUpdate

	err := s.DB.Run("get group update", func() error {
		state, err := s.messaging.GroupState(groupID)
		if err != nil {
			return err
		}

		update = &GroupUpdate{
			ID:                   groupID,
			AckedMemberCount:     state.AckedMemberCount,
			GroupState:           state.GroupState,
			MemberCount:          state.MemberCount,
			ConnectedMemberCount: state.ConnectedMemberCount,
			Seq:                  state.Seq,
			PendingMessageCount:  state.PendingMessageCount,
		}
		return nil
	})
	return update, err
}

// Creates a single table in the EAV database.
func (s *Slick) EAVCreateView(viewname string, def *eav.ViewDefinition) error {
	return s.data.EAV.CreateView(viewname, def)
}

func (s *Slick) EAVCreateViews(views map[string]*eav.ViewDefinition) error {
	for viewname, def := range views {
		if err := s.data.EAV.CreateView(viewname, def); err != nil {
			return err
		}
	}
	return nil
}

// Write to the EAV database.
func (s *Slick) EAVWrite(id ids.ID, ops *eav.Operations) error {
	return s.DB.Run("write eav", func() error {
		return s.eavWrite(id, ops)
	})
}

// Write to the EAV database.
func (s *Slick) EAVWriter(g *Group) *EAVWriter {
	return &EAVWriter{
		slick:     s,
		groupID:   g.ID,
		authorTag: g.AuthorTag,
	}
}

func (s *Slick) eavWrite(id ids.ID, ops *eav.Operations) error {
	s.log.Debugf("Writing to %x ops len=%d, %d", id, len(ops.OperationMap), len(ops.Names))
	publicGroupOps, selfGroupOps, err := s.data.EAV.Apply(id, eav.Self, ops)
	if err != nil {
		return err
	}
	if !publicGroupOps.Empty() {
		publicGroupOpsBytes, err := eav.SerializeOps(publicGroupOps)
		if err != nil {
			return err
		}

		if id == s.messaging.DeviceGroupID() {
			groupEnv := &deviceGroupApplicationMessage{GroupID: id, Name: data.EAVDatabase, Body: publicGroupOpsBytes}
			groupEnvBytes, err := bencode.Serialize(groupEnv)
			if err != nil {
				return err
			}
			if err := s.messaging.SendGroupMessage(id, groupEnvBytes); err != nil {
				return err
			}
		} else {
			groupEnv := &applicationMessage{Name: data.EAVDatabase, Body: publicGroupOpsBytes}
			groupEnvBytes, err := bencode.Serialize(groupEnv)
			if err != nil {
				return err
			}
			if err := s.messaging.SendGroupMessage(id, groupEnvBytes); err != nil {
				return err
			}
		}
	}

	if !selfGroupOps.Empty() {
		selfGroupOpsBytes, err := eav.SerializeOps(selfGroupOps)
		if err != nil {
			return err
		}

		selfEnv := &deviceGroupApplicationMessage{GroupID: id, Name: data.EAVDatabase, Body: selfGroupOpsBytes}
		selfEnvBytes, err := bencode.Serialize(selfEnv)
		if err != nil {
			return err
		}
		if err := s.messaging.SendGroupMessage(s.messaging.DeviceGroupID(), selfEnvBytes); err != nil {
			return err
		}
	}
	return nil
}

// Register callback for entities changes before they are committed
func (s *Slick) EAVSubscribeBeforeEntity(cb func(viewName string, groupID, id ids.ID) error, includeBackfill bool, views ...string) error {
	return s.DB.Run("subscribe before entity", func() error {
		return s.data.EAV.SubscribeBeforeEntity(cb, includeBackfill, views...)
	})
}

// Register callback for entities changes after they are committed
func (s *Slick) EAVSubscribeAfterEntity(cb func(viewName string, groupID, id ids.ID), includeBackfill bool, views ...string) error {
	return s.DB.Run("subscribe after entity", func() error {
		return s.data.EAV.SubscribeAfterEntity(cb, includeBackfill, views...)
	})
}

// Register callback for view changes before they are committed
func (s *Slick) EAVSubscribeBeforeView(cb func(viewName string) error, includeBackfill bool, views ...string) error {
	return s.DB.Run("subscribe before view", func() error {
		return s.data.EAV.SubscribeBeforeView(cb, includeBackfill, views...)
	})
}

// Register callback for view changes after they are committed
func (s *Slick) EAVSubscribeAfterView(cb func(viewName string), includeBackfill bool, views ...string) error {
	return s.DB.Run("subscribe after view", func() error {
		return s.data.EAV.SubscribeAfterView(cb, includeBackfill, views...)
	})
}

// Query the EAV database with a SQL statement.
func (s *Slick) EAVQuery(query string, vars ...interface{}) (*eav.Result, error) {
	var res *eav.Result
	if err := s.DB.RunReadOnly("query eav", func() error {
		var err error
		res, err = s.data.EAV.Query(query, vars...)
		return err
	}); err != nil {
		return nil, err
	}
	return res, nil
}

// Select multiple structs conforming to the sqlx-style tags.
func (s *Slick) EAVSelect(dest interface{}, query string, vars ...interface{}) error {
	return s.DB.RunReadOnly("select eav", func() error {
		return s.selectEAV(dest, query, vars...)
	})
}

func (s *Slick) selectEAV(dest interface{}, query string, vars ...interface{}) error {
	return s.data.EAV.Select(dest, query, vars...)
}

// Get a single struct conforming to the sqlx-style tags.
func (s *Slick) EAVGet(dest interface{}, query string, vars ...interface{}) error {
	return s.DB.RunReadOnly(fmt.Sprintf("get eav with query %s", query), func() error {
		return s.getEAV(dest, query, vars...)
	})
}

func (s *Slick) getEAV(dest interface{}, query string, vars ...interface{}) error {
	err := s.data.EAV.Get(dest, query, vars...)
	return err
}

func (s *Slick) startUpdatePassing(ctx context.Context) {
	s.finished.Add(1)
	go func() {
		defer s.finished.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-s.data.Updates():
				s.log.Debugf("passing update: data %#v", e)
				s.updates <- e
			case e := <-s.transport.Updates():
				switch v := e.(type) {
				case *heya.StateUpdate:
					url := fmt.Sprintf("heya://%s:%d", v.Host, v.Port)
					s.transportStateLock.Lock()
					s.transportStates[url] = v.State
					s.transportStateLock.Unlock()
					tsu := &TransportStateUpdate{URL: url, State: v.State}
					s.log.Debugf("passing update: transport state %#v", tsu)
					s.updates <- tsu
				}
			case e := <-s.messaging.Updates():
				switch v := e.(type) {
				case *messaging.GroupUpdate:
					s.log.Debugf("passing update: group update %#v", v)
					s.updates <- &GroupUpdate{
						ID:                   v.GroupID,
						AckedMemberCount:     v.AckedMemberCount,
						GroupState:           v.GroupState,
						MemberCount:          v.MemberCount,
						ConnectedMemberCount: v.ConnectedMemberCount,
						Seq:                  v.Seq,
						PendingMessageCount:  v.PendingMessageCount,
					}
				case *messaging.IntroFailed:
					s.log.Debugf("passing update: intro failed %#v", v)
					s.updates <- &GroupUpdate{
						ID:         v.GroupID,
						GroupState: IntroFailed,
					}
				case *messaging.IntroSucceeded:
					s.log.Debugf("passing update: intro succeeded %#v", v)
					s.updates <- &GroupUpdate{
						ID:         v.GroupID,
						GroupState: IntroSucceeded,
					}
				case *messaging.IntroUpdate:
					s.log.Debugf("passing update: intro update %#v", v)
					s.updates <- &IntroUpdate{
						GroupID:   v.GroupID,
						Stage:     v.Stage,
						Initiator: v.Initiator,
						Type:      v.Type,
					}
				default:
					s.log.Infof("Unpassed event %#v", e)
				}
			}
		}
	}()
}

func (s *Slick) sender() func(*messaging.AddressedMessage) []error {
	return func(m *messaging.AddressedMessage) []error {
		return s.transport.Send(m.From, m.URLs, m.Body)
	}
}

func (s *Slick) processor() *slickProcessor {
	return &slickProcessor{s}
}

func (s *Slick) Transports() *Transports {
	return &Transports{s}
}

func (s *Slick) setState(state int) {
	s.state = state
	s.updates <- &AppState{state}
}

func (s *Slick) loadTimeVersion() error {
	return s.DB.Run("load time", func() error {
		t := struct {
			TimeVersion uint8 `db:"time_version"`
		}{}
		if err := s.DB.Tx.Get(&t, "select time_version from _time_id_gen"); err != nil {
			return err
		}
		s.timeVersion = t.TimeVersion
		if _, err := s.DB.Tx.Exec("update _time_id_gen set time_version = (time_version + 1) % 256"); err != nil {
			return err
		}
		return nil
	})
}

func (s *Slick) membershipUpdater() messaging.MembershipUpdater {
	return func(groupID, identityID, membershipID ids.ID, membership *messaging.Membership) error {
		if groupID == s.messaging.DeviceGroupID() {
			return nil
		}
		s.log.Debugf("got a membership update! %x %x:%x", groupID, identityID, membershipID)
		return s.DeviceGroup.membershipUpdater(groupID, identityID, membershipID, membership)
	}
}

func (s *Slick) proposeMembership(groupID, identityID, membershipID ids.ID, membership *messaging.Membership) (ids.ID, *messaging.Membership, error) {
	return s.messaging.ProposeMembership(groupID, identityID, membershipID, membership)
}

func (s *Slick) addMembership(groupID, identityID, membershipID ids.ID, membership *messaging.Membership) error {
	return s.messaging.AddMembership(groupID, identityID, membershipID, membership)
}

type Transports struct {
	s *Slick
}

func (st *Transports) Preflight(urls []string) []bool {
	return st.s.transport.Preflight(urls)
}

func (st *Transports) StatusChanged(f func(string, bool)) {
	st.s.transport.StatusChanged(f)
}

func (st *Transports) URLsForGroup(id ids.ID) ([]string, error) {
	return st.s.transport.URLsForGroup(id)
}

func (st *Transports) ReportGroup(id ids.ID) error {
	return st.s.transport.ReportGroup(id)
}

type slickProcessor struct {
	s *Slick
}

func (sp *slickProcessor) Incoming(m *messaging.IncomingGroupMessage) error {
	if m.GroupID == sp.s.messaging.DeviceGroupID() {
		dgm := &deviceGroupApplicationMessage{}
		if err := bencode.Deserialize(m.Body, dgm); err != nil {
			sp.s.log.Warnf("error parsing incoming %#v", err)
			return nil
		}
		return sp.s.data.AddMessage(dgm.GroupID, dgm.Name, dgm.Body, true)
	}
	gm := &applicationMessage{}
	if err := bencode.Deserialize(m.Body, gm); err != nil {
		sp.s.log.Warnf("error parsing incoming %#v", err)
		return nil
	}
	return sp.s.data.AddMessage(m.GroupID, gm.Name, gm.Body, false)
}

func (sp *slickProcessor) Backfill(groupID, startFrom ids.ID, partial, fromSelf bool) ([]byte, ids.ID, bool, error) {
	var backfillBody []byte
	var nextID [16]byte
	var atEnd bool
	if err := sp.s.DB.Run("backfill", func() error {
		var err error
		group, err := sp.s.messaging.Group(groupID)
		if err != nil {
			return err
		}
		backfillBody, nextID, atEnd, err = sp.s.data.EAV.Backfill(groupID, group.AuthorTag, startFrom, partial, fromSelf)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, nextID, false, err
	}

	return backfillBody, nextID, atEnd, nil
}

func (sp *slickProcessor) ProcessBackfill(groupID ids.ID, backfill []byte, fromSelf bool) error {
	return sp.s.data.AddBackfill(groupID, data.EAVDatabase, backfill, fromSelf)
}
