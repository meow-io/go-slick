// Package implements reliable messaging layer for parties identified by a URL.
package messaging

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/kevinburke/nacl"
	"github.com/kevinburke/nacl/box"
	"github.com/kevinburke/nacl/scalarmult"
	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/crypto"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/transport/heya"
	"github.com/meow-io/go-slick/transport/local"
	"github.com/status-im/doubleratchet"
	"go.uber.org/zap"
)

const (
	jpakeSecretKey1 = "JPAKE_SECRET_KEY_1"
	jpakeSecretKey2 = "JPAKE_SECRET_KEY_2"
) // #nosec G101

type membershipMap struct {
	memberships map[ids.ID]map[ids.ID]*groupMembership
	groupID     ids.ID
	m           *Manager
}

func newMembershipMap(m *Manager, groupID ids.ID) *membershipMap {
	return &membershipMap{
		memberships: make(map[ids.ID]map[ids.ID]*groupMembership),
		groupID:     groupID,
		m:           m,
	}
}

func (mm *membershipMap) Load(identityID, membershipID ids.ID) (*groupMembership, error) {
	var err error
	membership := mm.Get(identityID, membershipID)
	if membership == nil {
		membership, err = mm.m.db.groupMembership(mm.groupID[:], identityID[:], membershipID[:])
		if err != nil {
			return nil, err
		}
		mm.Set(identityID, membershipID, membership)
	}
	return membership, nil
}

func (mm *membershipMap) Has(identityID, membershipID ids.ID) bool {
	if _, ok := mm.memberships[identityID]; !ok {
		return false
	} else if _, ok := mm.memberships[identityID][membershipID]; !ok {
		return false
	} else {
		return true
	}
}

func (mm *membershipMap) Get(identityID, membershipID ids.ID) *groupMembership {
	if _, ok := mm.memberships[identityID]; !ok {
		return nil
	} else if gm, ok := mm.memberships[identityID][membershipID]; ok {
		return gm
	} else {
		return nil
	}
}

func (mm membershipMap) Set(identityID, membershipID ids.ID, m *groupMembership) {
	if _, ok := mm.memberships[identityID]; !ok {
		mm.memberships[identityID] = make(map[ids.ID]*groupMembership)
	}
	mm.memberships[identityID][membershipID] = m
}

type Transports interface {
	URLsForGroup(id ids.ID) ([]string, error)
	ReportGroup(id ids.ID) error
	Preflight([]string) []bool
	StatusChanged(func(string, bool))
}

type Group struct {
	manager          *Manager
	ID               ids.ID
	Name             string
	AuthorTag        [7]byte
	SelfIdentityID   [16]byte
	SelfMembershipID [16]byte
	State            int
}

type Member struct {
	ID      ids.ID
	Members []ids.ID
}

type IntroUpdate struct {
	GroupID   ids.ID
	Initiator bool
	Stage     uint32
	Type      uint32
}

type GroupMembership []*Member

type GroupUpdate struct {
	GroupID              ids.ID
	GroupState           int
	MemberCount          uint
	ConnectedMemberCount uint
	Seq                  uint64
	PendingMessageCount  uint
	AckedMemberCount     uint
}

type IntroFailed struct {
	IntroID ids.ID
	GroupID ids.ID
}

type IntroSucceeded struct {
	IntroID ids.ID
	GroupID ids.ID
}

type DeviceInvite struct {
	ID      ids.ID
	CtimeMs uint64
}

type AddressedMessage struct {
	From string
	URLs []string
	Body []byte
}

type backfillSourceJob struct {
	BackfillSourceID ids.ID
	GroupID          ids.ID
	MaxID            ids.ID
	Partial          bool
	FromSelf         bool
}

type IncomingGroupMessage struct {
	GroupID      ids.ID
	IdentityID   ids.ID
	MembershipID ids.ID
	Seq          uint64
	Body         []byte
}

type processor interface {
	Incoming(*IncomingGroupMessage) error
	Backfill(groupID, startFrom ids.ID, partial, fromSelf bool) ([]byte, ids.ID, bool, error)
	ProcessBackfill(groupID ids.ID, body []byte, fromSelf bool) error
}

type nextDeliveryInfo struct {
	successCount  int
	errors        []error
	sentEndpoints []*messageBundleEndpoint
	nextEndpoints []*messageBundleEndpoint
	priority      uint8
	delaySec      uint64
}

func (ndi *nextDeliveryInfo) successful() bool {
	return ndi.successCount != 0
}

type (
	MembershipUpdater func(ids.ID, ids.ID, ids.ID, *Membership) error
	sender            func(*AddressedMessage) []error
	UpdateChannel     chan interface{}
	boolChannel       chan bool
	idChannel         chan ids.ID
)

type Manager struct {
	config                    *config.Config
	db                        *database
	sender                    sender
	transports                Transports
	log                       *zap.SugaredLogger
	processor                 processor
	clock                     clock.Clock
	groupAcks                 map[ids.ID]*GroupUpdate
	directMessageLock         sync.Mutex
	groupMessageLock          sync.Mutex
	finished                  sync.WaitGroup
	cancelFunc                context.CancelFunc
	pendingWaiting            bool
	pendingLock               sync.Mutex
	pending                   boolChannel
	backfillSourceReady       idChannel
	directMessage             boolChannel
	prekeySesssionsInitiation boolChannel
	deviceInvite              boolChannel
	dirtyMembershipAcks       boolChannel
	groupMessage              boolChannel
	lateDirectMessages        boolChannel
	groupUpdater              idChannel
	membershipDeactivator     boolChannel
	updates                   UpdateChannel
	messageBundle             idChannel
	backfillSinkCheck         idChannel
	prekeyUpdates             idChannel
	reportGroup               idChannel
	cleanGroupDescriptions    boolChannel
	backfillSourceJobs        chan *backfillSourceJob
	backfillInitator          boolChannel
	deferredPrekeyState       idChannel
	membershipUpdater         MembershipUpdater
	updatePending             boolChannel
}

func NewManager(config *config.Config, db *db.Database, t Transports, s sender, p processor, c clock.Clock, mu MembershipUpdater) (*Manager, error) {
	log := config.Logger("messaging/manager")
	d, err := newDatabase(db)
	if err != nil {
		return nil, fmt.Errorf("messaging: error making manager %w", err)
	}

	m := &Manager{
		sender:                    s,
		transports:                t,
		processor:                 p,
		clock:                     c,
		db:                        d,
		cancelFunc:                nil,
		config:                    config,
		log:                       log,
		membershipUpdater:         mu,
		pendingWaiting:            false,
		pending:                   make(boolChannel),
		groupAcks:                 make(map[ids.ID]*GroupUpdate),
		updates:                   make(UpdateChannel, 100),
		prekeyUpdates:             make(idChannel, 10),
		reportGroup:               make(idChannel, 10),
		directMessage:             make(boolChannel, 100),
		groupMessage:              make(boolChannel, 100),
		prekeySesssionsInitiation: make(boolChannel, 100),
		deviceInvite:              make(boolChannel, 100),
		lateDirectMessages:        make(boolChannel, 100),
		messageBundle:             make(idChannel, 100),
		groupUpdater:              make(idChannel, 100),
		membershipDeactivator:     make(boolChannel, 100),
		backfillSinkCheck:         make(idChannel, 100),
		dirtyMembershipAcks:       make(boolChannel, 100),
		backfillSourceReady:       make(idChannel, 100),
		backfillSourceJobs:        make(chan *backfillSourceJob, 100),
		cleanGroupDescriptions:    make(boolChannel),
		backfillInitator:          make(boolChannel, 100),
		deferredPrekeyState:       make(idChannel, 100),
		updatePending:             make(boolChannel, 100),
	}

	return m, nil
}

func (m *Manager) Start() error {
	if err := m.db.Run("reporting & creating device group", func() error {
		groups, err := m.db.groups()
		if err != nil {
			return err
		}
		for i := range groups {
			g := groups[i]
			m.db.AfterCommit(func() {
				m.reportGroup <- ids.ID(g.ID)
			})
		}

		deviceID := m.DeviceGroupID()
		if _, err := m.Group(deviceID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return m.CreateGroupWithID("Self", deviceID)
			}
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancelFunc = cancelFunc

	if err := m.resetMessageEndpoints(); err != nil {
		return err
	}
	if err := m.enqueueReadyMessageBundles(); err != nil {
		return err
	}
	if err := m.enqueueBackfillSources(); err != nil {
		return err
	}
	if err := m.enqueueDeferredPrekeyStates(); err != nil {
		return err
	}

	m.startPendingUpdater(ctx)
	m.startDelivery(ctx)
	m.startBundling(ctx)
	m.startDirtyAckProcessing(ctx)
	m.startGroupUpdater(ctx)
	m.startBackfiller(ctx)
	m.startBackfillSinkChecker(ctx)
	m.startGroupDescriptionCleaner(ctx)
	m.startPrekeySessionInitiator(ctx)
	m.startBackfillJobProcessor(ctx)
	m.startBackfillInitiator(ctx)
	m.startDeferredPrekeyStateProcessing(ctx)
	m.startMembershipDeactivator(ctx)
	m.startLateDirectMessageProcessor(ctx)
	m.startGroupReporter(ctx)

	go func() {
		m.pumpPendingUpdater()
		m.pumpMessageAvailability()
		m.pumpLateDirectMessages()
		m.initiateMembershipBackfills()
		m.initiateCleanGroupDescriptions()
		m.initiatePrekeySessions()
		m.initiateMembershipDeactivation()
	}()

	m.transports.StatusChanged(func(e string, s bool) {
		if !s {
			return
		}

		if err := m.db.Run(fmt.Sprintf("updating status for %s", e), func() error {
			changed, err := m.db.makeDirectMessageEndpointsReady(e)
			if err != nil {
				return err
			}
			if changed {
				m.pumpLateDirectMessages()
			}
			changed, err = m.db.makeMessageBundleEndpointsReady(e)
			if err != nil {
				return err
			}
			if changed {
				go func() {
					if err := m.db.Lock("re-enqueuing message bundles", func() error {
						return m.enqueueReadyMessageBundles()
					}); err != nil {
						m.log.Warnf("error while re-enqueuing message bundles %#v", err)
					}
				}()
			}
			return nil
		}); err != nil {
			m.log.Debugf(err.Error())
		}

	})
	return nil
}

func (m *Manager) Shutdown() (err error) {
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.finished.Wait()
	}
	return nil
}

func (m *Manager) ReportChange(groupID ids.ID, url string, active bool) error {
	g, err := m.db.group(groupID[:])
	if err != nil {
		return err
	}

	desc, err := m.db.groupDescription(g.DescDigest)
	if err != nil {
		return err
	}
	decoded, err := desc.decodeDescription()
	if err != nil {
		return err
	}
	selfIdentityID := ids.ID(g.SelfIdentityID)
	selfMembershipID := ids.ID(g.SelfMembershipID)

	if active {
		if identity, ok := decoded.Identities[selfIdentityID]; ok {
			if membership, ok := identity[selfMembershipID]; ok {
				endpoint, err := m.EndpointInfoForURL(url)
				if err != nil {
					return err
				}
				_, ok := membership.Description.Endpoints[url]
				if ok && membership.Description.Endpoints[url].Priority == endpoint.Priority {
					return nil
				}
				membership.Description.Endpoints[url] = endpoint
				membership.Description.Version++
				sig, err := signMembershipDesc(g.IntroKeyPriv, selfIdentityID, selfMembershipID, membership.Description)
				if err != nil {
					return err
				}
				membership.Signature = sig[:]
				decoded.Identities[selfIdentityID][selfMembershipID] = membership
				newDesc, newDigest, err := decoded.encode()
				if err != nil {
					return err
				}
				g.DescDigest = newDigest[:]

				if err := m.db.upsertGroup(g); err != nil {
					return err
				}
				groupd := &groupDescription{
					Digest: newDigest[:],
					Desc:   newDesc,
				}
				return m.db.insertGroupDescription(groupd)
			}
		}
		m.log.Warnf("no desc found while reporting group update")
		return nil
	}

	if identity, ok := decoded.Identities[selfIdentityID]; ok {
		if membership, ok := identity[selfMembershipID]; ok {
			if _, ok := membership.Description.Endpoints[url]; !ok {
				return nil
			}
			delete(membership.Description.Endpoints, url)
			membership.Description.Version++
			sig, err := signMembershipDesc(g.IntroKeyPriv, selfIdentityID, selfMembershipID, membership.Description)
			if err != nil {
				return err
			}
			membership.Signature = sig[:]
			decoded.Identities[selfIdentityID][selfMembershipID] = membership
			newDesc, newDigest, err := decoded.encode()
			if err != nil {
				return err
			}
			g.DescDigest = newDigest[:]

			if err := m.db.upsertGroup(g); err != nil {
				return err
			}
			groupd := &groupDescription{
				Digest: newDigest[:],
				Desc:   newDesc,
			}
			if err := m.db.insertGroupDescription(groupd); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) ProcessIncomingMessage(from string, body []byte) error {
	m.log.Infof("processing incoming message %s len=%d", from, len(body))
	envelope := &envelope{}
	if err := bencode.Deserialize(body, envelope); err != nil {
		if _, ok := err.(*bencode.DecodeError); ok {
			m.log.Warnf("error unmarshaling incoming message: %s", err)
			return nil
		}
		return err
	}

	m.log.Debugf("incoming message %s len=%d has type=%d", from, len(body), envelope.Type)
	switch envelope.Type {
	case envelopeRatchetMessage:
		m.log.Debugf("processing ratchet message from %s", from)
		rm, err := envelope.decodeRatchetMessage()
		if err != nil {
			m.log.Warnf("unable to decode ratchet message from %s: %#v", from, err)
			return nil
		}
		if err := m.processRatchetMessage(from, rm); err != nil {
			return fmt.Errorf("processing ratchet message from %s: %w", from, err)
		}
		return nil
	case envelopePrekey1:
		m.log.Debugf("processing prekey1 message from %s", from)
		pk1, err := envelope.decodePrekey1()
		if err != nil {
			m.log.Warnf("unable to decode prekey1 %#v", err)
			return nil
		}
		if err := m.processPrekey1(from, pk1); err != nil {
			return fmt.Errorf("processing prekey1 from %s: %w", from, err)
		}
		return nil
	case envelopePrekey2:
		m.log.Debugf("processing prekey2 message from %s", from)
		pk2, err := envelope.decodePrekey2()
		if err != nil {
			m.log.Warnf("unable to decode prekey2 %#v", err)
			return nil
		}
		if err := m.processPrekey2(pk2); err != nil {
			return fmt.Errorf("processing prekey2 from %s: %w", from, err)
		}
		return nil
	case envelopePrekey3:
		m.log.Debugf("processing prekey3 message from %s", from)
		pk3, err := envelope.decodePrekey3()
		if err != nil {
			m.log.Warnf("unable to decode prekey3 %#v", err)
			return nil
		}
		if err := m.processPrekey3(pk3); err != nil {
			return fmt.Errorf("processing prekey3 from %s: %w", from, err)
		}
		return nil
	case envelopePrekey4:
		m.log.Debugf("processing prekey4 message from %s", from)
		pk4, err := envelope.decodePrekey4()
		if err != nil {
			m.log.Warnf("unable to decode prekey3 %#v", err)
			return nil
		}
		if err := m.processPrekey4(pk4); err != nil {
			return fmt.Errorf("processing prekey3 from %s: %w", from, err)
		}
		return nil
	case envelopePrekey5:
		m.log.Debugf("processing prekey5 message from %s", from)
		pk5, err := envelope.decodePrekey5()
		if err != nil {
			m.log.Warnf("unable to decode prekey5 %#v", err)
			return nil
		}
		if err := m.processPrekey5(pk5); err != nil {
			return fmt.Errorf("processing prekey5 from %s: %w", from, err)
		}
		return nil
	case envelopeJpake2:
		m.log.Debugf("processing jpake2 from %s", from)
		jp2, err := envelope.decodeJpake2()
		if err != nil {
			m.log.Warnf("unable to decode jpake2 from %s: %#v", from, err)
			return nil
		}
		if err := m.processJpake2(jp2); err != nil {
			return fmt.Errorf("processing jpake2 %w", err)
		}
		return nil
	case envelopeJpake3:
		m.log.Debugf("processing jpake3 from %s", from)
		jp3, err := envelope.decodeJpake3()
		if err != nil {
			m.log.Warnf("unable to decode jpake3 from %s: %#v", from, err)
			return nil
		}
		if err := m.processJpake3(jp3); err != nil {
			return fmt.Errorf("processing jpake3 %w", err)
		}
		return nil
	case envelopeJpake4:
		m.log.Debugf("processing jpake4 from %s", from)
		jp4, err := envelope.decodeJpake4()
		if err != nil {
			m.log.Warnf("unable to decode jpake4 from %s: %#v", from, err)
			return nil
		}
		if err := m.processJpake4(jp4); err != nil {
			return fmt.Errorf("processing jpake4 %w", err)
		}
		return nil
	case envelopeJpake5:
		m.log.Debugf("processing jpake5 from %s", from)
		jp5, err := envelope.decodeJpake5()
		if err != nil {
			m.log.Warnf("unable to decode jpake5 from %s: %#v", from, err)
			return nil
		}
		if err := m.processJpake5(jp5); err != nil {
			return fmt.Errorf("processing jpake5 %w", err)
		}
		return nil
	case envelopeJpake6:
		m.log.Debugf("processing jpake6 from %s", from)
		jp6, err := envelope.decodeJpake6()
		if err != nil {
			m.log.Warnf("unable to decode jpake6 from %s: %#v", from, err)
			return nil
		}
		if err := m.processJpake6(jp6); err != nil {
			return fmt.Errorf("processing jpake6 %w", err)
		}
		return nil
	default:
		m.log.Warnf("unknown container type %d", envelope.Type)
		return nil
	}
}

func (m *Manager) CreateGroup(name string) (ids.ID, error) {
	groupID := ids.NewID()
	return groupID, m.CreateGroupWithID(name, groupID)
}

func (m *Manager) CreateGroupWithID(name string, id ids.ID) error {
	m.log.Infof("creating group with name=%s id=%x", name, id)
	identityID := ids.NewID()
	membershipID := ids.NewID()
	membership, privKey, err := m.generateGroupMembership(identityID, membershipID, id)
	if err != nil {
		return err
	}

	nowMs := m.clock.CurrentTimeMs()
	groupIdentities := make(IdentityMap)
	groupIdentities[identityID] = make(MembershipMap)
	groupIdentities[identityID][membershipID] = membership
	gd := &GroupDescription{
		Name:        &LWWString{name, nowMs},
		Description: &LWWString{},
		Icon:        &LWWBlob{},
		IconType:    &LWWString{},
		Identities:  groupIdentities,
	}

	gdb, digest, err := gd.encode()
	if err != nil {
		return err
	}

	groupd := groupDescription{
		Digest: digest[:],
		Desc:   gdb,
	}
	if err := m.db.insertGroupDescription(&groupd); err != nil {
		return err
	}
	group := &group{
		ID:                 id[:],
		DescDigest:         digest[:],
		SelfIdentityID:     identityID[:],
		SelfMembershipID:   membershipID[:],
		SourceIdentityID:   identityID[:],
		SourceMembershipID: membershipID[:],
		Seq:                0,
		State:              GroupStateSynced,
		IntroKeyPriv:       privKey[:],
	}
	m.db.AfterCommit(func() {
		m.reportGroup <- id
	})
	return m.upsertGroup(group)
}

func (m *Manager) Updates() UpdateChannel {
	return m.updates
}

func (m *Manager) Groups() ([]*Group, error) {
	groups, err := m.db.groups()
	if err != nil {
		return nil, err
	}

	outGroups := make([]*Group, len(groups))
	for i, g := range groups {
		desc, err := m.groupDescription(g)
		if err != nil {
			return nil, err
		}

		outGroups[i] = &Group{
			manager:          m,
			ID:               ids.IDFromBytes(g.ID),
			Name:             desc.Name.Value,
			AuthorTag:        g.authorTag(),
			SelfIdentityID:   ids.IDFromBytes(g.SelfIdentityID),
			SelfMembershipID: ids.IDFromBytes(g.SelfMembershipID),
			State:            g.State,
		}
	}
	return outGroups, nil
}

func (m *Manager) Group(id ids.ID) (*Group, error) {
	m.log.Infof("getting group id=%x", id)
	g, err := m.db.group(id[:])
	if err != nil {
		return nil, err
	}
	desc, err := m.groupDescription(g)
	if err != nil {
		return nil, err
	}

	return &Group{
		manager:          m,
		ID:               ids.IDFromBytes(g.ID),
		Name:             desc.Name.Value,
		AuthorTag:        g.authorTag(),
		SelfIdentityID:   ids.IDFromBytes(g.SelfIdentityID),
		SelfMembershipID: ids.IDFromBytes(g.SelfMembershipID),
		State:            g.State,
	}, nil
}

func (m *Manager) WaitForDelivery() error {
	var pending uint
	// check first if pending is at 0
	if err := m.db.Run("update pending", func() error {
		// get the lock from this point, because its safe given its inside a tx
		m.pendingLock.Lock()
		var err error
		pending, err = m.db.pendingMessageCount()
		return err
	}); err != nil {
		m.pendingLock.Unlock()
		return err
	}
	// if pending is 0, unlock and return
	if pending == 0 {
		m.pendingLock.Unlock()
		return nil
	}
	// otherwise, set pending to true, unlock and wait for the result
	m.pendingWaiting = true
	m.pendingLock.Unlock()
	<-m.pending
	return nil
}

func (m *Manager) InviteMember(groupID ids.ID, password string) (*Jpake1, error) {
	m.log.Infof("inviting member to group id=%x", groupID)
	s1priv := nacl.NewKey()
	s1pub := scalarmult.Base(s1priv)
	externalID := ids.NewID()
	jpake, err := initJpake(true, externalID[:], []byte(password))
	if err != nil {
		return nil, err
	}
	pass1, err := jpake.pass1Message()
	if err != nil {
		return nil, err
	}
	pass1.PublicKey = *s1pub
	replyEndpoints, err := m.endpointsForGroup(groupID)
	if err != nil {
		return nil, err
	}
	pass1.ReplyTo = replyEndpoints
	introID := ids.NewID()
	in := &intro{
		ID:            introID[:],
		ExistingGroup: true,
		GroupID:       groupID[:],
		Initiator:     true,
		InitialKey:    s1priv[:],
		PrivateKey:    s1priv[:],
		Stage:         1,
		Type:          IntroTypeJpake,
		StateID:       jpake.ID,
	}
	if err := m.db.upsertJpake(jpake); err != nil {
		return nil, err
	}

	m.log.Debugf("inserting intro for invite on group %x", groupID)
	if err := m.db.insertIntro(in); err != nil {
		return nil, err
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(in.GroupID),
			Initiator: in.Initiator,
			Stage:     in.Stage,
			Type:      in.Type,
		})
	})

	return pass1, nil
}

func (m *Manager) CancelInvites(groupID ids.ID) error {
	m.log.Infof("cancelling invites to group id=%x", groupID)
	return m.db.deleteIntrosByGroup(groupID[:])
}

func (m *Manager) ApproveJpakeInvite(jpake1 *Jpake1, password string) (ids.ID, error) {
	return m.ApproveJpakeInviteWithID(jpake1, password, nil)
}

func (m *Manager) ApproveJpakeInviteWithID(jpake1 *Jpake1, password string, existingGroupID *ids.ID) (ids.ID, error) {
	m.log.Infof("approving jpake invite with group id=%x", existingGroupID)
	var existingGroup bool
	var groupID ids.ID
	if existingGroupID == nil {
		groupID = ids.NewID()
		existingGroup = false
		m.log.Debugf("no id provided, generating a group id %x", groupID)
	} else {
		groupID = *existingGroupID
		existingGroup = true
	}

	m1priv := nacl.NewKey()
	m1pub := scalarmult.Base(m1priv)

	jpake, err := initJpake(false, jpake1.ID[:], []byte(password))
	if err != nil {
		return groupID, err
	}
	introID := ids.NewID()
	intro := &intro{
		ID:            introID[:],
		ExistingGroup: existingGroup,
		GroupID:       groupID[:],
		StateID:       jpake.ID,
		Initiator:     false,
		InitialKey:    jpake1.PublicKey[:],
		PeerPublicKey: jpake1.PublicKey[:],
		PrivateKey:    m1priv[:],
		Stage:         2,
		Type:          IntroTypeJpake,
	}

	m.log.Debugf("inserting intro for invite acceptable on group %x", groupID)
	if err := m.db.insertIntro(intro); err != nil {
		return groupID, err
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage,
			Type:      intro.Type,
		})
	})
	jpake2, err := jpake.processPass1Message(jpake1)
	if err != nil {
		return groupID, err
	}
	replyEndpoints, err := m.endpointsForGroup(groupID)
	if err != nil {
		return groupID, err
	}
	jpake2.ReplyTo = replyEndpoints
	jpake2.PublicKey = *m1pub
	if len(jpake1.ReplyTo) == 0 {
		return groupID, fmt.Errorf("expected at least one endpoint in ReplyTo, got 0")
	}
	if err := m.db.setJpakeEndpoints(jpake.ID, jpake1.ReplyTo); err != nil {
		return groupID, err
	}
	jpake.ExternalID = jpake1.ID[:]
	if err := m.db.upsertJpake(jpake); err != nil {
		return groupID, err
	}
	env, err := newEnvelopeWithJpake2(jpake2)
	if err != nil {
		return groupID, err
	}
	endpoints, err := m.db.jpakeEndpoints(jpake.ID)
	if err != nil {
		return groupID, err
	}
	if err := m.sendIntroResponse(intro, endpoints, env); err != nil {
		return groupID, err
	}
	return groupID, nil
}

func (m *Manager) GroupMembers(groupID ids.ID) (GroupMembership, error) {
	membershipMap := make(map[ids.ID][]ids.ID)
	memberships, err := m.db.groupMemberships(groupID[:])
	if err != nil {
		return nil, err
	}
	for _, membership := range memberships {
		identityID := ids.IDFromBytes(membership.IdentityID)
		membershipID := ids.IDFromBytes(membership.MembershipID)
		membershipMap[identityID] = append(membershipMap[identityID], membershipID)
	}
	i := 0
	gm := make(GroupMembership, len(membershipMap))
	for identityID, members := range membershipMap {
		gm[i] = &Member{
			ID:      identityID,
			Members: members,
		}
		i++
	}

	return gm, nil
}

func (m *Manager) SendGroupMessage(groupID ids.ID, body []byte) error {
	m.log.Infof("sending group message to %x len=%d", groupID, len(body))

	totalCount, err := m.db.groupMemberCount(groupID[:])
	if err != nil {
		return err
	}
	if totalCount == 0 {
		return nil
	}

	g, err := m.db.group(groupID[:])
	if err != nil {
		return fmt.Errorf("messaging: sending group message %w", err)
	}

	nextSeq := g.Seq + 1

	d, err := m.db.groupDescription(g.DescDigest)
	if err != nil {
		return err
	}
	desc, err := d.decodeDescription()
	if err != nil {
		return err
	}
	m.log.Debugf("sending to %d identities", len(desc.Identities))
	for identityID, memberships := range desc.Identities {
		for membershipID := range memberships {
			if identityID == ids.IDFromBytes(g.SelfIdentityID) && membershipID == ids.IDFromBytes(g.SelfMembershipID) {
				continue
			}
			sessions, err := m.db.sessions(g.ID, identityID[:], membershipID[:])
			if err != nil {
				return err
			}
			// FIXME: there are other states that would cause this to be unhandled
			if len(sessions) == 0 {
				ugmm := &unhandledGroupMessageMember{
					GroupID:      g.ID,
					Seq:          nextSeq,
					IdentityID:   identityID[:],
					MembershipID: membershipID[:],
				}

				if err := m.db.insertUnhandledGroupMessageMember(ugmm); err != nil {
					return err
				}
				continue
			}

			gmm := &groupMessageMember{
				GroupID:      g.ID,
				Seq:          nextSeq,
				IdentityID:   identityID[:],
				MembershipID: membershipID[:],
			}

			if err := m.db.upsertGroupMessageMember(gmm); err != nil {
				return err
			}
		}
	}
	message := &groupMessage{
		GroupID:    g.ID,
		Seq:        nextSeq,
		Body:       body,
		DescDigest: g.DescDigest,
	}

	if err := m.db.insertGroupMessage(message); err != nil {
		return err
	}

	g.Seq = nextSeq

	if err := m.upsertGroup(g); err != nil {
		return err
	}

	go m.markGroupMessageAvailable()

	return nil
}

func (m *Manager) DeviceGroupID() ids.ID {
	return ids.ID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
}

func (m *Manager) EndpointInfoForURL(u string) (*EndpointInfo, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case local.DigestScheme:
		return &EndpointInfo{
			Priority: 0,
		}, nil
	case heya.HeyaScheme:
		return &EndpointInfo{
			Priority: 10,
		}, nil
	default:
		return nil, fmt.Errorf("messaging: unknown scheme %s", parsed.Scheme)
	}
}

func (m *Manager) ApplyProposal(groupID, applierIdentityID, applierMembershipID, proposedMembershipID ids.ID, proposedMembership *Membership) error {
	m.log.Infof("applying proposal groupid=%x applier=%x:%x proposed membership id=%x", groupID, applierIdentityID, applierMembershipID, proposedMembershipID)

	group, err := m.db.group(groupID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			m.log.Debugf("skipping proposal application as group %x doesn't exist", groupID)
			return nil
		}

		return fmt.Errorf("messaging: applying proposal %w", err)
	}

	groupDesc, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return err
	}
	desc, err := groupDesc.decodeDescription()
	if err != nil {
		return err
	}
	if _, ok := desc.Identities[applierIdentityID][proposedMembershipID]; ok {
		m.log.Debugf("proposal already applied, skipping")
		return nil
	}

	desc.Identities[applierIdentityID][proposedMembershipID] = proposedMembership
	nextDescBytes, nextDescDigest, err := desc.encode()
	if err != nil {
		return err
	}

	nextGroupDesc := &groupDescription{
		Digest: nextDescDigest[:],
		Desc:   nextDescBytes,
	}

	if err := m.db.insertGroupDescription(nextGroupDesc); err != nil {
		return err
	}
	group.DescDigest = nextDescDigest[:]
	if err := m.upsertGroup(group); err != nil {
		return err
	}

	if _, err := m.processGroupDescription(groupID, ids.IDFromBytes(group.SelfIdentityID), ids.IDFromBytes(group.SelfMembershipID), nextDescDigest, desc, nil); err != nil {
		return err
	}
	return nil
}

func (m *Manager) sendRepairMessage(membership *groupMembership, fromIdentityID, fromMembershipID ids.ID, body *GroupMessageBody) error {
	m.log.Debugf("sending repair message on behalf of %x:%x with seq %d", fromIdentityID, fromMembershipID, body.Seq)
	return m.sendPrivateMessage(membership, privateMessageRepairMessage, &RepairBody{
		IdentityID:   fromIdentityID,
		MembershipID: fromMembershipID,
		Seq:          body.Seq,
		Body:         body.Body,
	})
}

func (m *Manager) sendPrivateMessage(membership *groupMembership, t uint8, message interface{}) error {
	body, err := bencode.Serialize(message)
	if err != nil {
		return err
	}
	return m.sendPrivateBody(membership, t, body)
}

func (m *Manager) sendPrivateBody(membership *groupMembership, t uint8, body []byte) error {
	m.log.Debugf("sending to %x %x:%x type=%d", membership.GroupID, membership.IdentityID, membership.MembershipID, t)

	pm := &privateMessage{
		GroupID:      membership.GroupID,
		IdentityID:   membership.IdentityID,
		MembershipID: membership.MembershipID,
		Seq:          membership.SendPrivateSeq,
		Type:         t,
		Body:         body,
	}

	if err := m.db.insertPrivateMessageAndIncrement(pm); err != nil {
		return err
	}
	m.db.AfterCommit(func() { m.markPrivateMessageAvailable() })
	return nil
}

func (m *Manager) startDeferredPrekeyStateProcessing(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case id := <-m.deferredPrekeyState:
				if err := m.db.Run(fmt.Sprintf("testing deferred prekey state %x", id), func() error {
					deferredPrekeyState, err := m.db.deferredPrekeyState(id[:])
					if err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							m.log.Warnf("skipping deferred prekey state, already deleted id=%x", id)
							return nil
						}
						return err
					}
					stage1PrekeyIntros, err := m.db.introsForTypeStage(IntroTypePrekey, 0)
					if err != nil {
						return err
					}
					m.log.Debugf("testing deferred prekey against %d intros", len(stage1PrekeyIntros))

					for _, intro := range stage1PrekeyIntros {
						g, err := m.db.group(intro.GroupID)
						if err != nil {
							return err
						}
						groupDesc, err := m.db.groupDescription(g.DescDigest)
						if err != nil {
							return err
						}
						desc, err := groupDesc.decodeDescription()
						if err != nil {
							return err
						}
						prekeyState, err := m.db.prekeyState(intro.StateID)
						if err != nil {
							return err
						}
						pk2, ok, err := prekeyState.processPrekey1(g, desc, deferredPrekeyState)
						if err != nil {
							return err
						}
						if !ok {
							m.log.Debugf("deferred prekey doesn't match")
							continue
						}
						membership, err := m.db.groupMembership(g.ID, prekeyState.OtherIdentityID, prekeyState.OtherMembershipID)
						if err != nil {
							return err
						}
						// if nonce is lower, ignore this
						if bytes.Compare(membership.LastPrekeyNonce, prekeyState.Nonce) != -1 {
							return m.db.deleteDeferredPrekeyState(id[:])
						}
						membership.LastPrekeyNonce = prekeyState.Nonce
						if err := m.db.upsertGroupMembership(membership); err != nil {
							return err
						}

						m.log.Infof("processed prekey1 nonce=%x", deferredPrekeyState.OtherNonce)
						m.log.Debugf("matched a deferred prekey went from %d to %d", intro.Stage, intro.Stage+2)
						m.log.Infof("moving intro from %d -> %d in prekey1", intro.Stage, intro.Stage+2)
						intro.Stage = 2
						if err := m.db.updateIntro(intro); err != nil {
							return err
						}
						if err := m.db.upsertPrekeyState(prekeyState); err != nil {
							return err
						}
						if err := m.db.deleteDeferredPrekeyState(id[:]); err != nil {
							return err
						}
						env, err := newEnvelopeWithPrekey2(pk2)
						if err != nil {
							return err
						}
						m.log.Infof("sending prekey2 nonce=%x", pk2.Nonce)
						return m.sendIntroResponse(intro, desc.Identities[ids.ID(prekeyState.OtherIdentityID)][ids.ID(prekeyState.OtherMembershipID)].Description.Endpoints, env)
					}
					m.log.Debugf("no deferred prekey matches detected")
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startMembershipDeactivator(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.membershipDeactivator:
				if err := m.db.Run("looking for memberships to deactivate", func() error {
					mems, err := m.db.groupMembershipsConnectionState(GroupMembershipConnectionStateDeactivating)
					if err != nil {
						return err
					}
					for _, mem := range mems {
						if err := m.db.deleteGroupMessageMembers(mem.GroupID, mem.IdentityID, mem.MembershipID); err != nil {
							return err
						}
						if err := m.db.deletePrivateMessages(mem.GroupID, mem.IdentityID, mem.MembershipID); err != nil {
							return err
						}
						// TODO: delete more things

						mem.ConnectionState = GroupMembershipConnectionStateDeactivated
						if err := m.db.upsertGroupMembership(mem); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startLateDirectMessageProcessor(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.lateDirectMessages:
				if err := m.db.Run("pump additional scheduled direct messages", func() error {
					lateDirectMessages, err := m.db.lateDirectMessages()
					if err != nil {
						return err
					}
					for i := range lateDirectMessages {
						dm := lateDirectMessages[i]
						go func() {
							time.Sleep(time.Until(time.UnixMilli(int64(dm.NextDeliveryAt)).Add(10 * time.Millisecond)))
							m.markDirectMessageAvailable()
						}()
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startGroupReporter(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case id := <-m.reportGroup:
				if err := m.db.Run("processing prekey message", func() error {
					return m.transports.ReportGroup(id)
				}); err != nil {
					m.log.Fatalf("error while reporting group %x: %#v", id, err)
				}
			}
		}
	}()
}

func (m *Manager) startBackfillInitiator(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.backfillInitator:
				// Kick off full backfill if needed
				if err := m.db.Run("initiate incomplete group backfills", func() error {
					groups, err := m.db.incompleteGroupBackfills()
					if err != nil {
						return err
					}

					for _, g := range groups {
						m.log.Debugf("attempting to initiate full backfill for %x", g.ID)
						membership, err := m.db.groupMembership(g.ID, g.SourceIdentityID, g.SourceMembershipID)
						if err != nil {
							if !errors.Is(err, sql.ErrNoRows) {
								return err
							}
							m.log.Debugf("no membership exists yet for this group, moving on")
							continue
						}

						groupDescription, err := m.db.groupDescription(g.DescDigest)
						if err != nil {
							return err
						}

						desc, err := groupDescription.decodeDescription()
						if err != nil {
							return err
						}

						if _, err := m.initiateFullBackfill(ids.IDFromBytes(*g.BackfillSinkID), g, membership, desc); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}

				if err := m.db.Run("initiate membership backfills", func() error {
					memberships, err := m.db.groupMembershipsBackfillState(GroupMembershipBackfillStateNew)
					if err != nil {
						return err
					}

					for _, membership := range memberships {
						id := ids.NewID()
						if _, err := m.initiatePartialBackfill(id, membership); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startBackfillJobProcessor(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case job := <-m.backfillSourceJobs:
				payload, nextMaxID, finished, err := m.processor.Backfill(job.GroupID, job.MaxID, job.Partial, job.FromSelf)
				if err != nil {
					m.log.Fatal(err)
					continue
				}
				if err := m.db.Run("sending backfill", func() error {
					backfillSource, err := m.db.backfillSource(job.BackfillSourceID[:])
					if err != nil {
						return err
					}
					membership, err := m.db.groupMembership(backfillSource.GroupID, backfillSource.IdentityID, backfillSource.MembershipID)
					if err != nil {
						return err
					}
					backfillSource.Total++

					if err := m.sendPrivateMessage(membership, privateMessageBackfillBody, &Backfill{
						ID:    ids.IDFromBytes(backfillSource.RequestID),
						Body:  payload,
						Total: backfillSource.Total,
					}); err != nil {
						return err
					}

					if finished {
						m.log.Debugf("backfill source completed backfill for %x %x:%x, finished count was %d", backfillSource.GroupID, backfillSource.IdentityID, backfillSource.MembershipID, backfillSource.Total)
						if err := m.db.deleteBackfillSource(backfillSource.ID); err != nil {
							return err
						}
						if err := m.sendPrivateMessage(membership, privateMessageBackfillComplete, &BackfillComplete{
							ID:    ids.IDFromBytes(backfillSource.RequestID),
							Total: backfillSource.Total,
						}); err != nil {
							return err
						}
					} else {
						backfillSource.MaxID = nextMaxID[:]
						err := m.db.upsertBackfillSource(backfillSource)
						if err != nil {
							return err
						}
						m.db.AfterCommit(func() {
							m.markBackfillReady(ids.IDFromBytes(backfillSource.ID))
						})
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startBackfiller(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case backfillSourceID := <-m.backfillSourceReady:
				err := m.db.Run("running backfill", func() error {
					m.log.Debugf("getting backfill source for %x", backfillSourceID)
					backfillSource, err := m.db.backfillSource(backfillSourceID[:])
					if err != nil {
						return err
					}
					m.backfillSourceJobs <- &backfillSourceJob{BackfillSourceID: ids.IDFromBytes(backfillSource.ID), GroupID: ids.IDFromBytes(backfillSource.GroupID), MaxID: ids.IDFromBytes(backfillSource.MaxID), Partial: backfillSource.Type == backfillRequestTypePartial, FromSelf: backfillSource.FromSelf}
					return nil
				})
				if err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startBundling(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.groupMessage:
				m.groupMessageLock.Lock()
				if err := m.db.Run("bundling messages", func() error {
					unbundledIdents, err := m.db.unbundledMessageIdentities()
					if err != nil {
						m.groupMessageLock.Unlock()
						return err
					}
					if len(unbundledIdents) == 0 {
						m.log.Debugf("no messages to bundle, stopping bundling")
						for len(m.groupMessage) > 0 {
							<-m.groupMessage
						}
						m.groupMessageLock.Unlock()
						return nil
					}

					m.groupMessageLock.Unlock()

					for _, ident := range unbundledIdents {
						g, err := m.db.group(ident.GroupID)
						if err != nil {
							return err
						}

						currentDesc, err := m.db.groupDescription(g.DescDigest)
						if err != nil {
							return err
						}
						decodedCurrentDesc, err := currentDesc.decodeDescription()
						if err != nil {
							return err
						}

						unbundledGroupMessageMembers, err := m.db.unbundledGroupMessageMembers(ident.GroupID, ident.IdentityID, ident.MembershipID)
						if err != nil {
							return err
						}

						unbundledPrivateMessages, err := m.db.unbundledPrivateMessages(ident.GroupID, ident.IdentityID, ident.MembershipID)
						if err != nil {
							return err
						}

						prevSendDescDigest, nextSendDescDigest, err := m.calculateSendDescDigest(g, decodedCurrentDesc, ident.IdentityID, ident.MembershipID)
						if err != nil {
							return err
						}

						bundleID := ids.NewID()
						bundle := &messageBundle{
							ID:                 bundleID[:],
							GroupID:            ident.GroupID,
							IdentityID:         ident.IdentityID,
							MembershipID:       ident.MembershipID,
							State:              MessageStateUndelivered,
							PrevSendDescDigest: prevSendDescDigest,
							NextSendDescDigest: nextSendDescDigest,
							From:               pickHighestURL(decodedCurrentDesc.Identities[ids.ID(g.SelfIdentityID)][ids.ID(g.SelfMembershipID)].Description.Endpoints),
						}

						for _, gmm := range unbundledGroupMessageMembers {
							gmm.BundleID = &bundle.ID
							if err := m.db.upsertGroupMessageMember(gmm); err != nil {
								return err
							}
						}

						for _, pm := range unbundledPrivateMessages {
							pm.BundleID = &bundle.ID
							if err := m.db.upsertPrivateMessage(pm); err != nil {
								return err
							}
						}

						if err := m.db.upsertMessageBundle(bundle); err != nil {
							return err
						}

						group, err := m.db.group(ident.GroupID)
						if err != nil {
							return err
						}

						groupd, err := m.db.groupDescription(group.DescDigest)
						if err != nil {
							return err
						}

						desc, err := groupd.decodeDescription()
						if err != nil {
							return err
						}

						identityID := ids.IDFromBytes(ident.IdentityID)
						membershipID := ids.IDFromBytes(ident.MembershipID)
						membership := desc.Identities[identityID][membershipID]
						available, err := m.insertMessageBundleEndpoints(bundleID, membership.Description.Endpoints)
						if err != nil {
							return err
						}
						if available {
							m.db.AfterCommit(func() {
								m.messageBundle <- bundleID
							})
						}
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startGroupUpdater(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case groupID := <-m.groupUpdater:
				if err := m.db.Run("re-calculating group acks", func() error {
					newGroupUpdate, err := m.GroupState(groupID)
					if err != nil {
						return err
					}

					if currentGroupUpdate, ok := m.groupAcks[groupID]; !ok || *newGroupUpdate != *currentGroupUpdate {
						m.log.Debugf("sending group update ack %#v", newGroupUpdate)
						m.groupAcks[groupID] = newGroupUpdate
						m.updates <- newGroupUpdate
					} else {
						m.log.Debugf("no update to send for ack %#v", newGroupUpdate)
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) GroupState(id ids.ID) (*GroupUpdate, error) {
	totalCount, err := m.db.groupMemberCount(id[:])
	if err != nil {
		return nil, err
	}
	connectedTotalCount, err := m.db.connectedGroupMemberCount(id[:])
	if err != nil {
		return nil, err
	}
	group, err := m.db.group(id[:])
	if err != nil {
		return nil, err
	}
	unbundledCount, err := m.db.unbundledCount(id[:])
	if err != nil {
		return nil, err
	}
	ackedMemberCount, err := m.db.ackedMemberCount(id[:], group.Seq)
	if err != nil {
		return nil, err
	}

	ga := &GroupUpdate{
		GroupID:              id,
		GroupState:           group.State,
		MemberCount:          totalCount,
		Seq:                  group.Seq,
		PendingMessageCount:  unbundledCount,
		AckedMemberCount:     ackedMemberCount,
		ConnectedMemberCount: connectedTotalCount,
	}
	return ga, nil
}

func (m *Manager) resetMessageEndpoints() error {
	return m.db.Run("reset undelivered messages", func() error {
		if err := m.db.updateGroupMembershipAckVersionsForUndelivered(); err != nil {
			return err
		}
		if err := m.db.updateMessageBundleEndpointStates(EndpointStatePreflightFailed, EndpointStateReady); err != nil {
			return err
		}
		if err := m.db.updateDirectMessageEndpointStates(EndpointStatePreflightFailed, EndpointStateReady); err != nil {
			return err
		}
		if err := m.db.updateMessageBundleEndpointStates(EndpointStateDelivering, EndpointStateReady); err != nil {
			return err
		}
		return m.db.updateDirectMessageEndpointStates(EndpointStateDelivering, EndpointStateReady)
	})
}

func (m *Manager) enqueueReadyMessageBundles() error {
	return m.db.Run("enqueuing ready message bundles", func() error {
		bundleIDs, err := m.db.readyMessageBundleIDs()
		if err != nil {
			return err
		}
		for i := range bundleIDs {
			id := bundleIDs[i]
			m.db.AfterCommit(func() {
				m.messageBundle <- ids.IDFromBytes(id)
			})
		}
		return nil
	})
}

func (m *Manager) enqueueBackfillSources() error {
	return m.db.Run("enqueuing ready backfill sources", func() error {
		sources, err := m.db.readyBackfillSources()
		if err != nil {
			return err
		}
		for _, source := range sources {
			m.markBackfillReady(ids.IDFromBytes(source.ID))
		}
		return nil
	})
}

func (m *Manager) startDirtyAckProcessing(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.dirtyMembershipAcks:
				if err := m.db.Run("pump dirty acks", func() error {
					groupMemberships, err := m.db.dirtyGroupMemberships()
					if err != nil {
						return err
					}

					if len(groupMemberships) == 0 {
						m.log.Debugf("got zero dirty group memberships, skipping")
						return nil
					}

					m.log.Debugf("Got %d dirty group memberships", len(groupMemberships))

					memberships := make(map[ids.ID]map[ids.ID]map[ids.ID]bool)

					for _, gm := range groupMemberships {
						groupID := ids.IDFromBytes(gm.GroupID)
						identityID := ids.IDFromBytes(gm.IdentityID)
						membershipID := ids.IDFromBytes(gm.MembershipID)
						if _, ok := memberships[groupID]; !ok {
							memberships[groupID] = make(map[ids.ID]map[ids.ID]bool)
						}
						if _, ok := memberships[groupID][identityID]; !ok {
							memberships[groupID][identityID] = make(map[ids.ID]bool)
						}
						memberships[groupID][identityID][membershipID] = true
					}

					for groupID, members := range memberships {
						if err := m.sendGroupMembershipAcks(groupID, members); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startBackfillSinkChecker(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case id := <-m.backfillSinkCheck:
				if err := m.db.Run("check backfill sink", func() error {
					m.log.Debugf("checking backfill sink %x for completion", id)
					bs, err := m.db.backfillSinkOrNil(id[:])
					if err != nil {
						return err
					}
					if bs == nil {
						return nil
					}
					var expectedTotal uint64
					if bs.ExpectedTotal != nil {
						expectedTotal = *bs.ExpectedTotal
					}

					m.log.Debugf("backfill sink status %x group_id=%x started=%#v completed=%#v expected=%d total=%d", id, bs.GroupID, bs.Started, bs.Completed, expectedTotal, bs.Total)
					if bs.Started && bs.Completed && bs.ExpectedTotal != nil && (*bs.ExpectedTotal) == bs.Total {
						m.log.Debugf("backfill sink %x complete", id)
						memberships, err := m.db.groupMembershipsForBackfillSinkID(bs.GroupID, bs.ID)
						if err != nil {
							return err
						}

						group, err := m.db.group(bs.GroupID)
						if err != nil {
							return fmt.Errorf("messaging: backfill sink checker %w", err)
						}

						for _, membership := range memberships {
							m.log.Debugf("processing membership %#v for backfill", membership)
							ack := newAcksFromBitmap(membership.SendGroupAckSeq, membership.SendGroupAckSparse)
							var sparse []byte
							if membership.BackfillSendGroupAckSeq == nil {
								m.log.Debugf("never got seq acks for this membership, skipping %x:%x", membership.IdentityID, membership.MembershipID)
								continue
							}
							if membership.BackfillSendGroupAckSparse != nil {
								sparse = *membership.BackfillSendGroupAckSparse
							}
							backfillAck := newAcksFromBitmap(*membership.BackfillSendGroupAckSeq, sparse)
							ack.update(backfillAck)

							membership.BackfillSinkID = nil
							membership.BackfillSendGroupAckSeq = nil
							membership.BackfillSendGroupAckSparse = nil
							membership.BackfillState = GroupMembershipBackfillStateSynced
							membership.SendGroupAckSeq = ack.seq
							membership.SendGroupAckSparse = ack.sparseBitmap()

							if err := m.db.upsertGroupMembership(membership); err != nil {
								return err
							}
						}

						group.BackfillSinkID = nil
						group.State = GroupStateSynced
						if err := m.upsertGroup(group); err != nil {
							return err
						}

						if err := m.db.deleteBackfillSink(bs.ID); err != nil {
							return err
						}

						m.db.AfterCommit(func() {
							m.enqueueGroupUpdate(ids.IDFromBytes(group.ID))
						})
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startGroupDescriptionCleaner(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.cleanGroupDescriptions:
				if err := m.db.Run("clean group descriptions", func() error {
					m.db.AfterCommit(func() {
						time.Sleep(1 * time.Hour)
						m.cleanGroupDescriptions <- true
					})

					return m.db.guardedDeleteGroupDescriptions()
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startPrekeySessionInitiator(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.prekeySesssionsInitiation:
				if err := m.InitiatePrekeySessions(); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) startDelivery(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.directMessage:
				if err := m.db.Run("delivering direct messages", func() error {
					m.directMessageLock.Lock()
					readyEndpoints, err := m.db.readyDirectMessageEndpoints()
					if err != nil {
						m.directMessageLock.Unlock()
						return err
					}

					if len(readyEndpoints) == 0 {
						m.log.Debugf("no ready direct message endpoints, stopping early")
						for len(m.directMessage) > 0 {
							<-m.directMessage
						}
						m.directMessageLock.Unlock()
						m.pumpLateDirectMessages()
						return nil
					}

					m.directMessageLock.Unlock()

					directMessageMap := make(map[ids.ID][]*directMessageEndpoint)
					for _, readyEndpoint := range readyEndpoints {
						directMessageMap[ids.IDFromBytes(readyEndpoint.MessageID)] = append(directMessageMap[ids.IDFromBytes(readyEndpoint.MessageID)], readyEndpoint)
					}

					for messageID, endpoints := range directMessageMap {
						message, err := m.db.directMessage(messageID[:])
						if err != nil {
							return err
						}

						am, err := m.AddressedMessageFromDirectMessage(message, endpoints)
						if err != nil {
							return err
						}
						return m.performDirectDelivery(messageID, am, endpoints)
					}
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			case messageBundleID := <-m.messageBundle:
				if err := m.db.Run(fmt.Sprintf("delivering message bundle %x", messageBundleID), func() error {
					m.log.Debugf("message bundle with id %x", messageBundleID)

					messageBundle, err := m.db.messageBundleOrNil(messageBundleID[:])
					if err != nil {
						m.log.Debugf("message bundle getting error %x %#v", messageBundleID, err)
						return err
					}
					if messageBundle == nil {
						m.log.Debugf("skipping message bundle with ids.ID %x, already deleted", messageBundleID)
						return nil
					}

					group, err := m.db.group(messageBundle.GroupID[:])
					if err != nil {
						return fmt.Errorf("messaging: deliverying message bundle %x %w", messageBundleID, err)
					}
					m.log.Debugf("message bundle with id %x being delivered to group %x", messageBundleID, group.ID)

					readyEndpoints, err := m.db.readyMessageBundleEndpoints(messageBundleID[:])
					if err != nil {
						return err
					}

					if len(readyEndpoints) == 0 {
						m.log.Debugf("no message bundle endpoints")
						return nil
					}

					groupMessages, err := m.db.groupMessagesForBundle(messageBundleID[:])
					if err != nil {
						return err
					}

					privateMessages, err := m.db.privateMessagesForBundle(messageBundleID[:])
					if err != nil {
						return err
					}

					gm, err := m.db.groupMembership(messageBundle.GroupID, messageBundle.IdentityID, messageBundle.MembershipID)
					if err != nil {
						return err
					}

					sessions, err := m.db.sessions(messageBundle.GroupID, messageBundle.IdentityID, messageBundle.MembershipID)
					if err != nil {
						return err
					}
					if len(sessions) == 0 {
						m.log.Debugf("cannot get session for %x %x:%x", messageBundle.GroupID, messageBundle.IdentityID, messageBundle.MembershipID)
						return nil
					}

					privateLost, err := m.lostPrivateMessages(gm)
					if err != nil {
						return err
					}

					groupLost, err := m.lostGroupMessages(group, gm)
					if err != nil {
						return err
					}

					active, err := m.membershipStillActive(group, gm, len(privateLost)+len(groupLost))
					if err != nil {
						return err
					}

					if !active {
						m.log.Debugf("removing group membership %x %x:%x due to inactivity", group.ID, gm.IdentityID, gm.MembershipID)
						if err := m.deactivateGroupMembership(group, gm); err != nil {
							m.log.Warnf("error while deactivating %#v", err)
						}
						return nil
					}

					i := 0
					lost := make([]*LostBody, len(privateLost)+len(groupLost))

					for _, message := range privateLost {
						lostPrivateMessage := PrivateMessage{message.Type, message.Body, message.Seq}
						body, err := bencode.Serialize(&lostPrivateMessage)
						if err != nil {
							return err
						}

						lost[i] = &LostBody{
							Type: lostBodyPrivate,
							Body: body,
						}
						i++
					}
					for _, message := range groupLost {
						unhandled, err := m.constructUnhandled(group.ID, message.Seq, message.DescDigest, group.DescDigest)
						if err != nil {
							return err
						}
						lostGroupMessage := GroupMessageBody{message.Body, message.Seq, unhandled}
						body, err := bencode.Serialize(&lostGroupMessage)
						if err != nil {
							return err
						}

						lost[i] = &LostBody{
							Type: lostBodyGroup,
							Body: body,
						}
						i++
					}
					m.log.Debugf("for bundle %x private lost=%d group lost=%d", messageBundleID, len(privateLost), len(groupLost))
					interiorPrivateMessages := make([]*PrivateMessage, len(privateMessages))
					for i, private := range privateMessages {
						m.log.Debugf("for bundle %x private %d:%d of type %d", messageBundleID, i, private.Seq, private.Type)
						interiorPrivateMessages[i] = &PrivateMessage{private.Type, private.Body, private.Seq}
					}

					var changesBytes, newDigest []byte
					m.log.Debugf("for bundle %x sending digest is %x -> %x, other side (%x:%x) has %x", messageBundleID, messageBundle.PrevSendDescDigest, messageBundle.NextSendDescDigest, gm.IdentityID, gm.MembershipID, gm.RecvDescDigest)
					if !bytes.Equal(messageBundle.PrevSendDescDigest, messageBundle.NextSendDescDigest) {
						sendDesc, err := m.db.groupDescription(messageBundle.NextSendDescDigest)
						if err != nil {
							return err
						}
						changesBytes = sendDesc.Desc
						newDigest = messageBundle.NextSendDescDigest
					}

					sendGroupAcks := newAcksFromBitmap(gm.SendGroupAckSeq, gm.SendGroupAckSparse)
					if gm.BackfillSendGroupAckSeq != nil {
						var sparse []byte
						if gm.BackfillSendGroupAckSparse != nil {
							sparse = *gm.BackfillSendGroupAckSparse
						}
						backfillGroupAcks := newAcksFromBitmap(*gm.BackfillSendGroupAckSeq, sparse)
						sendGroupAcks.update(backfillGroupAcks)
					}

					gm.SendAcksVersionSent = gm.SendAcksVersionWritten

					if err := m.db.upsertGroupMembership(gm); err != nil {
						return err
					}

					messageBodies := make([]*GroupMessageBody, len(groupMessages))
					for i, groupMessage := range groupMessages {
						unhandled, err := m.constructUnhandled(group.ID, groupMessage.Seq, groupMessage.DescDigest, group.DescDigest)
						if err != nil {
							return err
						}
						messageBodies[i] = &GroupMessageBody{groupMessage.Body, groupMessage.Seq, unhandled}
					}

					m.log.Debugf("message bundle %x delivering with %d group messages and %d private messages", messageBundleID, len(messageBodies), len(privateMessages))
					groupMessage := &GroupMessage{
						GroupMessages:    messageBodies,
						GroupAckSeq:      sendGroupAcks.seq,
						GroupAckSparse:   sendGroupAcks.sparseBitmap(),
						PrivateMessages:  interiorPrivateMessages,
						PrivateAckSeq:    gm.SendPrivateAckSeq,
						PrivateAckSparse: gm.SendPrivateAckSparse,
						BaseDigest:       gm.RecvDescDigest,
						NewDigest:        newDigest,
						GroupChanges:     changesBytes,
						Lost:             lost,
					}
					groupMessageBytes, err := bencode.Serialize(groupMessage)
					if err != nil {
						return err
					}

					session := sessions[0]
					encryptedMessage, err := m.encrypt(session, groupMessageBytes)
					if err != nil {
						return err
					}
					envelope, err := newEnvelopeWithRatchetMessage(encryptedMessage)
					if err != nil {
						return err
					}
					envelopeBytes, err := bencode.Serialize(envelope)
					if err != nil {
						return err
					}

					urls := make([]string, len(readyEndpoints))
					for i, e := range readyEndpoints {
						urls[i] = e.URL
					}

					am := &AddressedMessage{
						From: messageBundle.From,
						URLs: urls,
						Body: envelopeBytes,
					}

					return m.performBundledDelivery(messageBundle, am, readyEndpoints, messageBundle.DeleteAfterSuccess)
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) sendDirectMessage(introID []byte, endpoints map[string]*EndpointInfo, body []byte) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("message: no urls available to send direct message introid=%x", introID)
	}
	m.log.Infof("sending direct message with intro id=%x to endpoints=%#v", introID, endpoints)

	urls := make([]string, 0, len(endpoints))
	minPriority := uint8(255)
	for url := range endpoints {
		urls = append(urls, url)
	}

	available := false
	preflightResponse := m.transports.Preflight(urls)
	m.log.Debugf("preflight response on send direct %#v %#v", urls, preflightResponse)
	for i, u := range urls {
		if preflightResponse[i] && endpoints[u].Priority < minPriority {
			minPriority = endpoints[u].Priority
		}
	}

	messageID := ids.NewID()
	for i, u := range urls {
		var state uint8
		if preflightResponse[i] {
			if minPriority == endpoints[u].Priority {
				state = EndpointStateReady
				available = true
			} else {
				state = EndpointStateNotReady
			}
		} else {
			state = EndpointStatePreflightFailed
		}

		if err := m.db.upsertDirectMessageEndpoint(&directMessageEndpoint{
			MessageID: messageID[:],
			URL:       u,
			Priority:  endpoints[u].Priority,
			State:     state,
		}); err != nil {
			return fmt.Errorf("message: error sending group message %w", err)
		}
	}

	message := &directMessage{
		ID:      messageID[:],
		Body:    body,
		IntroID: introID,
		State:   MessageStateUndelivered,
		From:    "",
	}

	if err := m.db.upsertDirectMessage(message); err != nil {
		return fmt.Errorf("messaging: error sending direct message %w", err)
	}

	if available {
		m.db.AfterCommit(func() {
			m.markDirectMessageAvailable()
		})
	}
	return nil
}

func (m *Manager) processPrekey1(from string, pk1 *prekey1) error {
	m.log.Infof("processing prekey1 nonce=%x", pk1.Nonce)
	id := ids.NewID()
	m.db.AfterCommit(func() {
		m.deferredPrekeyState <- id
	})
	return m.db.upsertDeferredPrekeyState(&deferredPrekeyState{
		ID:             id[:],
		OtherNonce:     pk1.Nonce[:],
		OtherSig1:      pk1.Sig1[:],
		OtherPublicKey: pk1.PublicKey[:],
		FromURL:        from,
	})
}

func (m *Manager) processJpake2(jp2 *jpake2) error {
	m.log.Infof("processing jpake2 %x", jp2.ID[:])
	jpake, err := m.db.jpakeForExternalID(jp2.ID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			m.log.Infof("stop processing, no matching jpake2")
			return nil
		}
		return err
	}
	intro, err := m.db.introForStateTypeID(IntroTypeJpake, jpake.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 1 {
		m.log.Warnf("expected intro (jpake) to be in stage 1, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}
	pass3, err := jpake.processPass2Message(jp2)
	if err != nil {
		m.log.Warnf("error while processing pass2 message %#v", err)
		m.sendIntroFailed(intro)
		return nil
	}
	if err := m.db.upsertJpake(jpake); err != nil {
		return fmt.Errorf("messaging processing jpake2: %w", err)
	}
	if len(jp2.ReplyTo) == 0 {
		m.log.Warnf("no ReplyTo endpoints available, aborting")
		return m.cancelIntro(intro)
	}
	if err := m.db.setJpakeEndpoints(jpake.ID, jp2.ReplyTo); err != nil {
		return err
	}
	intro.PeerPublicKey = jp2.PublicKey[:]
	intro.Stage = 3
	if err = m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing jpake2: %w", err)
	}

	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage,
			Type:      intro.Type,
		})
	})

	env, err := newEnvelopeWithJpake3(pass3)
	if err != nil {
		return err
	}
	endpoints, err := m.db.jpakeEndpoints(jpake.ID)
	if err != nil {
		return err
	}
	return m.sendIntroResponse(intro, endpoints, env)
}

func (m *Manager) processJpake3(jp3 *jpake3) error {
	jpake, err := m.db.jpakeForExternalID(jp3.ID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	intro, err := m.db.introForStateTypeID(IntroTypeJpake, jpake.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 2 {
		m.log.Warnf("expected intro (jpake) to be in stage 2, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}
	conf1, err := jpake.processPass3Message(jp3)
	if err != nil {
		m.sendIntroFailed(intro)
		return nil
	}
	if err := m.db.upsertJpake(jpake); err != nil {
		return fmt.Errorf("messaging processing jpake3: %w", err)
	}

	intro.Stage = 4
	if err = m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing jpake3: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage,
			Type:      intro.Type,
		})
	})
	jpake4 := &jpake4{
		ID:      jp3.ID,
		Confirm: conf1,
	}
	env, err := newEnvelopeWithJpake4(jpake4)
	if err != nil {
		return err
	}
	endpoints, err := m.db.jpakeEndpoints(jpake.ID)
	if err != nil {
		return err
	}
	return m.sendIntroResponse(intro, endpoints, env)
}

func (m *Manager) processJpake4(jp4 *jpake4) error {
	jpake, err := m.db.jpakeForExternalID(jp4.ID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	intro, err := m.db.introForStateTypeID(IntroTypeJpake, jpake.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 3 {
		m.log.Warnf("expected intro (jpake) to be in stage 3, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}

	confirm2, err := jpake.processSessionConfirmation1(jp4.Confirm)
	if err != nil {
		m.log.Warnf("failed to confirm, ignoring")
		m.sendIntroFailed(intro)
		return nil
	}
	group, err := m.db.group(intro.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing jpake4: %w", err)
	}
	groupDescription, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging processing jpake4: %w", err)
	}
	var gd *GroupDescription
	err = bencode.Deserialize(groupDescription.Desc, &gd)
	if err != nil {
		if _, ok := err.(*bencode.DecodeError); ok {
			m.log.Warnf("messaging processing jpake4: %v", err)
			return nil
		}
		return fmt.Errorf("messaging processing jpake4: %w", err)
	}
	selfIdentityID := ids.ID(group.SelfIdentityID)
	selfMembershipID := ids.ID(group.SelfMembershipID)
	sig, err := signGroupDesc(group.IntroKeyPriv, selfIdentityID, selfMembershipID, gd)
	if err != nil {
		return err
	}
	ji := &jpakeInner{
		Sig:          sig,
		IdentityID:   selfIdentityID,
		MembershipID: selfMembershipID,
		Desc:         gd,
	}
	jiBytes, err := bencode.Serialize(ji)
	if err != nil {
		return err
	}
	e1e2 := *box.Precompute(nacl.Key(intro.PeerPublicKey[:]), nacl.Key(intro.PrivateKey))
	mac := hmac.New(sha256.New, e1e2[:])
	mac.Write([]byte(jpakeSecretKey1))
	k := mac.Sum(nil)
	innerBytes, err := crypto.EncryptWithKey(k, jiBytes, nil)
	if err != nil {
		return err
	}
	jp5 := &jpake5{
		ID:      jp4.ID,
		Confirm: confirm2,
		Inner:   innerBytes,
	}
	intro.Stage = 5
	if err := m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing jpake4: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage,
			Type:      intro.Type,
		})
	})
	env, err := newEnvelopeWithJpake5(jp5)
	if err != nil {
		return err
	}
	endpoints, err := m.db.jpakeEndpoints(jpake.ID)
	if err != nil {
		return err
	}
	return m.sendIntroResponse(intro, endpoints, env)
}

func (m *Manager) processJpake5(jp5 *jpake5) error {
	jpake, err := m.db.jpakeForExternalID(jp5.ID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	intro, err := m.db.introForStateTypeID(IntroTypeJpake, jpake.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 4 {
		m.log.Warnf("expected intro (jpake) to be in stage 4, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}

	if err := jpake.processSessionConfirmation2(jp5.Confirm); err != nil {
		m.sendIntroFailed(intro)
		return nil
	}
	if err := m.db.upsertJpake(jpake); err != nil {
		return err
	}
	// Modify description with self
	groupID := ids.IDFromBytes(intro.GroupID)
	var g *group
	var desc *GroupDescription
	var selfIdentityID, selfMembershipID ids.ID

	otherInner := &jpakeInner{}
	e1e2 := *box.Precompute(nacl.Key(intro.PeerPublicKey[:]), nacl.Key(intro.PrivateKey))
	otherMac := hmac.New(sha256.New, e1e2[:])
	otherMac.Write([]byte(jpakeSecretKey1))
	otherK := otherMac.Sum(nil)
	otherInnerBytes, err := crypto.DecryptWithKey(otherK, jp5.Inner, nil)
	if err != nil {
		return err
	}
	if err := bencode.Deserialize(otherInnerBytes, otherInner); err != nil {
		return err
	}
	ok, err := verifyGroupDesc(otherInner.Sig, otherInner.IdentityID, otherInner.MembershipID, otherInner.Desc)
	if err != nil {
		return err
	}
	if !ok {
		return m.cancelIntro(intro)
	}

	if intro.ExistingGroup {
		existingGroup, err := m.db.group(intro.GroupID)
		if err != nil {
			return err
		}
		existingGroupDesc, err := m.db.groupDescription(existingGroup.DescDigest)
		if err != nil {
			return err
		}
		existingDesc, err := existingGroupDesc.decodeDescription()
		if err != nil {
			return err
		}
		nextDesc, err := existingDesc.merge(otherInner.Desc)
		if err != nil {
			return err
		}
		nextDescBytes, nextDescDigest, err := nextDesc.encode()
		if err != nil {
			return err
		}
		nextGroupDesc := &groupDescription{
			Digest: nextDescDigest[:],
			Desc:   nextDescBytes,
		}
		existingGroup.DescDigest = nextDescDigest[:]
		if err := m.db.insertGroupDescription(nextGroupDesc); err != nil {
			return err
		}
		if err := m.db.upsertGroup(existingGroup); err != nil {
			return err
		}
		selfIdentityID = ids.ID(existingGroup.SelfIdentityID)
		selfMembershipID = ids.ID(existingGroup.SelfMembershipID)
		g = existingGroup
		desc = nextDesc
	} else {
		selfIdentityID = ids.NewID()
		selfMembershipID = ids.NewID()
		membership, privKey, err := m.generateGroupMembership(selfIdentityID, selfMembershipID, groupID)
		if err != nil {
			return err
		}
		nextDesc := otherInner.Desc
		if _, ok := nextDesc.Identities[selfIdentityID]; !ok {
			nextDesc.Identities[selfIdentityID] = make(MembershipMap)
		}
		nextDesc.Identities[selfIdentityID][selfMembershipID] = membership
		nextDescBytes, nextDescDigest, err := nextDesc.encode()
		if err != nil {
			return err
		}
		nextGroupDesc := &groupDescription{
			Digest: nextDescDigest[:],
			Desc:   nextDescBytes,
		}

		g = &group{
			ID:                 groupID[:],
			SelfIdentityID:     selfIdentityID[:],
			SelfMembershipID:   selfMembershipID[:],
			SourceIdentityID:   otherInner.IdentityID[:],
			SourceMembershipID: otherInner.MembershipID[:],
			DescDigest:         nextDescDigest[:],
			Seq:                0,
			State:              GroupStateProposed,
			IntroKeyPriv:       privKey[:],
		}

		if err := m.upsertGroup(g); err != nil {
			return fmt.Errorf("messaging processing jpake5: %w", err)
		}

		if err := m.db.insertGroupDescription(nextGroupDesc); err != nil {
			return err
		}
		desc = nextDesc
	}
	sig, err := signGroupDesc(g.IntroKeyPriv, selfIdentityID, selfMembershipID, desc)
	if err != nil {
		return err
	}
	ji := &jpakeInner{
		Sig:          sig,
		IdentityID:   selfIdentityID,
		MembershipID: selfMembershipID,
		Desc:         desc,
	}
	jiBytes, err := bencode.Serialize(ji)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, e1e2[:])
	mac.Write([]byte(jpakeSecretKey2))
	k := mac.Sum(nil)
	innerBytes, err := crypto.EncryptWithKey(k, jiBytes, nil)
	if err != nil {
		return err
	}

	jp6 := &jpake6{
		ID:    jp5.ID,
		Inner: innerBytes,
	}
	env, err := newEnvelopeWithJpake6(jp6)
	if err != nil {
		return err
	}
	endpoints, err := m.db.jpakeEndpoints(jpake.ID)
	if err != nil {
		return err
	}
	if err := m.sendIntroResponse(intro, endpoints, env); err != nil {
		return err
	}
	if err := m.finishGroupNegotiation(g, intro, jpake.SessionKey, otherInner.Desc, otherInner.IdentityID, otherInner.MembershipID); err != nil {
		return err
	}

	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage + 2,
			Type:      intro.Type,
		})
		m.updates <- &IntroSucceeded{
			IntroID: ids.IDFromBytes(intro.ID),
			GroupID: ids.IDFromBytes(g.ID),
		}
	})

	if err := m.db.deleteJpake(jpake.ID); err != nil {
		return fmt.Errorf("messaging processing jpake5: %w", err)
	}

	if err := m.db.deleteIntro(intro.ID); err != nil {
		return fmt.Errorf("messaging processing jpake5: %w", err)
	}

	return nil
}

func (m *Manager) processJpake6(jp6 *jpake6) error {
	jpake, err := m.db.jpakeForExternalID(jp6.ID[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	intro, err := m.db.introForStateTypeID(IntroTypeJpake, jpake.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 5 {
		m.log.Warnf("expected intro (jpake) to be in stage 5, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}

	g, err := m.db.group(intro.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing jpake6: %w", err)
	}

	otherInner := &jpakeInner{}
	e1e2 := *box.Precompute(nacl.Key(intro.PeerPublicKey[:]), nacl.Key(intro.PrivateKey))
	mac := hmac.New(sha256.New, e1e2[:])
	mac.Write([]byte(jpakeSecretKey2))
	otherK := mac.Sum(nil)
	otherInnerBytes, err := crypto.DecryptWithKey(otherK, jp6.Inner, nil)
	if err != nil {
		return err
	}
	if err := bencode.Deserialize(otherInnerBytes, otherInner); err != nil {
		return err
	}
	ok, err := verifyGroupDesc(otherInner.Sig, otherInner.IdentityID, otherInner.MembershipID, otherInner.Desc)
	if err != nil {
		return err
	}
	if !ok {
		return m.cancelIntro(intro)
	}

	lastDesc, err := m.db.groupDescription(g.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging processing jpake6: %w", err)
	}

	desc := &GroupDescription{}
	if err := bencode.Deserialize(lastDesc.Desc, desc); err != nil {
		return fmt.Errorf("messaging processing jpake6: %w", err)
	}

	if err := m.finishGroupNegotiation(g, intro, jpake.SessionKey, otherInner.Desc, otherInner.IdentityID, otherInner.MembershipID); err != nil {
		return err
	}

	if err := m.db.deleteJpake(jpake.ID); err != nil {
		return fmt.Errorf("messaging processing jpake6: %w", err)
	}

	m.db.AfterCommit(func() { m.initiateMembershipBackfills() })
	m.db.AfterCommit(func() {
		m.updates <- &IntroSucceeded{
			IntroID: ids.IDFromBytes(intro.ID),
			GroupID: ids.IDFromBytes(g.ID),
		}
	})
	return nil
}

func (m *Manager) processPrekey2(pk2 *prekey2) error {
	m.log.Debugf("processing prekey2 nonce=%x", pk2.Nonce)
	state, err := m.db.prekeyStateForNonce(pk2.Nonce[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			m.log.Debugf("no prekey2 found")
			return nil
		}
		return err
	}

	intro, err := m.db.introForStateTypeID(IntroTypePrekey, state.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 1 {
		m.log.Warnf("expected intro (prekey) to be in stage 1, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}

	group, err := m.db.group(intro.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}

	gd, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	desc, err := gd.decodeDescription()
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	endpoints := desc.Identities[ids.ID(state.OtherIdentityID)][ids.ID(state.OtherMembershipID)].Description.Endpoints
	if len(endpoints) == 0 {
		m.log.Warnf("no endpoints available for responding, aborting")
		return m.db.deleteIntro(intro.ID)
	}
	pk3, ok, err := state.processPrekey2(pk2)
	if err != nil {
		return err
	}
	if !ok {
		m.log.Warnf("messaging: unable to process prekey2: %#v", err)
		m.sendIntroFailed(intro)
		return nil
	}
	m.log.Infof("moving intro from %d -> %d in prekey2", intro.Stage, intro.Stage+2)
	intro.Stage = 3
	if err := m.db.upsertPrekeyState(state); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	if err := m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	m.log.Infof("sending prekey3 nonce=%x", pk3.Nonce)
	env, err := newEnvelopeWithPrekey3(pk3)
	if err != nil {
		return err
	}

	if err := m.sendIntroResponse(intro, endpoints, env); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(group.ID),
			Initiator: true,
			Stage:     intro.Stage,
			Type:      IntroTypePrekey,
		})
	})
	return nil
}

func (m *Manager) processPrekey3(pk3 *prekey3) error {
	m.log.Debugf("processing prekey3 nonce=%x", pk3.Nonce)
	state, err := m.db.prekeyStateForNonce(pk3.Nonce[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	intro, err := m.db.introForStateTypeID(IntroTypePrekey, state.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 2 {
		m.log.Warnf("expected intro (prekey) to be in stage 2, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}
	group, err := m.db.group(intro.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	gd, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	desc, err := gd.decodeDescription()
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	endpoints := desc.Identities[ids.ID(state.OtherIdentityID)][ids.ID(state.OtherMembershipID)].Description.Endpoints
	if len(endpoints) == 0 {
		m.log.Warnf("no endpoints available for responding, aborting")
		return m.db.deleteIntro(intro.ID)
	}
	pk4, ok, err := state.processPrekey3(pk3, desc)
	if err != nil {
		return err
	}
	if !ok {
		m.log.Warnf("prekey state did not correctly advance, deleting")
		m.sendIntroFailed(intro)
	}
	m.log.Infof("moving intro from %d -> %d in prekey2", intro.Stage, intro.Stage+2)
	intro.Stage = 4
	if err := m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	m.log.Infof("sending prekey4 nonce=%x", pk4.Nonce)
	env, err := newEnvelopeWithPrekey4(pk4)
	if err != nil {
		return err
	}
	if err := m.sendIntroResponse(intro, endpoints, env); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	return nil
}

func (m *Manager) processPrekey4(pk4 *prekey4) error {
	m.log.Debugf("processing prekey4 nonce=%x", pk4.Nonce)
	state, err := m.db.prekeyStateForNonce(pk4.Nonce[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	intro, err := m.db.introForStateTypeID(IntroTypePrekey, state.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 3 {
		m.log.Warnf("expected intro (prekey) to be in stage 3, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}
	group, err := m.db.group(intro.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	gd, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	desc, err := gd.decodeDescription()
	if err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	endpoints := desc.Identities[ids.ID(state.OtherIdentityID)][ids.ID(state.OtherMembershipID)].Description.Endpoints
	if len(endpoints) == 0 {
		m.log.Warnf("no endpoints available for responding, aborting")
		return m.db.deleteIntro(intro.ID)
	}
	pk5, otherDesc, ok, err := state.processPrekey4(pk4, desc)
	if err != nil {
		return err
	}
	if !ok {
		m.sendIntroFailed(intro)
		return nil
	}
	intro.Initiator = true
	intro.InitialKey = state.PrivKey
	if err := m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing prekey3: %w", err)
	}
	if err := m.finishGroupNegotiation(group, intro, state.sessionKey(), otherDesc, ids.IDFromBytes(state.OtherIdentityID), ids.IDFromBytes(state.OtherMembershipID)); err != nil {
		return err
	}
	if err := m.db.deletePrekeyState(state.ID); err != nil {
		return fmt.Errorf("messaging processing prekey3: %w", err)
	}
	m.log.Infof("sending prekey5 nonce=%x", pk5.Nonce)
	env, err := newEnvelopeWithPrekey5(pk5)
	if err != nil {
		return err
	}
	if err := m.sendIntroResponse(intro, endpoints, env); err != nil {
		return fmt.Errorf("messaging processing prekey2: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(group.ID),
			Initiator: true,
			Stage:     3,
			Type:      IntroTypePrekey,
		})
	})
	return nil
}

func (m *Manager) processPrekey5(pk5 *prekey5) error {
	m.log.Debugf("processing prekey3 nonce=%x", pk5.Nonce)
	state, err := m.db.prekeyStateForNonce(pk5.Nonce[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	intro, err := m.db.introForStateTypeID(IntroTypePrekey, state.ID)
	if err != nil {
		return err
	}
	if intro.Stage != 4 {
		m.log.Warnf("expected intro (prekey) to be in stage 4, got %d", intro.Stage)
		m.sendIntroFailed(intro)
		return nil
	}
	otherDesc, ok, err := state.processPrekey5(pk5)
	if err != nil {
		return err
	}
	if !ok {
		m.log.Warnf("prekey state did not correctly advance, deleting")
		m.sendIntroFailed(intro)
		return nil
	}
	group, err := m.db.group(state.GroupID)
	if err != nil {
		return fmt.Errorf("messaging processing prekey3: %w", err)
	}
	intro.Initiator = false
	intro.InitialKey = state.OtherPublicKey
	if err := m.db.updateIntro(intro); err != nil {
		return fmt.Errorf("messaging processing prekey3: %w", err)
	}
	if err := m.finishGroupNegotiation(group, intro, state.sessionKey(), otherDesc, ids.IDFromBytes(state.OtherIdentityID), ids.IDFromBytes(state.OtherMembershipID)); err != nil {
		return err
	}
	if err := m.db.deletePrekeyState(state.ID); err != nil {
		return fmt.Errorf("messaging processing prekey3: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(group.ID),
			Initiator: true,
			Stage:     3,
			Type:      IntroTypePrekey,
		})
	})
	return nil
}

func (m *Manager) sendIntroResponse(i *intro, endpoints map[string]*EndpointInfo, env *envelope) error {
	envelopeBytes, err := bencode.Serialize(env)
	if err != nil {
		return fmt.Errorf("messaging sending intro response: %w", err)
	}
	if err := m.sendDirectMessage(i.ID, endpoints, envelopeBytes); err != nil {
		return fmt.Errorf("messaging sending intro response: %w", err)
	}
	return nil
}

func (m *Manager) processRatchetMessage(from string, message *ratchetMessage) error {
	m.log.Debugf("processing ratchet message from %s of len %d", from, len(message.Body))
	sessions, err := m.db.sessionsByURL(from)
	if err != nil {
		return fmt.Errorf("messaging processing ratchet message: %w", err)
	}
	if len(sessions) == 0 {
		m.log.Debugf("deferring ratchet message from %s of len %d", from, len(message.Body))
		return m.deferRatchetMessage(from, message)
	}
	m.log.Debugf("attempting to match against %d sessions", len(sessions))
	for _, session := range sessions {
		decryptedBody, err := m.decrypt(session, message)
		if err != nil {
			m.log.Debugf("session was unable to decrypt with error %#v, moving on", err)
			continue
		}
		m.log.Debugf("successfully decrypted for group id %x", session.GroupID)
		var groupMessage GroupMessage
		if err := bencode.Deserialize(decryptedBody, &groupMessage); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("processing ratchet message: %#v", err)
				return nil
			}
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}
		group, err := m.db.group(session.GroupID)
		if err != nil {
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}
		groupd, err := m.db.groupDescription(group.DescDigest)
		if err != nil {
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}

		desc, err := groupd.decodeDescription()
		if err != nil {
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}
		sessionMembership, err := m.db.groupMembership(session.GroupID, session.IdentityID, session.MembershipID)
		if err != nil {
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}

		sessionGroupID := ids.IDFromBytes(session.GroupID)
		sessionIdentityID := ids.IDFromBytes(session.IdentityID)
		sessionMembershipID := ids.IDFromBytes(session.MembershipID)
		processedMemberships := newMembershipMap(m, ids.IDFromBytes(session.GroupID))
		processedMemberships.Set(sessionIdentityID, sessionMembershipID, sessionMembership)

		m.log.Debugf("interior digests are %x -> %x current group digest is %x", groupMessage.BaseDigest, groupMessage.NewDigest, group.DescDigest)

		if err := m.processMessageDesc(group, desc, session, &groupMessage, processedMemberships); err != nil {
			return err
		}

		// Process any new recv acks
		backfillMap := make(map[ids.ID]bool)
		ackedPrivateIDs := sessionMembership.recvPrivateAck.update(newAcksFromBitmap(groupMessage.PrivateAckSeq, groupMessage.PrivateAckSparse))
		ackedGroupIDs := sessionMembership.recvGroupAck.update(newAcksFromBitmap(groupMessage.GroupAckSeq, groupMessage.GroupAckSparse))

		for _, i := range ackedGroupIDs {
			if err := m.db.deleteGroupMessageMember(session.GroupID, session.IdentityID, session.MembershipID, i); err != nil {
				return fmt.Errorf("messaging processing ratchet message: %w", err)
			}
		}

		for _, i := range ackedPrivateIDs {
			if err := m.db.deletePrivateMessage(session.GroupID, session.IdentityID, session.MembershipID, i); err != nil {
				return fmt.Errorf("messaging processing ratchet message: %w", err)
			}
		}

		m.log.Debugf("processing private bodies len=%d", len(groupMessage.PrivateMessages))
		for i := range groupMessage.PrivateMessages {
			if sessionMembership.sendPrivateAck.add(groupMessage.PrivateMessages[i].Seq) {
				if err := m.processPrivateBody(group, groupMessage.PrivateMessages[i], session, processedMemberships, backfillMap); err != nil {
					return err
				}
			}
		}

		// Process interior lost messages.
		for _, lost := range groupMessage.Lost {
			switch lost.Type {
			case lostBodyPrivate:
				privateBody := PrivateMessage{}
				if err := bencode.Deserialize(lost.Body, &privateBody); err != nil {
					if _, ok := err.(*bencode.DecodeError); ok {
						m.log.Warnf("error while decoding message %v", err)
						return nil
					}
					return err
				}

				if sessionMembership.sendPrivateAck.add(privateBody.Seq) {
					if err := m.processPrivateBody(group, &privateBody, session, processedMemberships, backfillMap); err != nil {
						return err
					}
				}
			case lostBodyGroup:
				groupMessage := GroupMessageBody{}
				if err := bencode.Deserialize(lost.Body, &groupMessage); err != nil {
					return err
				}
				if sessionMembership.sendGroupAck.add(groupMessage.Seq) {
					groupMessage := &IncomingGroupMessage{
						GroupID:      ids.IDFromBytes(session.GroupID),
						IdentityID:   ids.IDFromBytes(session.IdentityID),
						MembershipID: ids.IDFromBytes(session.MembershipID),
						Seq:          groupMessage.Seq,
						Body:         groupMessage.Body,
					}
					if err := m.processIncoming(groupMessage); err != nil {
						return err
					}
				}
			}
		}

		// Process normal group messages.
		for _, message := range groupMessage.GroupMessages {
			if sessionMembership.sendGroupAck.add(message.Seq) {
				groupMessage := &IncomingGroupMessage{
					GroupID:      sessionGroupID,
					IdentityID:   sessionIdentityID,
					MembershipID: sessionMembershipID,
					Body:         message.Body,
					Seq:          message.Seq,
				}
				if err := m.processIncoming(groupMessage); err != nil {
					return err
				}
			} else {
				m.log.Debugf("skipping group message %#v", message)
			}
		}

		if err := m.db.guardedDeleteMessages(); err != nil {
			return fmt.Errorf("messaging processing ratchet message: %w", err)
		}

		repairCount := 0
		if len(groupMessage.GroupMessages) != 0 {
			for _, body := range groupMessage.GroupMessages {
				if len(body.UnhandledRecipients) == 0 {
					continue
				}

				m.log.Debugf("have body.UnhandledRecipients of %#v", body.UnhandledRecipients)

				for identityID, membershipIDs := range body.UnhandledRecipients {
					for _, membershipID := range membershipIDs {
						membership, err := processedMemberships.Load(identityID, membershipID)
						if err != nil {
							return err
						}
						if err := m.sendRepairMessage(membership, sessionIdentityID, sessionMembershipID, body); err != nil {
							return fmt.Errorf("messaging processing ratchet message: %w", err)
						}
						repairCount++
					}
				}
			}
			if repairCount > 0 {
				m.db.AfterCommit(func() {
					m.markPrivateMessageAvailable()
				})
			}
		}

		// Save any changed group memberships
		acksAvailable := false
		for identityID, memberships := range processedMemberships.memberships {
			for membershipID, mem := range memberships {
				recvChanged, sendChanged := mem.saveAcks()
				m.log.Debugf("%x:%x recvChanged %v sendChanged %v", identityID, membershipID, recvChanged, sendChanged)

				if sendChanged {
					mem.SendAcksVersionWritten++
					acksAvailable = true
				}

				m.log.Debugf("saving membership for %x %x:%x %d", mem.GroupID, mem.IdentityID, mem.MembershipID, mem.SendPrivateSeq)

				if err := m.db.upsertGroupMembership(mem); err != nil {
					return fmt.Errorf("messaging processing ratchet message: %w", err)
				}
			}
		}

		if acksAvailable {
			m.log.Debugf("making acks available")
			m.db.AfterCommit(func() {
				m.markAcksAvailable()
			})
		}

		for id := range backfillMap {
			m.backfillSinkCheck <- id
		}

		m.db.AfterCommit(func() {
			m.enqueueGroupUpdate(ids.IDFromBytes(group.ID))
		})

		m.log.Debugf("done processing ratchet message")
		return nil
	}
	// TODO: filter on from, should at least be in the session urls
	if err := m.deferRatchetMessage(from, message); err != nil {
		return err
	}
	m.log.Warnf("cannot find a matching identity for %s, deferring", from)
	return nil

}

func (m *Manager) processPrivateBody(g *group, pb *PrivateMessage, session *session, processedMemberships *membershipMap, backfillMap map[ids.ID]bool) error {
	groupID := ids.IDFromBytes(session.GroupID)
	sessionIdentityID := ids.IDFromBytes(session.IdentityID)
	sessionMembershipID := ids.IDFromBytes(session.MembershipID)

	sessionMembership := processedMemberships.Get(sessionIdentityID, sessionMembershipID)

	m.log.Debugf("processing private body type=%d", pb.Type)

	switch pb.Type {
	case privateMessageRepairMessage:
		var repairBody RepairBody
		if err := bencode.Deserialize(pb.Body, &repairBody); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding message %v", err)
				return nil
			}
			return err
		}

		groupMembership, err := processedMemberships.Load(repairBody.IdentityID, repairBody.MembershipID)
		if err != nil {
			return err
		}

		if groupMembership.sendGroupAck.add(repairBody.Seq) {
			incomingMessage := &IncomingGroupMessage{
				GroupID:      groupID,
				IdentityID:   repairBody.IdentityID,
				MembershipID: repairBody.MembershipID,
				Body:         repairBody.Body,
				Seq:          repairBody.Seq,
			}

			if err := m.processIncoming(incomingMessage); err != nil {
				return fmt.Errorf("messaging processing repair body: %w", err)
			}
		}
		return nil
	case privateMessageBackfillRequest:
		var b BackfillRequest
		if err := bencode.Deserialize(pb.Body, &b); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding message %v", err)
				return nil
			}
			return err
		}

		bsID := ids.NewID()
		if err := m.db.upsertBackfillSource(&backfillSource{
			ID:           bsID[:],
			RequestID:    b.ID[:],
			GroupID:      session.GroupID,
			IdentityID:   session.IdentityID,
			MembershipID: session.MembershipID,
			MaxID:        []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Type:         b.Type,
			FromSelf:     bytes.Equal(session.IdentityID, g.SelfIdentityID),
			Total:        0,
		}); err != nil {
			return err
		}

		group, err := m.db.group(session.GroupID)
		if err != nil {
			return fmt.Errorf("messaging processing backfill request: %w", err)
		}
		selfIdentityID := ids.IDFromBytes(group.SelfIdentityID)
		selfMembershipID := ids.IDFromBytes(group.SelfMembershipID)

		allMemberships, err := m.db.groupMemberships(session.GroupID)
		if err != nil {
			return fmt.Errorf("messaging processing backfill request: %w", err)
		}

		acks := GroupAcks{make(map[ids.ID]map[ids.ID]*Ack)}

		if b.Type == backfillRequestTypeFull {
			for _, membership := range allMemberships {
				identityID := ids.IDFromBytes(membership.IdentityID)
				membershipID := ids.IDFromBytes(membership.MembershipID)

				if sessionIdentityID == identityID && sessionMembershipID == membershipID {
					continue
				}

				if _, ok := acks.Acks[identityID]; !ok {
					acks.Acks[identityID] = make(map[ids.ID]*Ack)
				}

				acks.Acks[identityID][membershipID] = &Ack{membership.RecvGroupAckSeq, membership.RecvGroupAckSparse}
			}
		}

		acks.Acks[selfIdentityID] = make(map[ids.ID]*Ack)
		acks.Acks[selfIdentityID][selfMembershipID] = &Ack{group.Seq, []byte{}}

		if err := m.sendPrivateMessage(sessionMembership, privateMessageBackfillStart, &BackfillStart{b.ID, acks}); err != nil {
			return err
		}

		m.db.AfterCommit(func() {
			m.markPrivateMessageAvailable()
			m.markBackfillReady(bsID)
		})

		return nil
	case privateMessageBackfillStart:
		var b BackfillStart
		if err := bencode.Deserialize(pb.Body, &b); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("messaging processing backfill start: %v", err)
				return nil
			}
			return fmt.Errorf("messaging processing backfill start: %w", err)
		}

		backfillSink, err := m.db.backfillSinkOrNil(b.ID[:])
		if err != nil {
			m.log.Debugf("error in processing backfill start for %x %#v", b.ID, err)
			return fmt.Errorf("messaging processing backfill start: %w", err)
		}
		if backfillSink == nil {
			return nil
		}

		backfillSink.Started = true
		for identityID, memberships := range b.Acks.Acks {
			for membershipID, acks := range memberships {
				m.log.Debugf("setting membership for %x %x:%x", g.ID, identityID, membershipID)
				membership, err := processedMemberships.Load(identityID, membershipID)
				if err != nil {
					return fmt.Errorf("messaging processing backfill start: %w", err)
				}
				membership.BackfillSinkID = &backfillSink.ID
				membership.BackfillSendGroupAckSeq = &acks.LastSeqAck
				membership.BackfillSendGroupAckSparse = &acks.LastSparseAck

				m.log.Debugf("membership setting ack for %x:%x with ack %#v", identityID, membershipID, acks)
			}
		}

		backfillMap[b.ID] = true
		if err := m.db.upsertBackfillSink(backfillSink); err != nil {
			return fmt.Errorf("messaging processing backfill start: %w", err)
		}

		return nil
	case privateMessageBackfillBody:
		group, err := m.db.group(session.GroupID)
		if err != nil {
			return fmt.Errorf("messaging: processing backfill body %w", err)
		}

		m.log.Debugf("Processing backfill body")
		var b Backfill
		if err := bencode.Deserialize(pb.Body, &b); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding message %v", err)
				return nil
			}
			return fmt.Errorf("messaging processing backfill body: %w", err)
		}

		backfillSink, err := m.db.backfillSinkOrNil(b.ID[:])
		if err != nil {
			return fmt.Errorf("messaging processing backfill body: %w", err)
		}
		if backfillSink == nil {
			m.log.Debugf("no backfill exists for %x", b.ID[:])
			return nil
		}
		m.log.Debugf("backfill body has %d total", b.Total)

		if err := m.processor.ProcessBackfill(ids.IDFromBytes(session.GroupID), b.Body, bytes.Equal(session.IdentityID, group.SelfIdentityID)); err != nil {
			return fmt.Errorf("messaging processing backfill body: %w", err)
		}

		backfillSink.Total++
		if backfillSink.ExpectedTotal == nil || *backfillSink.ExpectedTotal < b.Total {
			backfillSink.ExpectedTotal = &b.Total
		}
		if err := m.db.upsertBackfillSink(backfillSink); err != nil {
			return err
		}
		backfillMap[b.ID] = true

		return nil
	case privateMessageBackfillComplete:
		m.log.Debugf("processing backfill complete")
		var b BackfillComplete
		if err := bencode.Deserialize(pb.Body, &b); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding message: %#v", err)
				return nil
			}
			return err
		}

		m.log.Debugf("processing backfill complete id=%x total=%d", b.ID, b.Total)

		backfillSink, err := m.db.backfillSinkOrNil(b.ID[:])
		if err != nil {
			return err
		}
		if backfillSink == nil {
			m.log.Debugf("no backfill exists for %x", b.ID[:])
			return nil
		}
		backfillSink.Completed = true
		backfillSink.ExpectedTotal = &b.Total
		if err := m.db.upsertBackfillSink(backfillSink); err != nil {
			return err
		}
		backfillMap[b.ID] = true
		return nil
	case privateMessageBackfillAborted:
		m.log.Debugf("processing backfill abort")
		var b Backfill
		if err := bencode.Deserialize(pb.Body, &b); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding message: %#v", err)
				return nil
			}
			return err
		}

		backfillSink, err := m.db.backfillSinkOrNil(b.ID[:])
		if err != nil {
			return err
		}

		if backfillSink == nil {
			m.log.Debugf("no backfill exists for %x", b.ID[:])
			return nil
		}
		memberships, err := m.db.groupMembershipsForBackfillSinkID(backfillSink.GroupID, backfillSink.ID)
		if err != nil {
			return err
		}

		group, err := m.db.group(backfillSink.GroupID)
		if err != nil {
			return fmt.Errorf("messaging processing backfill aborted: %w", err)
		}

		for _, membership := range memberships {
			identityID := ids.IDFromBytes(membership.IdentityID)
			membershipID := ids.IDFromBytes(membership.IdentityID)
			if processedMemberships.Has(identityID, membershipID) {
				membership = processedMemberships.Get(identityID, membershipID)
			}
			membership.BackfillSinkID = nil
			membership.BackfillSendGroupAckSeq = nil
			membership.BackfillSendGroupAckSparse = nil
			membership.BackfillState = GroupMembershipBackfillStateNew

			if err := m.db.upsertGroupMembership(membership); err != nil {
				return err
			}
		}

		group.BackfillSinkID = nil
		group.State = GroupStateNew
		if err := m.upsertGroup(group); err != nil {
			return err
		}

		if err := m.db.deleteBackfillSink(b.ID[:]); err != nil {
			return err
		}

		m.db.AfterCommit(func() { m.initiateMembershipBackfills() })

		return nil
	default:
		return fmt.Errorf("unrecognized private body type type=%d", pb.Type)
	}
}

func (m *Manager) InitiatePrekeySessions() error {
	err := m.db.Run("initiating prekey sessions", func() error {
		memberships, err := m.db.groupMembershipsConnectionState(GroupMembershipConnectionStateNew)
		if err != nil {
			return err
		}

		m.log.Debugf("have %d memberships that need prekey initiatation", len(memberships))

		for _, membership := range memberships {
			m.log.Debugf("membership %x %x:%x needs prekey initiatation", membership.GroupID, membership.IdentityID, membership.MembershipID)
			group, err := m.db.group(membership.GroupID)
			if err != nil {
				return fmt.Errorf("messaging: initing prekey %w", err)
			}
			targetIdentityID := ids.IDFromBytes(membership.IdentityID)
			targetMembershipID := ids.IDFromBytes(membership.MembershipID)
			initiator := false
			memberComaprison := ids.Compare(ids.IDFromBytes(group.SelfMembershipID), targetMembershipID)
			if memberComaprison == -1 {
				initiator = true
			} else if memberComaprison == 0 {
				initiator = ids.Compare(ids.IDFromBytes(group.SelfIdentityID), targetIdentityID) == -1
			}
			groupDescription, err := m.db.groupDescription(group.DescDigest)
			if err != nil {
				return err
			}
			desc, err := groupDescription.decodeDescription()
			if err != nil {
				return err
			}
			targetMembershipDesc := desc.Identities[targetIdentityID][targetMembershipID]
			if err := m.initiatePrekeySession(group, targetIdentityID, targetMembershipID, targetMembershipDesc, initiator); err != nil {
				return err
			}
			membership.ConnectionState = GroupMembershipConnectionStateInitiating
			if err := m.db.upsertGroupMembership(membership); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) initiatePrekeySessions() {
	m.prekeySesssionsInitiation <- true
}

func (m *Manager) initiateMembershipDeactivation() {
	m.membershipDeactivator <- true
}

func (m *Manager) initiatePrekeySession(g *group, identityID, membershipID ids.ID, membership *Membership, initiator bool) error {
	m.log.Infof("initiating prekey session group %x with %x:%x -> %x:%x", g.ID, g.SelfIdentityID, g.SelfMembershipID, identityID, membershipID)
	e1priv := nacl.NewKey()
	stateID := ids.NewID()
	nonce, err := newMonotonicNonce()
	if err != nil {
		return err
	}

	state := &prekeyState{
		ID:                stateID[:],
		Nonce:             nonce[:],
		Initiator:         initiator,
		PrivKey:           e1priv[:],
		OtherPublicKey:    []byte{},
		GroupID:           g.ID,
		IdentityID:        g.SelfIdentityID,
		MembershipID:      g.SelfMembershipID,
		OtherIdentityID:   identityID[:],
		OtherMembershipID: membershipID[:],
		OtherSigningKey:   membership.Description.IntroKey[:],
		PrivSigningKey:    g.IntroKeyPriv,
	}
	m.log.Debugf("inserting prekey state %x with groupid=%x nonce=%x", state.ID, state.GroupID, nonce)
	if err := m.db.upsertPrekeyState(state); err != nil {
		return fmt.Errorf("messaging: processing bootstrap prekey %w", err)
	}
	stage := uint32(0)
	if initiator {
		stage = 1
	}

	introID := ids.NewID()
	intro := &intro{
		ID:            introID[:],
		ExistingGroup: true,
		GroupID:       g.ID,
		StateID:       state.ID,
		Initiator:     initiator,
		InitialKey:    []byte{},
		PeerPublicKey: []byte{},
		PrivateKey:    e1priv[:],
		Stage:         stage,
		Type:          IntroTypePrekey,
	}

	m.log.Debugf("inserting intro for prekey init on group %x", g.ID)
	if err := m.db.insertIntro(intro); err != nil {
		return fmt.Errorf("messaging initiating prekey session: %w", err)
	}
	m.db.AfterCommit(func() {
		m.enqueueIntroUpdate(&IntroUpdate{
			GroupID:   ids.IDFromBytes(intro.GroupID),
			Initiator: intro.Initiator,
			Stage:     intro.Stage,
			Type:      intro.Type,
		})
	})

	if initiator {
		// generates h1 on prekey state
		pk1 := state.prekey1()
		m.log.Infof("sending prekey1 nonce=%x", pk1.Nonce)
		envelope, err := newEnvelopeWithPrekey1(pk1)
		if err != nil {
			return fmt.Errorf("message: error inviting member %w", err)
		}
		envelopeBytes, err := bencode.Serialize(envelope)
		if err != nil {
			return fmt.Errorf("message: error inviting member %w", err)
		}
		if err := m.sendDirectMessage(intro.ID, membership.Description.Endpoints, envelopeBytes); err != nil {
			return fmt.Errorf("messaging initiating prekey session: %w", err)
		}
	} else {
		for url := range membership.Description.Endpoints {
			states, err := m.db.deferredPrekeyStatesFrom(url)
			if err != nil {
				return err
			}
			for _, s := range states {
				func(id ids.ID) {
					m.db.AfterCommit(func() {
						m.deferredPrekeyState <- id
					})
				}(ids.ID(s.ID))
			}
		}
	}
	return nil
}

func (m *Manager) AddressedMessageFromDirectMessage(message *directMessage, dme []*directMessageEndpoint) (*AddressedMessage, error) {
	urls := make([]string, len(dme))

	for i, d := range dme {
		urls[i] = d.URL
	}

	return &AddressedMessage{
		From: message.From,
		URLs: urls,
		Body: message.Body,
	}, nil
}

func (m *Manager) AddMembership(groupID, identityID, membershipID ids.ID, membership *Membership) error {
	m.log.Infof("adding membership group_id=%x %x:%x", groupID, identityID, membershipID)
	g, err := m.db.group(groupID[:])
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return nil
	}
	groupd, err := m.db.groupDescription(g.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging adding membership: %w", err)
	}

	desc, err := groupd.decodeDescription()
	if err != nil {
		return fmt.Errorf("messaging adding membership: %w", err)
	}
	if _, ok := desc.Identities[identityID]; ok {
		if _, ok := desc.Identities[identityID][membershipID]; ok {
			// already added
			m.log.Debugf("skipping adding membership group_id=%x %x:%x", groupID, identityID, membershipID)
			return nil
		}
	} else {
		desc.Identities[identityID] = make(MembershipMap)
	}
	desc.Identities[identityID][membershipID] = membership

	newDescBytes, newDescDigest, err := desc.encode()
	if err != nil {
		return err
	}
	newDesc := &groupDescription{
		Digest: newDescDigest[:],
		Desc:   newDescBytes,
	}
	if err := m.db.insertGroupDescription(newDesc); err != nil {
		return fmt.Errorf("messaging adding membership: %w", err)
	}
	g.DescDigest = newDescDigest[:]
	if err := m.db.upsertGroup(g); err != nil {
		return fmt.Errorf("messaging adding membership: %w", err)
	}

	if _, err := m.processGroupDescription(groupID, ids.IDFromBytes(g.SelfIdentityID), ids.IDFromBytes(g.SelfMembershipID), newDescDigest, desc, nil); err != nil {
		return fmt.Errorf("messaging adding membership: %w", err)
	}
	m.log.Debugf("finished adding membership group_id=%x %x:%x", groupID, identityID, membershipID)
	return nil
}

func (m *Manager) ProposeMembership(groupID, identityID, otherMembershipID ids.ID, otherMembership *Membership) (ids.ID, *Membership, error) {
	// construct minimal description
	// upsert group
	// return new membership id

	membershipID := ids.NewID()
	membership, introKeyPriv, err := m.generateGroupMembership(identityID, membershipID, groupID)
	if err != nil {
		return membershipID, nil, fmt.Errorf("messaging proposing membership: %w", err)
	}
	m.log.Infof("proposing membership to group id=%x source membershipid=%x our membershipid=%x", groupID, otherMembershipID, membershipID)

	groupIdentities := make(IdentityMap)
	groupIdentities[identityID] = make(MembershipMap)
	groupIdentities[identityID][membershipID] = membership
	groupIdentities[identityID][otherMembershipID] = otherMembership
	gd := &GroupDescription{
		Name:        &LWWString{"", 0},
		Description: &LWWString{},
		Icon:        &LWWBlob{},
		IconType:    &LWWString{},
		Identities:  groupIdentities,
	}

	gdb, digest, err := gd.encode()
	if err != nil {
		return membershipID, nil, fmt.Errorf("messaging proposing membership: %w", err)
	}

	groupd := &groupDescription{
		Digest: digest[:],
		Desc:   gdb,
	}
	if err := m.db.insertGroupDescription(groupd); err != nil {
		return membershipID, nil, fmt.Errorf("messaging proposing membership: %w", err)
	}
	group := &group{
		ID:                 groupID[:],
		DescDigest:         digest[:],
		SelfIdentityID:     identityID[:],
		SelfMembershipID:   membershipID[:],
		SourceIdentityID:   identityID[:],
		SourceMembershipID: otherMembershipID[:],
		Seq:                0,
		State:              GroupStateProposed,
		IntroKeyPriv:       introKeyPriv[:],
	}
	if err := m.upsertGroup(group); err != nil {
		return membershipID, nil, fmt.Errorf("messaging proposing membership: %w", err)
	}
	if _, err := m.processGroupDescription(groupID, identityID, membershipID, digest, gd, nil); err != nil {
		return membershipID, nil, fmt.Errorf("messaging proposing membership: %w", err)
	}
	return membershipID, membership, nil
}

func (m *Manager) encrypt(session *session, body []byte) (*ratchetMessage, error) {
	drSession, err := doubleratchet.Load(session.ID, m.db.doubleratchetSessionStorage(), doubleratchet.WithCrypto(m.db.doubleratchetCrypto()), doubleratchet.WithKeysStorage(m.db.doubleratchetKeysStorage(session.ID)))
	if err != nil {
		return nil, fmt.Errorf("messaging encrypt: %w", err)
	}

	msg, err := drSession.RatchetEncrypt(body, nil)
	if err != nil {
		return nil, fmt.Errorf("messaging encrypt: %w", err)
	}
	rm := &ratchetMessage{
		Dh:   msg.Header.DH,
		N:    msg.Header.N,
		Pn:   msg.Header.PN,
		Body: msg.Ciphertext,
	}
	return rm, nil
}

func (m *Manager) decrypt(session *session, rm *ratchetMessage) ([]byte, error) {
	message := doubleratchet.Message{
		Header: doubleratchet.MessageHeader{
			DH: rm.Dh,
			N:  rm.N,
			PN: rm.Pn,
		},
		Ciphertext: rm.Body,
	}

	drSession, err := doubleratchet.Load(session.ID, m.db.doubleratchetSessionStorage(), doubleratchet.WithCrypto(m.db.doubleratchetCrypto()), doubleratchet.WithKeysStorage(m.db.doubleratchetKeysStorage(session.ID)))
	if err != nil {
		return nil, fmt.Errorf("messaging decrypt: %w", err)
	}
	msg, err := drSession.RatchetDecrypt(message, nil)
	if err != nil {
		return nil, fmt.Errorf("messaging decrypt: %w", err)
	}
	return msg, nil
}

func (m *Manager) cancelIntro(i *intro) error {
	m.sendIntroFailed(i)
	return m.db.deleteIntro(i.ID)
}

func (m *Manager) sendIntroFailed(i *intro) {
	m.updates <- &IntroFailed{
		IntroID: ids.IDFromBytes(i.ID),
		GroupID: ids.IDFromBytes(i.GroupID),
	}
}

func (m *Manager) markGroupMessageAvailable() {
	time.Sleep(time.Duration(m.config.GroupMessageWaitTimeMs) * time.Millisecond)
	m.groupMessageLock.Lock()
	m.groupMessage <- true
	m.groupMessageLock.Unlock()
}

func (m *Manager) markAcksAvailable() {
	time.Sleep(time.Duration(m.config.AckWaitTimeMs) * time.Millisecond)
	m.dirtyMembershipAcks <- true
}

func (m *Manager) markBackfillReady(id ids.ID) {
	m.backfillSourceReady <- id
}

func (m *Manager) markPrivateMessageAvailable() {
	time.Sleep(time.Duration(m.config.PrivateMessageWaitTimeMs) * time.Millisecond)
	m.groupMessageLock.Lock()
	m.groupMessage <- true
	m.groupMessageLock.Unlock()
}

func (m *Manager) markDirectMessageAvailable() {
	m.directMessageLock.Lock()
	m.directMessage <- true
	m.directMessageLock.Unlock()
}

func (m *Manager) pumpMessageAvailability() {
	m.directMessageLock.Lock()
	m.groupMessageLock.Lock()
	m.directMessage <- true
	m.groupMessage <- true
	m.directMessageLock.Unlock()
	m.groupMessageLock.Unlock()
}

func (m *Manager) pumpPendingUpdater() {
	m.updatePending <- true
}

func (m *Manager) sendGroupMembershipAcks(groupID ids.ID, memberships map[ids.ID]map[ids.ID]bool) error {
	group, err := m.db.group(groupID[:])
	if err != nil {
		return fmt.Errorf("messaging sending group membership acks: %w", err)
	}

	groupd, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return fmt.Errorf("messaging sending group membership acks: %w", err)
	}

	desc, err := groupd.decodeDescription()
	if err != nil {
		return fmt.Errorf("messaging sending group membership acks: %w", err)
	}

	for identityID, identityDesc := range desc.Identities {
		for membershipID, membership := range identityDesc {
			if _, ok := memberships[identityID]; !ok {
				continue
			}

			if _, ok := memberships[identityID][membershipID]; !ok {
				continue
			}

			prevSendDescDigest, nextSendDescDigest, err := m.calculateSendDescDigest(group, desc, identityID[:], membershipID[:])
			if err != nil {
				return err
			}

			messageBundleID := ids.NewID()
			bundle := &messageBundle{
				ID:                 messageBundleID[:],
				GroupID:            groupID[:],
				IdentityID:         identityID[:],
				MembershipID:       membershipID[:],
				DeleteAfterSuccess: true,
				State:              MessageStateUndelivered,
				PrevSendDescDigest: prevSendDescDigest,
				NextSendDescDigest: nextSendDescDigest,
				From:               pickHighestURL(desc.Identities[ids.ID(group.SelfIdentityID)][ids.ID(group.SelfMembershipID)].Description.Endpoints),
			}

			if err := m.db.upsertMessageBundle(bundle); err != nil {
				return fmt.Errorf("messaging sending group membership acks: %w", err)
			}

			m.log.Debugf("Adding group membership endpoints for %x:%x on bundle %x", identityID, membershipID, messageBundleID)
			available, err := m.insertMessageBundleEndpoints(messageBundleID, membership.Description.Endpoints)
			if err != nil {
				return fmt.Errorf("messaging sending group membership acks: %w", err)
			}

			if available {
				m.db.AfterCommit(func() {
					m.log.Debugf("adding message bundle ids.ID %x to channel", messageBundleID)
					m.messageBundle <- messageBundleID
				})
			}
		}
	}

	return nil
}

func (m *Manager) insertMessageBundleEndpoints(bundleID ids.ID, endpoints map[string]*EndpointInfo) (bool, error) {
	urls := make([]string, 0, len(endpoints))

	minPriority := uint8(255)
	for u := range endpoints {
		urls = append(urls, u)
	}
	preflightResponse := m.transports.Preflight(urls)
	m.log.Debugf("preflight response on bundled %#v %#v", urls, preflightResponse)
	available := false
	for i, url := range urls {
		endpoint := endpoints[url]
		if preflightResponse[i] && endpoint.Priority < minPriority {
			minPriority = endpoint.Priority
		}
	}

	for i, url := range urls {
		endpoint := endpoints[url]
		var state uint8
		if preflightResponse[i] {
			if endpoint.Priority == minPriority {
				state = EndpointStateReady
				available = true
			} else {
				state = EndpointStateNotReady
			}
		} else {
			state = EndpointStatePreflightFailed
		}

		bundleEndpoint := messageBundleEndpoint{
			BundleID: bundleID[:],
			URL:      url,
			Priority: endpoint.Priority,
			State:    state,
		}
		if err := m.db.upsertMessageBundleEndpoint(&bundleEndpoint); err != nil {
			return available, fmt.Errorf("messaging insert message bundle endpoints: %w", err)
		}
	}
	return available, nil
}

func (m *Manager) processIncoming(message *IncomingGroupMessage) error {
	m.log.Debugf("processing incoming message %x %x:%x (%d) of len=%d", message.GroupID, message.IdentityID, message.MembershipID, message.Seq, len(message.Body))
	return m.processor.Incoming(message)
}

func (m *Manager) enqueueGroupUpdate(groupID ids.ID) {
	m.log.Debugf("enqueing group update for group %x", groupID)
	m.groupUpdater <- groupID
}

func (m *Manager) enqueueIntroUpdate(u *IntroUpdate) {
	m.updates <- u
}

func (m *Manager) initiatePartialBackfill(id ids.ID, src *groupMembership) (*backfillSink, error) {
	backfill, err := m.initiateBackfillWithType(id, src, backfillRequestTypePartial)
	if err != nil {
		return nil, fmt.Errorf("messaging initiating partial backfill: %w", err)
	}

	src.BackfillState = GroupMembershipBackfillStateBackfilling
	src.BackfillSinkID = &backfill.ID
	src.BackfillSendGroupAckSeq = nil
	src.BackfillSendGroupAckSparse = nil

	if err := m.db.upsertGroupMembership(src); err != nil {
		return nil, fmt.Errorf("messaging initiating partial backfill: %w", err)
	}

	return backfill, nil
}

func (m *Manager) initiateFullBackfill(id ids.ID, g *group, src *groupMembership, desc *GroupDescription) (*backfillSink, error) {
	backfill, err := m.initiateBackfillWithType(id, src, backfillRequestTypeFull)
	if err != nil {
		return nil, fmt.Errorf("messaging initiating full backfill: %w", err)
	}

	selfIdentityID := ids.IDFromBytes(g.SelfIdentityID)
	selfMembershipID := ids.IDFromBytes(g.SelfMembershipID)

	for identityID, memberships := range desc.Identities {
		for membershipID := range memberships {
			if identityID == selfIdentityID && membershipID == selfMembershipID {
				continue
			}

			membership, err := m.db.groupMembership(src.GroupID, identityID[:], membershipID[:])
			if err != nil {
				return nil, fmt.Errorf("messaging initiating full backfill: %w", err)
			}

			membership.BackfillSinkID = &backfill.ID
			membership.BackfillState = GroupMembershipBackfillStateBackfilling
			membership.BackfillSendGroupAckSeq = nil
			membership.BackfillSendGroupAckSparse = nil

			if err := m.db.upsertGroupMembership(membership); err != nil {
				return nil, fmt.Errorf("messaging initiating full backfill: %w", err)
			}
		}
	}

	return backfill, nil
}

func (m *Manager) initiateBackfillWithType(id ids.ID, src *groupMembership, t uint8) (*backfillSink, error) {
	m.log.Debugf("initiating backfill request %x for group %x with %x:%x of type %d", id, src.GroupID, src.IdentityID, src.MembershipID, t)
	if err := m.sendPrivateMessage(src, privateMessageBackfillRequest, &BackfillRequest{id, t}); err != nil {
		return nil, err
	}
	bfs := &backfillSink{
		ID:           id[:],
		Type:         t,
		GroupID:      src.GroupID,
		IdentityID:   src.IdentityID,
		MembershipID: src.MembershipID,
		Started:      false,
		Total:        0,
	}

	if err := m.db.upsertBackfillSink(bfs); err != nil {
		return nil, err
	}

	return bfs, nil
}

func (m *Manager) initiateCleanGroupDescriptions() {
	m.cleanGroupDescriptions <- true
}

func (m *Manager) processGroupDescription(groupID, selfIdentityID, selfMembershipID ids.ID, digest [32]byte, desc *GroupDescription, defaultFn func(gm *groupMembership) error) (bool, error) {
	added := false
	prekeySessionsNeeded := false
	for descIdentityID, memberships := range desc.Identities {
		for descMembershipID, membershipDesc := range memberships {
			if descIdentityID == selfIdentityID && descMembershipID == selfMembershipID {
				continue
			}

			identityID := descIdentityID
			membershipID := descMembershipID

			defaultMembership := &groupMembership{
				GroupID:                    groupID[:],
				IdentityID:                 identityID[:],
				MembershipID:               membershipID[:],
				ConnectionState:            GroupMembershipConnectionStateNew,
				BackfillState:              GroupMembershipBackfillStateNew,
				BackfillSinkID:             &[]byte{},
				BackfillSendGroupAckSeq:    new(uint64),
				BackfillSendGroupAckSparse: &[]byte{},
				LastPrekeyNonce:            []byte{},
				SendPrivateSeq:             0,
				SendAcksVersionWritten:     0,
				SendAcksVersionSent:        0,
				SendGroupAckSeq:            0,
				SendGroupAckSparse:         []byte{},
				SendPrivateAckSeq:          0,
				SendPrivateAckSparse:       []byte{},
				RecvGroupAckSeq:            0,
				RecvGroupAckSparse:         []byte{},
				RecvPrivateAckSeq:          0,
				RecvPrivateAckSparse:       []byte{},
				RecvDescDigest:             digest[:],
				sendPrivateAck:             &acks{},
				sendGroupAck:               &acks{},
				recvPrivateAck:             &acks{},
				recvGroupAck:               &acks{},
			}
			defaultMembership.loadAcks()
			if defaultFn != nil {
				if err := defaultFn(defaultMembership); err != nil {
					return added, err
				}
			}

			mem, isNew, err := m.db.groupMembershipDefault(groupID[:], identityID[:], membershipID[:], defaultMembership)
			if err != nil {
				return added, err
			}

			if isNew {
				m.log.Debugf("inserting group membership %x:%x with digest %x", identityID, membershipID, digest)
				added = true
				if mem.ConnectionState == GroupMembershipConnectionStateNew {
					prekeySessionsNeeded = true
				}
				if err := m.db.upsertGroupMembership(mem); err != nil {
					return added, err
				}
			}

			// set sessions
			urls := make([]string, len(membershipDesc.Description.Endpoints))
			i := 0
			for url := range membershipDesc.Description.Endpoints {
				urls[i] = url
				i++
			}

			sessions, err := m.db.sessions(groupID[:], identityID[:], membershipID[:])
			if err != nil {
				return added, err
			}

			for _, session := range sessions {
				if err := m.db.setURLsForSession(session.ID, urls); err != nil {
					return added, err
				}
			}

			if err := m.db.setURLsForGroupMembership(groupID[:], identityID[:], membershipID[:], urls); err != nil {
				return added, err
			}
		}
	}
	if prekeySessionsNeeded {
		m.db.AfterCommit(func() {
			m.prekeySesssionsInitiation <- true
		})
	}
	return added, nil
}

func (m *Manager) deferRatchetMessage(from string, message *ratchetMessage) error {
	id := ids.NewID()
	nowMs := m.clock.CurrentTimeMs()
	messageBytes, err := bencode.Serialize(message)
	if err != nil {
		return err
	}
	if err := m.db.upsertDeferredMessage(&deferredMessage{id[:], from, messageBytes, nowMs}); err != nil {
		return err
	}

	return m.db.trimDeferredMessages(from)
}

func (m *Manager) processDeferredMessages(from []string) error {
	messages, err := m.db.deferredMessages(from)
	if err != nil {
		return fmt.Errorf("messaging processing deferred message: %w", err)
	}

	for _, message := range messages {
		rm := &ratchetMessage{}
		if err := bencode.Deserialize(message.Body, rm); err != nil {
			return fmt.Errorf("messaging processing deferred message: %w", err)
		}
		if err := m.processRatchetMessage(message.URL, rm); err != nil {
			m.log.Debugf("error processing deferred message: %#v", err)
			continue
		}
		if err := m.db.deleteDeferredMessage(message.ID); err != nil {
			return fmt.Errorf("messaging processing deferred message: %w", err)
		}
	}
	return nil
}

func (m *Manager) generateGroupMembership(identityID, membershipID, groupID ids.ID) (*Membership, ed25519.PrivateKey, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	defaultEndpoints, err := m.endpointsForGroup(groupID)
	if err != nil {
		return nil, nil, err
	}
	desc := &MembershipDescription{
		IntroKey:  [32]byte(publicKey),
		Version:   1,
		Protocol:  1,
		Endpoints: defaultEndpoints,
	}
	sig, err := signMembershipDesc(privateKey, identityID, membershipID, desc)
	if err != nil {
		return nil, nil, err
	}
	return &Membership{
		Description: desc,
		Signature:   sig[:],
	}, privateKey, nil
}

func (m *Manager) initiateMembershipBackfills() {
	m.backfillInitator <- true
}

func (m *Manager) upsertGroup(g *group) error {
	if err := m.db.upsertGroup(g); err != nil {
		return fmt.Errorf("messaging upserting group: %w", err)
	}

	m.db.AfterCommit(func() {
		m.enqueueGroupUpdate(ids.IDFromBytes(g.ID))
	})

	if m.membershipUpdater == nil {
		return nil
	}

	m.db.BeforeCommit(func() error {
		desc, err := m.db.groupDescription(g.DescDigest)
		if err != nil {
			return fmt.Errorf("messaging upserting group: %w", err)
		}

		decodedDesc, err := desc.decodeDescription()
		if err != nil {
			return fmt.Errorf("messaging upserting group: %w", err)
		}

		groupID := ids.IDFromBytes(g.ID)
		identityID := ids.IDFromBytes(g.SelfIdentityID)
		membershipID := ids.IDFromBytes(g.SelfMembershipID)

		return m.membershipUpdater(groupID, identityID, membershipID, decodedDesc.Identities[identityID][membershipID])
	})
	return nil
}

func (m *Manager) performDirectDelivery(messageID ids.ID, am *AddressedMessage, readyEndpoints []*directMessageEndpoint) error {
	m.log.Debugf("%x performing direct delivery to %d endpoints", messageID, len(readyEndpoints))

	m.db.AfterCommit(func() {
		m.pumpPendingUpdater()
	})

	for _, e := range readyEndpoints {
		m.log.Debugf("%x performing direct delivery to %s with priority %d", messageID, e.URL, e.Priority)
		e.State = EndpointStateDelivering
		if err := m.db.upsertDirectMessageEndpoint(e); err != nil {
			return fmt.Errorf("messaging performing direct delivery: %w", err)
		}
	}
	m.finished.Add(1)
	go func(messageID ids.ID, am *AddressedMessage, readyEndpoints []*directMessageEndpoint) {
		defer m.finished.Done()
		defer m.pumpPendingUpdater()
		errors := m.sender(am)
		m.log.Debugf("%x delivered message to=%#v len=%d with errors %#v", messageID, am.URLs, len(am.Body), errors)
		if err := m.db.Run(fmt.Sprintf("save direct delivery state %x", messageID), func() error {
			successCount := 0
			for i, e := range readyEndpoints {
				if errors[i] != nil {
					e.State = EndpointStateFailed
					e.LastError = errors[i].Error()
				} else {
					e.State = EndpointStateDelivered
					successCount++
				}
			}

			// at least one was successful, stop here
			if successCount != 0 {
				m.log.Debugf("%x delivery was successful", messageID)
				return m.db.deleteDirectMessage(messageID[:])
			}

			notReadyEndpoints, err := m.db.directMessageEndpointsByState(messageID[:], EndpointStateNotReady)
			if err != nil {
				return err
			}
			if len(notReadyEndpoints) != 0 {
				priority := uint8(255)
				for _, e := range notReadyEndpoints {
					m.log.Debugf("examining endpoint %s with priority %d and state %d", e.URL, e.Priority, e.State)
					if e.Priority < priority {
						priority = e.Priority
					}
				}
				for _, e := range notReadyEndpoints {
					if e.Priority == priority {
						m.log.Debugf("setting endpoint %s with priority %d to ready", e.URL, e.Priority)
						e.State = EndpointStateReady
						if err := m.db.upsertDirectMessageEndpoint(e); err != nil {
							return err
						}
					}
				}
				m.db.AfterCommit(func() {
					m.markDirectMessageAvailable()
				})
			} else {
				// rebuild endpoints and perform delivery again after a short period of time
				m.log.Debugf("no endpoints to deliver to, retrying")
				endpoints, err := m.db.directMessageEndpoints(messageID[:])
				if err != nil {
					return err
				}
				urls := make([]string, len(endpoints))
				minPriority := uint8(255)
				for i, e := range endpoints {
					urls[i] = e.URL
				}
				available := false
				preflightResponse := m.transports.Preflight(urls)
				m.log.Debugf("preflight response on dd retry %#v %#v", urls, preflightResponse)

				for i, e := range endpoints {
					if preflightResponse[i] && e.Priority < minPriority {
						minPriority = e.Priority
					}
				}

				for i, e := range endpoints {
					if preflightResponse[i] {
						if e.Priority == minPriority {
							m.log.Debugf("direct message retry, setting %s to ready", e.URL)
							e.State = EndpointStateReady
							available = true
						} else {
							m.log.Debugf("direct message retry, setting %s to not ready", e.URL)
							e.State = EndpointStateNotReady
						}
					} else {
						m.log.Debugf("direct message retry, setting %s to preflight failed", e.URL)
						e.State = EndpointStatePreflightFailed
					}
					if err := m.db.upsertDirectMessageEndpoint(e); err != nil {
						return fmt.Errorf("messaging performing direct delivery: %w", err)
					}
				}

				message, err := m.db.directMessage(messageID[:])
				if err != nil {
					return err
				}
				maxWait := 5 * time.Second
				t := (2 << message.DeliveryAttempts) * 100 * time.Millisecond
				if t > maxWait {
					t = maxWait
				}
				nextAt := time.Now().Add(t)
				message.NextDeliveryAt = uint64(nextAt.UnixMilli())
				message.DeliveryAttempts++
				if err := m.db.upsertDirectMessage(message); err != nil {
					return err
				}
				if available {
					m.db.AfterCommit(func() {
						time.Sleep(time.Until(nextAt) + 10*time.Millisecond)
						m.markDirectMessageAvailable()
					})

				}
			}
			return nil
		}); err != nil {
			m.log.Fatal(err)
		}
	}(messageID, am, readyEndpoints)
	return nil
}

func (m *Manager) performBundledDelivery(messageBundle *messageBundle, am *AddressedMessage, readyEndpoints []*messageBundleEndpoint, shouldDelete bool) error {
	m.log.Debugf("performing bundled delivery to %d endpoints", len(readyEndpoints))

	m.db.AfterCommit(func() {
		m.pumpPendingUpdater()
	})

	for _, e := range readyEndpoints {
		m.log.Debugf("performing bundled delivery to %s", e.URL)
		e.State = EndpointStateDelivering
		if err := m.db.upsertMessageBundleEndpoint(e); err != nil {
			return fmt.Errorf("messaging performing bundled delivery: %w", err)
		}
	}
	m.finished.Add(1)
	go func() {
		defer m.finished.Done()
		defer m.pumpPendingUpdater()
		errors := m.sender(am)
		if err := m.db.Run("save bundle delivery state", func() error {
			messageBundleID := messageBundle.ID
			messageBundle, err := m.db.messageBundleOrNil(messageBundleID)
			if err != nil {
				m.log.Debugf("message bundle getting error %x %#v", messageBundle.ID, err)
				return err
			}
			if messageBundle == nil {
				m.log.Debugf("skipping message bundle with ids.ID %x, already deleted", messageBundleID)
				return nil
			}

			identityID := ids.IDFromBytes(messageBundle.IdentityID)
			membershipID := ids.IDFromBytes(messageBundle.MembershipID)
			// build up information for the next delivery
			nextEndpointsMap := make(map[ids.ID]map[ids.ID]*nextDeliveryInfo)
			for i, e := range readyEndpoints {
				if _, ok := nextEndpointsMap[identityID]; !ok {
					nextEndpointsMap[identityID] = make(map[ids.ID]*nextDeliveryInfo)
				}
				if _, ok := nextEndpointsMap[identityID][membershipID]; !ok {
					nextEndpointsMap[identityID][membershipID] = &nextDeliveryInfo{
						successCount:  0,
						errors:        []error{},
						sentEndpoints: []*messageBundleEndpoint{},
						nextEndpoints: []*messageBundleEndpoint{},
						priority:      255,
						delaySec:      0,
					}
				}
				m.log.Debugf("delivery response %d:%#v with error %#v", i, e, errors[i])
				deliveryError := errors[i]
				ndi := nextEndpointsMap[identityID][membershipID]
				ndi.sentEndpoints = append(ndi.sentEndpoints, e)
				ndi.errors = append(ndi.errors, deliveryError)
				if deliveryError != nil {
					e.State = EndpointStateFailed
					e.LastError = deliveryError.Error()
				} else {
					ndi.successCount++
					e.State = EndpointStateDelivered
				}
				if err := m.db.upsertMessageBundleEndpoint(e); err != nil {
					return err
				}
			}

			notReadyEndpoints, err := m.db.messageBundleEndpoints(messageBundle.ID, EndpointStateNotReady)
			if err != nil {
				return err
			}
			available := false
			// put next endpoints into ndi
			for _, e := range notReadyEndpoints {
				ndi := nextEndpointsMap[identityID][membershipID]
				// If we experienced any success stop here

				if ndi.successful() {
					continue
				}
				if e.Priority < ndi.priority {
					ndi.priority = e.Priority
				}
				ndi.nextEndpoints = append(ndi.nextEndpoints, e)
			}

			acksAvailable := false

			for identityID, membership := range nextEndpointsMap {
				for membershipID, ndi := range membership {
					// These messages should be deleted no matter what, but maybe we want to make the group membership dirty again and allow an ack messages to be sent
					if shouldDelete {
						m.log.Debugf("deleting ack only message bundle %x for %x:%x", messageBundle.ID, identityID, membershipID)
						if err := m.db.guardDeleteMessageBundle(messageBundle.ID); err != nil {
							return err
						}
						// Here we make things dirty again because there was no successful sends
						if !ndi.successful() {
							m.log.Debugf("ack only message bundle %x for %x:%x wasn't successful however, dirtying membership", messageBundle.ID, identityID, membershipID)
							gm, err := m.db.groupMembership(messageBundle.GroupID, identityID[:], membershipID[:])
							if err != nil {
								return err
							}
							gm.SendAcksVersionWritten++
							if err := m.db.upsertGroupMembership(gm); err != nil {
								return err
							}
							acksAvailable = true
						}
						continue
					}

					// There was some success, so mark it successful and move on
					if ndi.successful() {
						m.log.Debugf("message bundle %x was successfully sent for %x:%x", messageBundle.ID, identityID, membershipID)
						messageBundle.State = MessageStateDelivered
						if err := m.db.upsertMessageBundle(messageBundle); err != nil {
							return err
						}
						continue
					}

					// No more endpoints left, so make it available again
					if len(ndi.nextEndpoints) == 0 {
						m.log.Debugf("message bundle %x for %x:%x has no available endpoints, de-bundling", messageBundle.ID, identityID, membershipID)
						if err := m.db.unsetBundleIDForGroupMessageMembers(messageBundle.ID); err != nil {
							return err
						}
						if err := m.db.unsetBundleIDForPrivateMessages(messageBundle.ID); err != nil {
							return err
						}
						if err := m.db.guardDeleteMessageBundle(messageBundle.ID); err != nil {
							return err
						}
						m.db.AfterCommit(func() {
							m.markGroupMessageAvailable()
						})
						continue
					}

					// There are more endpoints to try, so lets try them
					m.log.Debugf("message bundle %x for %x:%x was not successfully sent, more endpoints to try", messageBundle.ID, identityID, membershipID)
					for _, next := range ndi.nextEndpoints {
						if next.Priority != ndi.priority {
							continue
						}
						m.log.Debugf("message bundle %x for %x:%x setting %s to ready", messageBundle.ID, identityID, membershipID, next.URL)
						available = true
						next.State = EndpointStateReady
						if err := m.db.upsertMessageBundleEndpoint(next); err != nil {
							return err
						}
					}
				}
			}
			if available {
				m.db.AfterCommit(func() {
					m.messageBundle <- ids.IDFromBytes(messageBundle.ID)
				})
			}
			if acksAvailable {
				m.db.AfterCommit(func() {
					m.markAcksAvailable()
				})
			}
			return nil
		}); err != nil {
			m.log.Fatal(err)
		}
	}()
	return nil
}

func (m *Manager) groupDescription(group *group) (*GroupDescription, error) {
	gd, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return nil, err
	}

	desc, err := gd.decodeDescription()
	if err != nil {
		return nil, err
	}
	return desc, nil
}

func (m *Manager) lostPrivateMessages(membership *groupMembership) ([]*privateMessage, error) {
	if len(membership.recvPrivateAck.sparse) == 0 {
		return nil, nil
	}
	pms := make([]*privateMessage, 0, membership.SendPrivateSeq-membership.recvPrivateAck.seq)
	for i := membership.recvPrivateAck.seq + 1; i <= membership.SendPrivateSeq; i++ {
		if membership.recvPrivateAck.sparse[i] {
			continue
		}
		message, err := m.db.privateMessage(membership.GroupID, membership.IdentityID, membership.MembershipID, i)
		if err != nil {
			return nil, err
		}
		pms = append(pms, message)
	}
	return pms, nil
}

func (m *Manager) lostGroupMessages(g *group, mem *groupMembership) ([]*groupMessage, error) {
	if len(mem.recvGroupAck.sparse) == 0 {
		return nil, nil
	}
	gms := make([]*groupMessage, 0, g.Seq-mem.recvGroupAck.seq)
	for i := mem.recvGroupAck.seq + 1; i <= g.Seq; i++ {
		if mem.recvGroupAck.sparse[i] {
			continue
		}
		message, err := m.db.groupMessage(g.ID, i)
		if err != nil {
			return nil, err
		}
		gms = append(gms, message)
	}
	return gms, nil
}

func (m *Manager) membershipStillActive(g *group, mem *groupMembership, lostCount int) (bool, error) {
	if g.Seq-mem.RecvGroupAckSeq > 1000 || mem.SendPrivateSeq-mem.RecvPrivateAckSeq > 1000 {
		return false, nil
	}
	if lostCount > 1000 {
		return false, nil
	}
	return true, nil
}

func (m *Manager) deactivateGroupMembership(g *group, mem *groupMembership) error {
	desc, err := m.db.groupDescription(g.DescDigest)
	if err != nil {
		return err
	}
	decodedDesc, err := desc.decodeDescription()
	if err != nil {
		return err
	}
	identityID := ids.IDFromBytes(mem.IdentityID)
	membershipID := ids.IDFromBytes(mem.MembershipID)
	membership := decodedDesc.Identities[identityID][membershipID]
	membership.Description.Endpoints = make(map[string]*EndpointInfo)
	decodedDesc.Identities[identityID][membershipID] = membership

	newDesc, newDescDigest, err := decodedDesc.encode()
	if err != nil {
		return err
	}
	groupd := groupDescription{
		Digest: newDescDigest[:],
		Desc:   newDesc,
	}
	if err := m.db.insertGroupDescription(&groupd); err != nil {
		return err
	}
	mem.ConnectionState = GroupMembershipConnectionStateDeactivating
	g.DescDigest = newDescDigest[:]
	if err := m.upsertGroup(g); err != nil {
		return err
	}

	m.db.AfterCommit(func() {
		m.membershipDeactivator <- true
	})

	return nil
}

func (m *Manager) pumpLateDirectMessages() {
	m.lateDirectMessages <- true
}

func (m *Manager) enqueueDeferredPrekeyStates() error {
	return m.db.Run("enqueuing deferred prekey states", func() error {
		prekeyStates, err := m.db.deferredPrekeyStates()
		if err != nil {
			return err
		}
		for _, s := range prekeyStates {
			m.deferredPrekeyState <- ids.ID(s.ID)
		}
		return nil
	})
}

func (m *Manager) startPendingUpdater(ctx context.Context) {
	m.finished.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.finished.Done()
				return
			case <-m.updatePending:
				if err := m.db.Run("update pending", func() error {
					pending, err := m.db.pendingMessageCount()
					if err != nil {
						m.log.Warnf("error while updating pending %#v", err)
						return err
					}
					// get lock pending channel
					// if in waiting mode && 0 send true, then put back into non-waiting mode
					// unlock
					m.pendingLock.Lock()
					if m.pendingWaiting && pending == 0 {
						m.pending <- true
						m.pendingWaiting = false
					}
					m.pendingLock.Unlock()
					return nil
				}); err != nil {
					m.log.Fatal(err)
				}
			}
		}
	}()
}

func (m *Manager) finishGroupNegotiation(group *group, intro *intro, sessionKey []byte, otherDesc *GroupDescription, otherIdentityID, otherMembershipID ids.ID) error {
	desc, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return err
	}
	decodedDesc, err := desc.decodeDescription()
	if err != nil {
		return err
	}
	decodedDesc, err = decodedDesc.merge(otherDesc)
	if err != nil {
		return err
	}

	previousDescBytes, previousDescDigest, err := otherDesc.encode()
	if err != nil {
		return err
	}

	nextDescBytes, nextDescDigest, err := decodedDesc.encode()
	if err != nil {
		return err
	}

	previousGroupDesc := &groupDescription{
		Digest: previousDescDigest[:],
		Desc:   previousDescBytes,
	}
	m.log.Debugf("inserting previous group description with digest %x", previousDescDigest[:])
	if err := m.db.insertGroupDescription(previousGroupDesc); err != nil {
		return err
	}

	if nextDescDigest != previousDescDigest {
		nextGroupDesc := &groupDescription{
			Digest: nextDescDigest[:],
			Desc:   nextDescBytes,
		}
		m.log.Debugf("inserting next group description with digest %x", nextDescDigest[:])
		if err := m.db.insertGroupDescription(nextGroupDesc); err != nil {
			return err
		}
	}

	group.DescDigest = nextDescDigest[:]
	if err := m.upsertGroup(group); err != nil {
		return err
	}

	var descProcessor func(*groupMembership) error
	if ids.IDFromBytes(group.ID) == m.DeviceGroupID() || group.State == GroupStateProposed {
		backfillID := ids.NewID()
		backfillIDSlice := backfillID[:]
		group.BackfillSinkID = &backfillIDSlice
		group.State = GroupStateBackfilling
		descProcessor = func(gm *groupMembership) error {
			identityID := ids.IDFromBytes(gm.IdentityID)
			membershipID := ids.IDFromBytes(gm.MembershipID)
			if identityID == otherIdentityID && membershipID == otherMembershipID {
				gm.RecvDescDigest = previousDescDigest[:]
				gm.ConnectionState = GroupMembershipConnectionStateConnected
				m.db.BeforeCommit(func() error {
					if _, err := m.initiateFullBackfill(backfillID, group, gm, otherDesc); err != nil {
						return fmt.Errorf("messaging finishing group negotiation: %w", err)
					}
					return nil
				})
			}
			gm.BackfillState = GroupMembershipBackfillStateNew
			if ids, ok := otherDesc.Identities[identityID]; ok {
				if _, ok := ids[membershipID]; ok {
					gm.BackfillState = GroupMembershipBackfillStateBackfilling
				}
			}
			gm.BackfillSinkID = &backfillIDSlice
			return nil
		}
	} else {
		mem, isNew, err := m.db.groupMembershipDefault(group.ID, otherIdentityID[:], otherMembershipID[:], &groupMembership{
			GroupID:                    group.ID,
			IdentityID:                 otherIdentityID[:],
			MembershipID:               otherMembershipID[:],
			ConnectionState:            GroupMembershipConnectionStateConnected,
			BackfillState:              GroupMembershipBackfillStateNew,
			BackfillSinkID:             &[]byte{},
			BackfillSendGroupAckSeq:    new(uint64),
			BackfillSendGroupAckSparse: &[]byte{},
			LastPrekeyNonce:            []byte{},
			SendPrivateSeq:             0,
			SendAcksVersionWritten:     0,
			SendAcksVersionSent:        0,
			SendGroupAckSeq:            0,
			SendGroupAckSparse:         []byte{},
			SendPrivateAckSeq:          0,
			SendPrivateAckSparse:       []byte{},
			RecvGroupAckSeq:            0,
			RecvGroupAckSparse:         []byte{},
			RecvPrivateAckSeq:          0,
			RecvPrivateAckSparse:       []byte{},
			RecvDescDigest:             []byte{},
			sendPrivateAck:             &acks{},
			sendGroupAck:               &acks{},
			recvPrivateAck:             &acks{},
			recvGroupAck:               &acks{},
		})
		if err != nil {
			return err
		}
		m.log.Debugf("setting %x %x:%x to have digest of %x", group.ID, otherIdentityID, otherMembershipID, group.DescDigest)
		mem.RecvDescDigest = previousDescDigest[:]
		mem.ConnectionState = GroupMembershipConnectionStateConnected
		if isNew {
			m.log.Debugf("inserting group membership for %x:%x -> %x:%x with digest %x", group.SelfIdentityID, group.SelfMembershipID, otherIdentityID, otherMembershipID, group.DescDigest)
		}
		if err := m.db.upsertGroupMembership(mem); err != nil {
			return err
		}
	}
	if _, err := m.processGroupDescription(ids.IDFromBytes(group.ID), ids.IDFromBytes(group.SelfIdentityID), ids.IDFromBytes(group.SelfMembershipID), [32]byte(group.DescDigest), decodedDesc, descProcessor); err != nil {
		return err
	}
	if err := m.establishGroup(group, intro, sessionKey, otherIdentityID, otherMembershipID); err != nil {
		return err
	}
	m.db.AfterCommit(func() { m.initiateMembershipBackfills() })
	return nil
}

func (m *Manager) establishGroup(group *group, intro *intro, sessionKey []byte, otherIdentityID, otherMembershipID ids.ID) error {
	intro.Finished = true
	if err := m.db.updateIntro(intro); err != nil {
		return err
	}

	m.log.Debugf("establishing group %x with %x:%x -> %x:%x", group.ID, group.SelfIdentityID, group.SelfMembershipID, otherIdentityID, otherMembershipID)
	sessionID := ids.NewID()
	session := session{
		ID:           sessionID[:],
		GroupID:      group.ID,
		IdentityID:   otherIdentityID[:],
		MembershipID: otherMembershipID[:],
	}

	m.log.Debugf("inserting session %#v", session)

	if err := m.db.insertSession(intro.Initiator, intro.InitialKey, &session, sessionKey); err != nil {
		return err
	}

	groupDescription, err := m.db.groupDescription(group.DescDigest)
	if err != nil {
		return err
	}

	desc, err := groupDescription.decodeDescription()
	if err != nil {
		return err
	}

	m.log.Debugf("description has %d Identities with description digest %x", len(desc.Identities), group.DescDigest)

	i := 0
	urls := make([]string, len(desc.Identities[otherIdentityID][otherMembershipID].Description.Endpoints))
	for url := range desc.Identities[otherIdentityID][otherMembershipID].Description.Endpoints {
		urls[i] = url
		i++
	}
	m.log.Debugf("setting urls for session %v", urls)

	if len(urls) == 0 {
		return errors.New("no URLs for establishing this session")
	}
	if err := m.db.setURLsForSession(sessionID[:], urls); err != nil {
		return err
	}
	m.db.AfterCommit(func() {
		m.enqueueGroupUpdate(ids.IDFromBytes(group.ID))
	})

	return m.processDeferredMessages(urls)
}

func (m *Manager) calculateSendDescDigest(g *group, gd *GroupDescription, identityID, membershipID []byte) ([]byte, []byte, error) {
	gm, err := m.db.groupMembership(g.ID, identityID, membershipID)
	if err != nil {
		return nil, nil, err
	}

	if !bytes.Equal(gm.RecvDescDigest, g.DescDigest) {
		desc, err := m.db.groupDescription(gm.RecvDescDigest)
		if err != nil {
			return nil, nil, err
		}
		decodedDesc, err := desc.decodeDescription()
		if err != nil {
			return nil, nil, err
		}
		newDesc, err := decodedDesc.merge(gd)
		if err != nil {
			return nil, nil, err
		}
		newDescBytes, newDescDigest, err := newDesc.encode()
		if err != nil {
			return nil, nil, err
		}

		if err := m.db.insertGroupDescription(&groupDescription{
			Digest: newDescDigest[:],
			Desc:   newDescBytes,
		}); err != nil {
			return nil, nil, err
		}

		return gm.RecvDescDigest[:], newDescDigest[:], nil
	}
	return g.DescDigest, g.DescDigest, nil
}

func (m *Manager) processMessageDesc(g *group, desc *GroupDescription, s *session, gm *GroupMessage, processedMemberships *membershipMap) error {
	m.log.Debugf("processing message desc changes base digest=%x len(new digest)=%d", gm.BaseDigest, len(gm.NewDigest))

	// no changes, do nothing
	if bytes.Equal(g.DescDigest, gm.BaseDigest) && len(gm.NewDigest) == 0 {
		m.log.Debugf("no changes to process, group desc already in sync")
		return nil
	}

	sessionIdentityID := ids.IDFromBytes(s.IdentityID)
	sessionMembershipID := ids.IDFromBytes(s.MembershipID)
	sessionMembership, err := processedMemberships.Load(sessionIdentityID, sessionMembershipID)
	if err != nil {
		return err
	}

	// check if the base digest can be loaded
	baseDesc, err := m.db.groupDescription(gm.BaseDigest)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		m.log.Warnf("no desc found for %x, skipping processing", gm.BaseDigest)
		return nil
	}
	decodedBaseDesc, err := baseDesc.decodeDescription()
	if err != nil {
		return err
	}

	// merge any updates if they can be found
	// and update the membership with the new desc
	if len(gm.NewDigest) != 0 {
		m.log.Debugf("new digest has changes %x", gm.NewDigest)
		var changes GroupDescription
		if err := bencode.Deserialize(gm.GroupChanges, &changes); err != nil {
			if _, ok := err.(*bencode.DecodeError); ok {
				m.log.Warnf("error while decoding description %v", err)
				return nil
			}
			return err
		}
		decodedBaseDesc, err = decodedBaseDesc.merge(&changes)
		if err != nil {
			return err
		}
		updatedDescBytes, updatedDescDigest, err := decodedBaseDesc.encode()
		if err != nil {
			return err
		}
		if !bytes.Equal(updatedDescDigest[:], gm.NewDigest) {
			return fmt.Errorf("digests didn't match! computed digest %x but given %x", updatedDescDigest, gm.NewDigest)
		}
		sessionMembership.RecvDescDigest = updatedDescDigest[:]
		if err := m.db.insertGroupDescription(&groupDescription{
			Desc:   updatedDescBytes,
			Digest: updatedDescDigest[:],
		}); err != nil {
			return err
		}
	}

	// merge the new desc into our existing desc and save it on the group
	newDesc, err := desc.merge(decodedBaseDesc)
	if err != nil {
		return err
	}
	newDescBytes, newDescDigest, err := newDesc.encode()
	if err != nil {
		return err
	}
	if err := m.db.insertGroupDescription(&groupDescription{
		Desc:   newDescBytes,
		Digest: newDescDigest[:],
	}); err != nil {
		return err
	}
	g.DescDigest = newDescDigest[:]
	if err := m.db.upsertGroup(g); err != nil {
		return err
	}

	groupID := ids.IDFromBytes(s.GroupID)
	selfIdentityID := ids.IDFromBytes(g.SelfIdentityID)
	selfMembershipID := ids.IDFromBytes(g.SelfMembershipID)

	if len(gm.GroupMessages) != 0 {
		m.log.Debugf("have %d group messages to check for repairs", len(gm.GroupMessages))

		// check for any repair messages that need to be sent
		// using the previous desc so we note differences
		for identityID, identity := range desc.Identities {
			for membershipID := range identity {
				if identityID == selfIdentityID && membershipID == selfMembershipID {
					continue
				}
				if identityID == sessionIdentityID && membershipID == sessionMembershipID {
					continue
				}

				if _, ok := decodedBaseDesc.Identities[identityID]; ok {
					if _, ok := decodedBaseDesc.Identities[identityID][membershipID]; ok {
						continue
					}
				}
				m.log.Debugf("loading %x:%x for repair message", identityID, membershipID)
				membership, err := processedMemberships.Load(identityID, membershipID)
				if err != nil {
					return err
				}
				for _, body := range gm.GroupMessages {
					m.log.Debugf("sending repair from desc discrepency from %x:%x -> %x:%x", sessionIdentityID, sessionMembershipID, identityID, membershipID)
					if err := m.sendRepairMessage(membership, sessionIdentityID, sessionMembershipID, body); err != nil {
						continue
					}
				}
			}
		}
	} else {
		m.log.Debugf("not checking for repair messages as there is no group messages")
	}

	if _, err := m.processGroupDescription(groupID, selfIdentityID, selfMembershipID, newDescDigest, newDesc, nil); err != nil {
		return err
	}
	return nil
}

func (m *Manager) endpointsForGroup(groupID ids.ID) (map[string]*EndpointInfo, error) {
	urls, err := m.transports.URLsForGroup(groupID)
	if err != nil {
		return nil, err
	}

	info := make(map[string]*EndpointInfo, len(urls))
	for _, u := range urls {
		i, err := m.EndpointInfoForURL(u)
		if err != nil {
			return nil, err
		}
		if i == nil {
			continue
		}
		info[u] = i
	}
	return info, nil
}

func (m *Manager) constructUnhandled(groupID []byte, seq uint64, atTimeDigest, currentDigest []byte) (map[ids.ID][]ids.ID, error) {
	atTimeDesc, err := m.db.groupDescription(atTimeDigest)
	if err != nil {
		return nil, err
	}
	currentDesc, err := m.db.groupDescription(currentDigest)
	if err != nil {
		return nil, err
	}
	atTimeDescDecoded, err := atTimeDesc.decodeDescription()
	if err != nil {
		return nil, err
	}
	currentDescDecoded, err := currentDesc.decodeDescription()
	if err != nil {
		return nil, err
	}
	unhandledMembers, err := m.db.unhandledGroupMessageMembers(groupID, seq)
	if err != nil {
		return nil, err
	}
	unhandled := make(map[ids.ID][]ids.ID)
	for identityID, membership := range currentDescDecoded.Identities {
		for membershipID := range membership {
			if _, ok := atTimeDescDecoded.Identities[identityID][membershipID]; ok {
				continue
			}
			if _, ok := unhandled[identityID]; !ok {
				unhandled[identityID] = make([]ids.ID, 0, 1)
			}
			unhandled[identityID] = append(unhandled[identityID], membershipID)
		}
	}
	for _, uhm := range unhandledMembers {
		identityID := ids.IDFromBytes(uhm.IdentityID)
		membershipID := ids.IDFromBytes(uhm.MembershipID)
		if _, ok := unhandled[identityID]; !ok {
			unhandled[identityID] = make([]ids.ID, 0, 1)
		}
		unhandled[identityID] = append(unhandled[identityID], membershipID)
	}
	for identityID := range unhandled {
		sort.Sort(ids.ByLexicographical(unhandled[identityID]))
	}
	return unhandled, nil
}

func pickHighestURL(e map[string]*EndpointInfo) string {
	highestURL := ""
	priority := uint8(0)
	for u, in := range e {
		if in.Priority < priority {
			continue
		}
		priority = in.Priority
		highestURL = u
	}
	return highestURL
}

func newMonotonicNonce() (externalID [16]byte, err error) {
	now := uint64(time.Now().UnixNano())
	binary.BigEndian.PutUint64(externalID[0:8], now)
	_, err = io.ReadFull(crypto_rand.Reader, externalID[8:])
	return
}
