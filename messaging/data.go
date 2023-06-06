package messaging

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kevinburke/nacl/scalarmult"
	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/crypto"
	"github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/migration"
	"github.com/status-im/doubleratchet"
)

const (
	// group states
	GroupStateNew         = 0
	GroupStateBackfilling = 1
	GroupStateSynced      = 2
	GroupStateProposed    = 3

	// group membership connection states
	GroupMembershipConnectionStateNew          = 0
	GroupMembershipConnectionStateInitiating   = 1
	GroupMembershipConnectionStateConnected    = 2
	GroupMembershipConnectionStateDeactivating = 3
	GroupMembershipConnectionStateDeactivated  = 4

	// group membership backfill states
	GroupMembershipBackfillStateNew         = 0
	GroupMembershipBackfillStateBackfilling = 1
	GroupMembershipBackfillStateSynced      = 2

	// bundle member states
	MessageStateUndelivered = 0
	MessageStateDelivered   = 1

	// endpoint states
	EndpointStateReady           = 0
	EndpointStateDelivering      = 1
	EndpointStateNotReady        = 2
	EndpointStateDelivered       = 3
	EndpointStateFailed          = 4
	EndpointStatePreflightFailed = 5

	IntroTypeJpake  = 0
	IntroTypePrekey = 1
)

type groupIdentity struct {
	GroupID      []byte `db:"group_id"`
	IdentityID   []byte `db:"identity_id"`
	MembershipID []byte `db:"membership_id"`
}

type intro struct {
	ID            []byte `db:"id"`
	ExistingGroup bool   `db:"existing_group"`
	GroupID       []byte `db:"group_id"`
	InitialKey    []byte `db:"initial_key"`
	Initiator     bool   `db:"initiator"`
	PrivateKey    []byte `db:"private_key"`
	PeerPublicKey []byte `db:"peer_public_key"`
	StateID       []byte `db:"state_id"`
	Stage         uint32 `db:"stage"`
	Type          uint32 `db:"type"`
	Finished      bool   `db:"finished"`
}

type session struct {
	ID           []byte `db:"id"`
	GroupID      []byte `db:"group_id"`
	IdentityID   []byte `db:"identity_id"`
	MembershipID []byte `db:"membership_id"`
}

type doubleratchetKey struct {
	PublicKey      []byte `db:"pub_key"`
	MessageKey     []byte `db:"message_key"`
	MessageNumber  uint   `db:"msg_num"`
	SessionID      []byte `db:"session_id"`
	SequenceNumber uint   `db:"seq_num"`
}

type doubleratchetState struct {
	ID                       []byte `db:"id"`
	Dhr                      []byte `db:"dhr"`
	DhsPub                   []byte `db:"dhs_pub"`
	DhsPriv                  []byte `db:"dhs_priv"`
	RootChKey                []byte `db:"root_ch_key"`
	SendChKey                []byte `db:"send_ch_key"`
	SendChCount              uint32 `db:"send_ch_count"`
	RecvChKey                []byte `db:"recv_ch_key"`
	RecvChCount              uint32 `db:"recv_ch_count"`
	PN                       uint32 `db:"pn"`
	MaxSkip                  uint   `db:"max_skip"`
	HKr                      []byte `db:"hkr"`
	NHKr                     []byte `db:"nhkr"`
	HKs                      []byte `db:"hks"`
	NHKs                     []byte `db:"nhks"`
	MaxKeep                  uint   `db:"max_keep"`
	MaxMessageKeysPerSession int    `db:"mmk_per_session"`
	Step                     uint   `db:"step"`
	KeysCount                uint   `db:"keys_count"`
}

type group struct {
	ID                 []byte  `db:"id"`
	DescDigest         []byte  `db:"desc_digest"`
	SelfIdentityID     []byte  `db:"self_identity_id"`
	SelfMembershipID   []byte  `db:"self_membership_id"`
	Seq                uint64  `db:"seq"`
	BackfillSinkID     *[]byte `db:"backfill_sink_id"`
	SourceIdentityID   []byte  `db:"source_identity_id"`
	SourceMembershipID []byte  `db:"source_membership_id"`
	State              int     `db:"state"`
	IntroKeyPriv       []byte  `db:"intro_key_priv"`
}

func (g *group) authorTag() [7]byte {
	var authorTag [7]byte
	copy(authorTag[0:4], g.SelfIdentityID[0:4])
	copy(authorTag[4:], g.SelfMembershipID[4:7])
	return authorTag
}

type groupDescription struct {
	Desc   []byte `db:"desc"`
	Digest []byte `db:"digest"`
}

func (bd groupDescription) decodeDescription() (*GroupDescription, error) {
	desc := &GroupDescription{}
	if err := bencode.Deserialize(bd.Desc, desc); err != nil {
		return nil, err
	}
	return desc, nil
}

type groupMembership struct {
	GroupID                    []byte  `db:"group_id"`
	IdentityID                 []byte  `db:"identity_id"`
	MembershipID               []byte  `db:"membership_id"`
	ConnectionState            int     `db:"connection_state"`
	BackfillState              int     `db:"backfill_state"`
	BackfillSinkID             *[]byte `db:"backfill_sink_id"`
	BackfillSendGroupAckSeq    *uint64 `db:"backfill_send_group_ack_seq"`
	BackfillSendGroupAckSparse *[]byte `db:"backfill_send_group_ack_sparse"`
	LastPrekeyNonce            []byte  `db:"last_prekey_nonce"`

	// for sending
	SendPrivateSeq         uint64 `db:"send_private_seq"`
	SendAcksVersionWritten uint64 `db:"send_acks_version_written"`
	SendAcksVersionSent    uint64 `db:"send_acks_version_sent"`
	SendGroupAckSeq        uint64 `db:"send_group_ack_seq"`
	SendGroupAckSparse     []byte `db:"send_group_ack_sparse"`
	SendPrivateAckSeq      uint64 `db:"send_private_ack_seq"`
	SendPrivateAckSparse   []byte `db:"send_private_ack_sparse"`

	// on recv
	RecvGroupAckSeq      uint64 `db:"recv_group_ack_seq"`
	RecvGroupAckSparse   []byte `db:"recv_group_ack_sparse"`
	RecvPrivateAckSeq    uint64 `db:"recv_private_ack_seq"`
	RecvPrivateAckSparse []byte `db:"recv_private_ack_sparse"`
	RecvDescDigest       []byte `db:"recv_desc_digest"`

	sendPrivateAck *acks
	sendGroupAck   *acks
	recvPrivateAck *acks
	recvGroupAck   *acks
}

func (gm *groupMembership) loadAcks() {
	gm.recvPrivateAck = newAcksFromBitmap(gm.RecvPrivateAckSeq, gm.RecvPrivateAckSparse)
	gm.recvGroupAck = newAcksFromBitmap(gm.RecvGroupAckSeq, gm.RecvGroupAckSparse)
	gm.sendPrivateAck = newAcksFromBitmap(gm.SendPrivateAckSeq, gm.SendPrivateAckSparse)
	gm.sendGroupAck = newAcksFromBitmap(gm.SendGroupAckSeq, gm.SendGroupAckSparse)
}

func (gm *groupMembership) saveAcks() (recvChanged, sendChanged bool) {
	if gm.recvPrivateAck.changed {
		gm.RecvPrivateAckSeq = gm.recvPrivateAck.seq
		gm.RecvPrivateAckSparse = gm.recvPrivateAck.sparseBitmap()
		recvChanged = true
	}
	if gm.recvGroupAck.changed {
		gm.RecvGroupAckSeq = gm.recvGroupAck.seq
		gm.RecvGroupAckSparse = gm.recvGroupAck.sparseBitmap()
		recvChanged = true
	}
	if gm.sendPrivateAck.changed {
		gm.SendPrivateAckSeq = gm.sendPrivateAck.seq
		gm.SendPrivateAckSparse = gm.sendPrivateAck.sparseBitmap()
		sendChanged = true
	}
	if gm.sendGroupAck.changed {
		gm.SendGroupAckSeq = gm.sendGroupAck.seq
		gm.SendGroupAckSparse = gm.sendGroupAck.sparseBitmap()
		sendChanged = true
	}
	return
}

type directMessage struct {
	ID               []byte `db:"id"`
	Body             []byte `db:"body"`
	DeliveryAttempts int    `db:"delivery_attempts"`
	NextDeliveryAt   uint64 `db:"next_delivery_at_ms"`
	IntroID          []byte `db:"intro_id"`
	State            uint8  `db:"state"`
	From             string `db:"from_url"`
}

type directMessageEndpoint struct {
	MessageID []byte `db:"message_id"`
	URL       string `db:"url"`
	Priority  uint8  `db:"priority"`
	State     uint8  `db:"state"`
	LastError string `db:"last_error"`
}

type groupMessage struct {
	GroupID    []byte `db:"group_id"`
	Seq        uint64 `db:"seq"`
	Body       []byte `db:"body"`
	DescDigest []byte `db:"desc_digest"`
}

type groupMessageMember struct {
	GroupID      []byte  `db:"group_id"`
	Seq          uint64  `db:"seq"`
	IdentityID   []byte  `db:"identity_id"`
	MembershipID []byte  `db:"membership_id"`
	BundleID     *[]byte `db:"bundle_id"`
}

type unhandledGroupMessageMember struct {
	GroupID      []byte `db:"group_id"`
	Seq          uint64 `db:"seq"`
	IdentityID   []byte `db:"identity_id"`
	MembershipID []byte `db:"membership_id"`
}

type privateMessage struct {
	GroupID      []byte  `db:"group_id"`
	IdentityID   []byte  `db:"identity_id"`
	MembershipID []byte  `db:"membership_id"`
	Type         uint8   `db:"type"`
	Seq          uint64  `db:"seq"`
	BundleID     *[]byte `db:"bundle_id"`
	Body         []byte  `db:"body"`
}

type messageBundle struct {
	ID                 []byte `db:"id"`
	GroupID            []byte `db:"group_id"`
	IdentityID         []byte `db:"identity_id"`
	MembershipID       []byte `db:"membership_id"`
	DeleteAfterSuccess bool   `db:"delete_after_success"`
	State              int    `db:"state"`
	PrevSendDescDigest []byte `db:"prev_send_desc_digest"`
	NextSendDescDigest []byte `db:"next_send_desc_digest"`
	From               string `db:"from_url"`
}

type messageBundleEndpoint struct {
	BundleID  []byte `db:"bundle_id"`
	URL       string `db:"url"`
	Priority  uint8  `db:"priority"`
	State     uint8  `db:"state"`
	LastError string `db:"last_error"`
}

type backfillSource struct {
	ID           []byte `db:"id"`
	RequestID    []byte `db:"request_id"`
	Type         uint8  `db:"type"`
	FromSelf     bool   `db:"from_self"`
	GroupID      []byte `db:"group_id"`
	IdentityID   []byte `db:"identity_id"`
	MembershipID []byte `db:"membership_id"`
	MaxID        []byte `db:"max_id"`
	Total        uint64 `db:"total"`
}

type backfillSink struct {
	ID            []byte  `db:"id"`
	Type          uint8   `db:"type"`
	GroupID       []byte  `db:"group_id"`
	IdentityID    []byte  `db:"identity_id"`
	MembershipID  []byte  `db:"membership_id"`
	Started       bool    `db:"started"`
	Completed     bool    `db:"completed"`
	Total         uint64  `db:"total"`
	ExpectedTotal *uint64 `db:"expected_total"`
}

type deferredMessage struct {
	ID      []byte `db:"id"`
	URL     string `db:"url"`
	Body    []byte `db:"body"`
	CtimeMs uint64 `db:"ctime_ms"`
}

type jpakeEndpoint struct {
	JpakeID  []byte `db:"jpake_id"`
	URL      string `db:"url"`
	Priority uint8  `db:"priority"`
}

type database struct {
	*db.Database
}

func newDatabase(internalDB *db.Database) (*database, error) {
	d := &database{internalDB}

	if err := internalDB.MigrateNoLock("_messaging", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
					CREATE TABLE _prekey_states (
						id BLOB PRIMARY KEY,
						nonce BLOB NOT NULL,
						initiator NUMBER NOT NULL,
						priv_key BLOB NOT NULL,
						other_public_key BLOB NOT NULL,
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						other_identity_id BLOB NOT NULL,
						other_membership_id BLOB NOT NULL,
						other_signing_key BLOB NOT NULL,
						priv_signing_key BLOB NOT NULL,
						FOREIGN KEY(group_id) REFERENCES _groups(id) ON DELETE CASCADE
					);
					CREATE INDEX prekey_states_group_id on _prekey_states (group_id);
					CREATE INDEX prekey_states_nonce on _prekey_states (nonce);

					CREATE TABLE _deferred_prekey_states (
						id BLOB PRIMARY KEY,
						other_nonce BLOB NOT NULL,
						other_sig1 BLOB NOT NULL,
						other_public_key BLOB NOT NULL,
						from_url STRING NOT NULL
					);

					CREATE TABLE _jpake (
						id BLOB PRIMARY KEY,
						other_x1_g BLOB,
						other_x2_g BLOB,
						x1 BLOB NOT NULL,
						x2 BLOB NOT NULL,
						s BLOB NOT NULL,
						sk BLOB,
						other_user_id BLOB,
						external_id BLOB NOT NULL,
						stage NUMBER NOT NULL
					);

					CREATE TABLE _jpake_endpoints (
						jpake_id BLOB NOT NULL,
						url STRING NOT NULL,
						priority INTEGER NOT NULL,
						FOREIGN KEY(jpake_id) REFERENCES _jpake(id) ON DELETE CASCADE
					);
					CREATE INDEX jpake_endpoints_jpake_id_idx on _jpake_endpoints (jpake_id);

					CREATE TABLE _intros (
						id BLOB PRIMARY KEY,
						existing_group INTEGER NOT NULL,
						group_id BLOB NOT NULL,
						initial_key BLOB NOT NULL,
						initiator INTEGER NOT NULL,
						private_key BLOB NOT NULL,
						peer_public_key BLOB,
						type INTEGER NOT NULL,
						stage INTEGER NOT NULL,
						state_id BLOB NOT NULL,
						finished INTEGER NOT NULL
					);

					CREATE TABLE _sessions (
						id BLOB PRIMARY KEY,
						group_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						identity_id BLOB NOT NULL
					);
					CREATE INDEX sessions_idx on _sessions (group_id, identity_id, membership_id);

					CREATE TABLE _session_urls (
						session_id BLOB NOT NULL,
						url STRING NOT NULL,
						PRIMARY KEY(session_id, url),
						FOREIGN KEY(session_id) REFERENCES _sessions(id)
					);
					CREATE INDEX session_urls_idx on _session_urls (url);

					CREATE TABLE _doubleratchet_keys (
						pub_key BLOB NOT NULL,
						message_key BLOB NOT NULL,
						msg_num INTEGER NOT NULL,
						session_id BLOB NOT NULL,
						seq_num INTEGER NOT NULL
					);
					CREATE UNIQUE INDEX doubleratchet_keys_pubkey_msg_num on _doubleratchet_keys (pub_key, msg_num);
					CREATE UNIQUE INDEX doubleratchet_keys_session_id_seq_num on _doubleratchet_keys (session_id, seq_num);

					CREATE TABLE _doubleratchet_states (
						id BLOB NOT NULL PRIMARY KEY,
						dhr BLOB,
						dhs_pub BLOB NOT NULL,
						dhs_priv BLOB NOT NULL,
						root_ch_key BLOB NOT NULL,
						send_ch_key BLOB NOT NULL,
						send_ch_count BLOB NOT NULL,
						recv_ch_key BLOB NOT NULL,
						recv_ch_count BLOB NOT NULL,
						pn INTEGER NOT NULL,
						max_skip INTEGER NOT NULL,
						hkr BLOB,
						nhkr BLOB,
						hks BLOB,
						nhks BLOB,
						max_keep INTEGER NOT NULL,
						mmk_per_session INTEGER NOT NULL,
						step INTEGER NOT NULL,
						keys_count INTEGER NOT NULL
					);

					CREATE TABLE _groups (
						id BLOB PRIMARY KEY,
						state INTEGER,
						backfill_sink_id BLOB,
						desc_digest BLOB NOT NULL,
						self_identity_id BLOB NOT NULL,
						self_membership_id BLOB NOT NULL,
						source_identity_id BLOB NOT NULL,
						source_membership_id BLOB NOT NULL,
						seq NUMBER NOT NULL,
						intro_key_priv BLOB NOT NULL,
						FOREIGN KEY(desc_digest) REFERENCES _group_descriptions(digest)
					);

					CREATE TABLE _group_descriptions (
						digest BLOB PRIMARY KEY,
						desc BLOB NOT NULL
					);

					CREATE TABLE _group_memberships (
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						connection_state INTEGER NOT NULL,
						backfill_state INTEGER NOT NULL,
						backfill_sink_id BLOB,
						backfill_send_group_ack_seq INTEGER,
						backfill_send_group_ack_sparse BLOB,
						send_private_seq INTEGER NOT NULL,
						send_acks_version_written INTEGER NOT NULL,
						send_acks_version_sent INTEGER NOT NULL,
						send_group_ack_seq INTEGER NOT NULL,
						send_group_ack_sparse BLOB,
						send_private_ack_seq INTEGER NOT NULL,
						send_private_ack_sparse BLOB,
						recv_group_ack_seq INTEGER NOT NULL,
						recv_group_ack_sparse BLOB,
						recv_private_ack_seq INTEGER NOT NULL,
						recv_private_ack_sparse BLOB,
						recv_desc_digest BLOB NOT NULL,
						last_prekey_nonce BLOB NOT NULL,
						FOREIGN KEY(group_id) REFERENCES _groups(id),
						FOREIGN KEY(recv_desc_digest) REFERENCES _group_descriptions(digest),
						PRIMARY KEY(group_id, identity_id, membership_id)
					);
					CREATE INDEX group_memberships_ack_versions on _group_memberships (group_id, connection_state, send_acks_version_written, send_acks_version_sent);
					CREATE INDEX group_memberships_synced on _group_memberships (group_id, recv_group_ack_sparse, recv_group_ack_seq, recv_private_ack_sparse, recv_private_ack_seq, send_private_seq);
					CREATE INDEX group_memberships_connection_backfill on _group_memberships (connection_state, backfill_state);
					CREATE INDEX group_memberships_group_backfill on _group_memberships (group_id, backfill_state);

					CREATE TABLE _group_membership_urls (
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						url STRING NOT NULL,
						PRIMARY KEY(group_id, identity_id, membership_id, url),
						FOREIGN KEY(group_id, identity_id, membership_id) REFERENCES _group_memberships(group_id, identity_id, membership_id)
					);
					CREATE INDEX group_membership_urls_idx on _group_membership_urls (url);

					CREATE TABLE _direct_messages (
						id BLOB PRIMARY KEY,
						body BLOB NOT NULL,
						delivery_attempts INTEGER NOT NULL,
						next_delivery_at_ms INTEGER NOT NULL,
						state INTEGER NOT NULL,
						intro_id BLOB NOT NULL,
						from_url STRING NOT NULL
					);

					CREATE TABLE _direct_message_endpoints (
						message_id BLOB NOT NULL,
						url BLOB NOT NULL,
						priority INTEGER NOT NULL,
						state INTEGER NOT NULL,
						last_error STRING,
						FOREIGN KEY (message_id) REFERENCES _direct_messages(id) ON DELETE CASCADE,
						PRIMARY KEY (message_id, url)
					);
					CREATE INDEX direct_message_endpoints_message_state on _direct_message_endpoints (message_id, state);
					CREATE INDEX direct_message_endpoints_state on _direct_message_endpoints (state);

					CREATE TABLE _group_messages (
						group_id BLOB NOT NULL,
						seq INTERER NOT NULL,
						body BLOB NOT NULL,
						desc_digest BLOB NOT NULL,
						FOREIGN KEY (group_id) REFERENCES _groups(id),
						PRIMARY KEY (group_id, seq)
					);
					CREATE UNIQUE INDEX group_messages_group_seq_idx on _group_messages (group_id, seq);

					CREATE TABLE _group_message_members (
						group_id BLOB NOT NULL,
						seq INTEGER NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						bundle_id BLOB,
						FOREIGN KEY (group_id, seq) REFERENCES _group_messages(group_id, seq) ON DELETE CASCADE,
						FOREIGN KEY (bundle_id) REFERENCES _message_bundles(id) ON DELETE CASCADE,
						PRIMARY KEY (group_id, seq, identity_id, membership_id)
					);
					CREATE INDEX group_message_members_idx on _group_message_members (group_id, identity_id, membership_id);

					CREATE TABLE _private_messages (
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						seq INTEGER NOT NULL,
						type INTEGER NOT NULL,
						bundle_id BLOB,
						body BLOB,
						PRIMARY KEY (group_id, identity_id, membership_id, seq),
						FOREIGN KEY (group_id) REFERENCES _groups(id),
						FOREIGN KEY (bundle_id) REFERENCES _message_bundles(id) ON DELETE CASCADE
					);
					CREATE INDEX private_messages_bundle_idx on _private_messages (bundle_id);

					CREATE TABLE _message_bundles (
						id BLOB PRIMARY KEY,
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						delete_after_success INTEGER NOT NULL,
						state INTEGER NOT NULL,
						prev_send_desc_digest BLOB NOT NULL,
						next_send_desc_digest BLOB NOT NULL,
						from_url STRING NOT NULL,
						FOREIGN KEY (group_id) REFERENCES _groups(id) ON DELETE CASCADE
					);

					CREATE TABLE _message_bundle_endpoints (
						bundle_id BLOB NOT NULL,
						url BLOB NOT NULL,
						priority INTEGER NOT NULL,
						state INTEGER NOT NULL,
						last_error STRING,
						PRIMARY KEY (bundle_id, url),
						FOREIGN KEY (bundle_id) REFERENCES _message_bundles(id) ON DELETE CASCADE
					);

					CREATE TABLE _backfill_sources (
						id BLOB PRIMARY KEY,
						request_id BLOB NOT NULL,
						type INTEGER NOT NULL,
						from_self INTEGER NOT NULL,
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						max_id BLOB NOT NULL,
						total INTEGER NOT NULL,
						FOREIGN KEY (group_id) REFERENCES _groups(id) ON DELETE CASCADE
					);

					CREATE TABLE _backfill_sinks (
						id BLOB PRIMARY KEY,
						type INTEGER NOT NULL,
						group_id BLOB NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						started INTEGER NOT NULL,
						completed INTEGER NOT NULL,
						total INTEGER NOT NULL,
						expected_total INTEGER,
						FOREIGN KEY (group_id) REFERENCES _groups(id) ON DELETE CASCADE
					);

					CREATE TABLE _deferred_messages (
						id BLOB PRIMARY KEY,
						url STRING NOT NULL,
						body BLOB NOT NULL,
						ctime_ms INTEGER NOT NULL
					);
					CREATE INDEX deferred_messages_url_idx on _deferred_messages (url, ctime_ms);
					CREATE INDEX deferred_messages_ctime_idx on _deferred_messages (ctime_ms);

					CREATE TABLE _unhandled_group_message_members (
						group_id BLOB NOT NULL,
						seq INTEGER NOT NULL,
						identity_id BLOB NOT NULL,
						membership_id BLOB NOT NULL,
						FOREIGN KEY (group_id, seq) REFERENCES _group_messages(group_id, seq) ON DELETE CASCADE,
						PRIMARY KEY (group_id, seq, identity_id, membership_id)
					);
				`)
				return err
			},
		},
	}); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *database) doubleratchetState(id []byte) (*doubleratchetState, error) {
	s := &doubleratchetState{}
	if err := db.Tx.Get(s, "select * from _doubleratchet_states where id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting doubleratchet_state: %w", err)
	}
	return s, nil
}

func (db *database) upsertDoubleratchetState(s *doubleratchetState) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _doubleratchet_states (id, dhr, dhs_pub, dhs_priv, root_ch_key, send_ch_key, send_ch_count, recv_ch_key, recv_ch_count, pn, max_skip, hkr, nhkr, hks, nhks, max_keep, mmk_per_session, step, keys_count) VALUES (:id, :dhr, :dhs_pub, :dhs_priv, :root_ch_key, :send_ch_key, :send_ch_count, :recv_ch_key, :recv_ch_count, :pn, :max_skip, :hkr, :nhkr, :hks, :nhks, :max_keep, :mmk_per_session, :step, :keys_count) on CONFLICT(id) DO UPDATE SET dhr = :dhr, dhs_pub = :dhs_pub, dhs_priv = :dhs_priv, root_ch_key = :root_ch_key, send_ch_key = :send_ch_key, send_ch_count = :send_ch_count, recv_ch_key = :recv_ch_key, recv_ch_count = :recv_ch_count, pn = :pn, max_skip = :max_skip, hkr = :hkr, nhkr = :nhkr, hks = :hks, nhks = :nhks, max_keep = :max_keep, mmk_per_session = :mmk_per_session, step = :step, keys_count = :keys_count", s); err != nil {
		return fmt.Errorf("messaging: error upserting doubleratchet_state: %w", err)
	}
	return nil
}

func (db *database) doubleratchetSessionStorage() doubleratchet.SessionStorage {
	return &sessionStorageImpl{db: db}
}

func (db *database) doubleratchetCrypto() doubleratchet.Crypto {
	return &cryptoImpl{}
}

func (db *database) doubleratchetKeysStorage(sessionID []byte) doubleratchet.KeysStorage {
	return &keysStorageImpl{sessionID: sessionID, db: db}
}

func (db *database) keyByMsgNum(sessionID []byte, k doubleratchet.Key, msgNum uint) (*doubleratchetKey, bool, error) {
	kr := &doubleratchetKey{}
	err := db.Tx.Get(kr, "SELECT * FROM _doubleratchet_keys WHERE pub_key = ? and msg_num = ? and session_id = ?", k, msgNum, sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return kr, true, nil
}

func (db *database) upsertKeyByMsgNum(sessionID []byte, k doubleratchet.Key, msgNum uint, mk doubleratchet.Key, keySeqNum uint) error {
	_, err := db.Tx.Exec("INSERT INTO _doubleratchet_keys (pub_key, message_key, msg_num, session_id, seq_num) VALUES (?, ?, ?, ?, ?)", k, mk, msgNum, sessionID, keySeqNum)
	if err != nil {
		return fmt.Errorf("messaging: error upserting key by msgnum: %w", err)
	}
	return nil
}

func (db *database) deleteKeyByMsgNum(sessionID []byte, k doubleratchet.Key, msgNum uint) error {
	_, err := db.Tx.Exec("DELETE FROM _doubleratchet_keys WHERE pub_key = ? and msg_num = ? and session_id = ?", k, msgNum, sessionID)
	if err != nil {
		return fmt.Errorf("messaging: error deleting key by msgnum: %w", err)
	}
	return nil
}

func (db *database) deleteOldMks(sessionID []byte, deleteUntilSeqKey uint) error {
	_, err := db.Tx.Exec("DELETE FROM _doubleratchet_keys WHERE session_id = ? and seq_num < ?", sessionID, deleteUntilSeqKey)
	if err != nil {
		return fmt.Errorf("messaging: error deleting old keys: %w", err)
	}
	return nil
}

func (db *database) truncateMks(sessionID []byte, maxKeys int) error {
	_, err := db.Tx.Exec("DELETE FROM _doubleratchet_keys where session_id = ? and seq_num not in (select seq_num from _doubleratchet_keys where session_id = ? ORDER BY seq_num DESC LIMIT ?)", sessionID, sessionID, maxKeys)
	if err != nil {
		return fmt.Errorf("messaging: error truncating keys: %w", err)
	}
	return nil
}

func (db *database) countKeys(k doubleratchet.Key) (uint, error) {
	counter := &struct {
		Count uint `db:"keys_count"`
	}{Count: 0}
	if err := db.Tx.Get(counter, "SELECT count(*) as keys_count FROM _doubleratchet_keys WHERE pub_key = ?", k); err != nil {
		return 0, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	return counter.Count, nil
}

func (db *database) upsertGroup(g *group) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _groups (id, desc_digest, state, backfill_sink_id, self_identity_id, self_membership_id, source_identity_id, source_membership_id, seq, intro_key_priv) VALUES (:id, :desc_digest, :state, :backfill_sink_id, :self_identity_id, :self_membership_id, :source_identity_id, :source_membership_id, :seq, :intro_key_priv) ON CONFLICT(id) DO UPDATE SET desc_digest = :desc_digest, state = :state, backfill_sink_id = :backfill_sink_id, seq = :seq", g); err != nil {
		return fmt.Errorf("messaging: error upserting group: %w", err)
	}
	return nil
}

func (db *database) upsertPrekeyState(ps *prekeyState) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _prekey_states (id, nonce, initiator, priv_key, other_public_key, group_id, identity_id, membership_id, other_identity_id, other_membership_id, other_signing_key, priv_signing_key) VALUES (:id, :nonce, :initiator, :priv_key, :other_public_key, :group_id, :identity_id, :membership_id, :other_identity_id, :other_membership_id, :other_signing_key, :priv_signing_key) ON CONFLICT(id) DO UPDATE SET nonce = :nonce, initiator = :initiator, priv_key = :priv_key, other_public_key = :other_public_key, group_id = :group_id, identity_id = :identity_id, membership_id = :membership_id, other_identity_id = :other_identity_id, other_membership_id = :other_membership_id, other_signing_key = :other_signing_key, priv_signing_key = :priv_signing_key", ps); err != nil {
		return fmt.Errorf("messaging: error upserting prekey_states: %w", err)
	}
	return nil
}

func (db *database) prekeyState(id []byte) (*prekeyState, error) {
	ps := prekeyState{}
	if err := db.Tx.Get(&ps, "SELECT * FROM _prekey_states where id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting prekey_state: %w", err)
	}
	return &ps, nil
}

func (db *database) prekeyStateForNonce(id []byte) (*prekeyState, error) {
	ps := prekeyState{}
	if err := db.Tx.Get(&ps, "SELECT * FROM _prekey_states where nonce = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting prekey_state for nonce: %w", err)
	}
	return &ps, nil
}

func (db *database) deferredPrekeyState(id []byte) (*deferredPrekeyState, error) {
	dps := &deferredPrekeyState{}
	if err := db.Tx.Get(dps, "SELECT * FROM _deferred_prekey_states WHERE id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting deferred_prekey_state: %w", err)
	}
	return dps, nil
}

func (db *database) deferredPrekeyStates() ([]*deferredPrekeyState, error) {
	var dps []*deferredPrekeyState
	if err := db.Tx.Select(&dps, "SELECT * FROM _deferred_prekey_states"); err != nil {
		return nil, fmt.Errorf("messaging: error getting deferred_prekey_state: %w", err)
	}
	return dps, nil
}

func (db *database) deferredPrekeyStatesFrom(from string) ([]*deferredPrekeyState, error) {
	var dps []*deferredPrekeyState
	if err := db.Tx.Select(&dps, "SELECT * FROM _deferred_prekey_states WHERE from_url = $1", from); err != nil {
		return nil, fmt.Errorf("messaging: error getting deferred_prekey_state with from %s: %w", from, err)
	}
	return dps, nil
}

func (db *database) upsertDeferredPrekeyState(ps *deferredPrekeyState) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _deferred_prekey_states (id, other_nonce, other_public_key, other_sig1, from_url) VALUES (:id, :other_nonce, :other_public_key, :other_sig1, :from_url) ON CONFLICT(id) DO UPDATE SET other_nonce = :other_nonce, other_public_key = :other_public_key, other_sig1 = :other_sig1, from_url = :from_url", ps); err != nil {
		return fmt.Errorf("messaging: error upserting prekey_states: %w", err)
	}
	return nil
}

func (db *database) deleteDeferredPrekeyState(id []byte) error {
	if _, err := db.Tx.Exec("DELETE from _deferred_prekey_states WHERE id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting deferred_prekey_state: %w", err)
	}
	return nil
}

func (db *database) upsertGroupMembership(gm *groupMembership) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _group_memberships (group_id, identity_id, membership_id, connection_state, backfill_state, backfill_sink_id, backfill_send_group_ack_seq, backfill_send_group_ack_sparse, send_private_seq, send_acks_version_written, send_acks_version_sent, send_group_ack_seq, send_group_ack_sparse, send_private_ack_seq, send_private_ack_sparse, recv_group_ack_seq, recv_group_ack_sparse, recv_private_ack_seq, recv_private_ack_sparse, recv_desc_digest, last_prekey_nonce) VALUES (:group_id, :identity_id, :membership_id, :connection_state, :backfill_state, :backfill_sink_id, :backfill_send_group_ack_seq, :backfill_send_group_ack_sparse, 0, :send_acks_version_written, :send_acks_version_sent, :send_group_ack_seq, :send_group_ack_sparse, :send_private_ack_seq, :send_private_ack_sparse, :recv_group_ack_seq, :recv_group_ack_sparse, :recv_private_ack_seq, :recv_private_ack_sparse, :recv_desc_digest, :last_prekey_nonce) ON CONFLICT(group_id, identity_id, membership_id) DO UPDATE SET connection_state = :connection_state, backfill_state = :backfill_state, backfill_sink_id = :backfill_sink_id, backfill_send_group_ack_seq = :backfill_send_group_ack_seq, backfill_send_group_ack_sparse = :backfill_send_group_ack_sparse, send_acks_version_written = :send_acks_version_written, send_acks_version_sent = :send_acks_version_sent, send_group_ack_seq = :send_group_ack_seq, send_group_ack_sparse = :send_group_ack_sparse, send_private_ack_seq = :send_private_ack_seq, send_private_ack_sparse = :send_private_ack_sparse, recv_group_ack_seq = :recv_group_ack_seq, recv_group_ack_sparse = :recv_group_ack_sparse, recv_private_ack_seq = :recv_private_ack_seq, recv_private_ack_sparse = :recv_private_ack_sparse, recv_desc_digest = :recv_desc_digest, last_prekey_nonce = :last_prekey_nonce", gm); err != nil {
		return fmt.Errorf("messaging: error upserting group membership: %w", err)
	}
	return nil
}

func (db *database) groupMembershipDefault(groupID, identityID, membershipID []byte, defaultGroupMembership *groupMembership) (*groupMembership, bool, error) {
	groupMembership, err := db.groupMembership(groupID, identityID, membershipID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			defaultGroupMembership.loadAcks()
			return defaultGroupMembership, true, nil
		}
		return nil, false, err
	}
	groupMembership.loadAcks()
	return groupMembership, false, nil
}

func (db *database) insertIntro(i *intro) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _intros (id, existing_group, group_id, initial_key, initiator, private_key, peer_public_key, type, stage, state_id, finished) VALUES (:id, :existing_group, :group_id, :initial_key, :initiator, :private_key, :peer_public_key, :type, :stage, :state_id, :finished)", i); err != nil {
		return fmt.Errorf("messaging: error inserting intro: %w", err)
	}
	return nil
}

func (db *database) deleteIntro(introID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _intros WHERE id = $1", introID); err != nil {
		return fmt.Errorf("messaging: error deleting intro: %w", err)
	}
	return nil
}

func (db *database) deleteIntrosByGroup(groupID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _intros WHERE group_id = $1", groupID); err != nil {
		return fmt.Errorf("messaging: error deleting intro: %w", err)
	}
	return nil
}

func (db *database) deleteJpake(jpakeID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _jpake WHERE id = $1", jpakeID); err != nil {
		return fmt.Errorf("messaging: error deleting jpake: %w", err)
	}
	return nil
}

func (db *database) deletePrekeyState(prekeyStateID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _prekey_states WHERE id = $1", prekeyStateID); err != nil {
		return fmt.Errorf("messaging: error deleting prekey_state: %w", err)
	}
	return nil
}

func (db *database) upsertDirectMessage(dm *directMessage) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _direct_messages (id, body, intro_id, next_delivery_at_ms, delivery_attempts, state, from_url) VALUES (:id, :body, :intro_id, :next_delivery_at_ms, :delivery_attempts, :state, :from_url) ON CONFLICT (id) DO UPDATE SET next_delivery_at_ms = :next_delivery_at_ms, delivery_attempts = :delivery_attempts, state = :state", dm); err != nil {
		return fmt.Errorf("messaging: error upserting direct message: %w", err)
	}
	return nil
}

func (db *database) upsertDirectMessageEndpoint(dme *directMessageEndpoint) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _direct_message_endpoints (message_id, url, priority, state, last_error) VALUES (:message_id, :url, :priority, :state, :last_error) ON CONFLICT (message_id, url) DO UPDATE SET state = :state, last_error = :last_error", dme); err != nil {
		return fmt.Errorf("messaging: error upserting direct_message_endpoint: %w", err)
	}
	return nil
}

func (db *database) directMessage(id []byte) (*directMessage, error) {
	dm := directMessage{}
	if err := db.Tx.Get(&dm, "SELECT * FROM _direct_messages where id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting direct_messages: %w", err)
	}
	return &dm, nil
}

func (db *database) lateDirectMessages() ([]*directMessage, error) {
	var dms []*directMessage
	if err := db.Tx.Select(&dms, "SELECT * FROM _direct_messages dms where next_delivery_at_ms != 0 AND EXISTS (select 1 from _direct_message_endpoints dme where dme.message_id = dms.id AND dme.state = $1)", EndpointStateReady); err != nil {
		return nil, fmt.Errorf("messaging: error getting ready direct message endpoint: %w", err)
	}
	return dms, nil
}

func (db *database) readyDirectMessageEndpoints() ([]*directMessageEndpoint, error) {
	var dme []*directMessageEndpoint
	if err := db.Tx.Select(&dme, "SELECT * FROM _direct_message_endpoints where state = $1 and message_id IN (select id from _direct_messages where next_delivery_at_ms = 0 OR next_delivery_at_ms <= $2)", EndpointStateReady, time.Now().UnixMilli()); err != nil {
		return nil, fmt.Errorf("messaging: error getting ready direct message endpoint: %w", err)
	}
	return dme, nil
}

func (db *database) directMessageEndpoints(messageID []byte) ([]*directMessageEndpoint, error) {
	var dme []*directMessageEndpoint
	if err := db.Tx.Select(&dme, "SELECT * FROM _direct_message_endpoints where message_id = $1", messageID); err != nil {
		return nil, fmt.Errorf("messaging: error getting direct message endpoint: %w", err)
	}
	return dme, nil
}

func (db *database) directMessageEndpointsByState(messageID []byte, state uint8) ([]*directMessageEndpoint, error) {
	var dme []*directMessageEndpoint
	if err := db.Tx.Select(&dme, "SELECT * FROM _direct_message_endpoints where message_id = $1 AND state = $2", messageID, state); err != nil {
		return nil, fmt.Errorf("messaging: error getting direct message endpoint: %w", err)
	}
	return dme, nil
}

func (db *database) insertGroupMessage(gm *groupMessage) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _group_messages (group_id, seq, body, desc_digest) VALUES (:group_id, :seq, :body, :desc_digest)", gm); err != nil {
		return fmt.Errorf("messaging: error upserting group message: %w", err)
	}
	return nil
}

func (db *database) upsertGroupMessageMember(gmm *groupMessageMember) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _group_message_members (group_id, seq, bundle_id, identity_id, membership_id) VALUES (:group_id, :seq, :bundle_id, :identity_id, :membership_id) ON CONFLICT(group_id, seq, identity_id, membership_id) DO UPDATE SET bundle_id = :bundle_id", gmm); err != nil {
		return fmt.Errorf("messaging: error upserting group message member: %w", err)
	}
	return nil
}

func (db *database) insertUnhandledGroupMessageMember(ugmm *unhandledGroupMessageMember) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _unhandled_group_message_members (group_id, seq, identity_id, membership_id) VALUES (:group_id, :seq, :identity_id, :membership_id)", ugmm); err != nil {
		return fmt.Errorf("messaging: error upserting group message member: %w", err)
	}
	return nil
}

func (db *database) unhandledGroupMessageMembers(groupID []byte, seq uint64) ([]*unhandledGroupMessageMember, error) {
	var unhandledGroupMessageMembers []*unhandledGroupMessageMember
	if err := db.Tx.Select(&unhandledGroupMessageMembers, "SELECT * FROM _unhandled_group_message_members WHERE group_id = $1 AND seq = $2", groupID, seq); err != nil {
		return nil, fmt.Errorf("messaging: error getting unbundled group messages: %w", err)
	}
	return unhandledGroupMessageMembers, nil
}

func (db *database) privateMessage(groupID, identityID, membershipID []byte, seq uint64) (*privateMessage, error) {
	pm := &privateMessage{}
	if err := db.Tx.Get(pm, "SELECT * FROM _private_messages where group_id = $1 AND identity_id = $2 AND membership_id = $3 AND seq = $4", groupID, identityID, membershipID, seq); err != nil {
		return nil, fmt.Errorf("messaging: error getting private message: %w", err)
	}
	return pm, nil
}

func (db *database) insertPrivateMessageAndIncrement(pm *privateMessage) error {
	type seqStruct struct {
		SendPrivateSeq uint64 `db:"send_private_seq"`
	}

	seq := seqStruct{}
	if err := db.Tx.Get(&seq, "select send_private_seq from _group_memberships where group_id = $1 and identity_id = $2 and membership_id = $3", pm.GroupID, pm.IdentityID, pm.MembershipID); err != nil {
		return fmt.Errorf("messaging: error inserting private message1: %w", err)
	}
	pm.Seq = seq.SendPrivateSeq + 1
	if _, err := db.Tx.NamedExec(`
	INSERT INTO _private_messages (group_id, identity_id, membership_id, type, bundle_id, body, seq)
	VALUES (:group_id, :identity_id, :membership_id, :type, :bundle_id, :body, :seq)`, pm); err != nil {
		return fmt.Errorf("messaging: error inserting private message2 %w", err)
	}

	up, err := db.Tx.NamedExec(`UPDATE _group_memberships SET send_private_seq = send_private_seq + 1 where group_id = :group_id and identity_id = :identity_id and membership_id = :membership_id`, pm)
	if err != nil {
		return fmt.Errorf("messaging: error inserting private message3: %w", err)
	}
	rows, err := up.RowsAffected()
	if err != nil {
		return fmt.Errorf("messaging: error inserting private message4: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("messaging: expected 1 row to be udpated, got %d", rows)
	}

	return nil
}

func (db *database) upsertPrivateMessage(pm *privateMessage) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _private_messages (group_id, identity_id, membership_id, type, seq, bundle_id, body) VALUES (:group_id, :identity_id, :membership_id, :type, :seq, :bundle_id, :body) ON CONFLICT(group_id, identity_id, membership_id, seq) DO UPDATE SET type = :type, bundle_id = :bundle_id, body = :body", pm); err != nil {
		return fmt.Errorf("messaging: error upserting private message: %w", err)
	}
	return nil
}

func (db *database) upsertMessageBundle(mb *messageBundle) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _message_bundles (id, group_id, identity_id, membership_id, delete_after_success, state, prev_send_desc_digest, next_send_desc_digest, from_url) VALUES (:id, :group_id, :identity_id, :membership_id, :delete_after_success, :state, :prev_send_desc_digest, :next_send_desc_digest, :from_url) ON CONFLICT(id) DO UPDATE SET state = :state", mb); err != nil {
		return fmt.Errorf("messaging: error inserting message bundle: %w", err)
	}
	return nil
}

func (db *database) messageBundleOrNil(id []byte) (*messageBundle, error) {
	mb := messageBundle{}
	if err := db.Tx.Get(&mb, "SELECT * FROM _message_bundles where id = $1", id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("messaging: error getting message bundle: %w", err)
	}
	return &mb, nil
}

func (db *database) messageBundleEndpoints(id []byte, state uint8) ([]*messageBundleEndpoint, error) {
	var mbe []*messageBundleEndpoint
	if err := db.Tx.Select(&mbe, "SELECT * FROM _message_bundle_endpoints where bundle_id = $1 AND state = $2", id, state); err != nil {
		return nil, fmt.Errorf("messaging: error getting direct message endpoint: %w", err)
	}
	return mbe, nil
}

func (db *database) readyMessageBundleEndpoints(id []byte) ([]*messageBundleEndpoint, error) {
	var mbe []*messageBundleEndpoint
	if err := db.Tx.Select(&mbe, "SELECT * FROM _message_bundle_endpoints where bundle_id = $1 AND state = $2", id, EndpointStateReady); err != nil {
		return nil, fmt.Errorf("messaging: error getting ready message bundle endpoints: %w", err)
	}
	return mbe, nil
}

func (db *database) unbundledMessageIdentities() ([]*groupIdentity, error) {
	var groupIdentities []*groupIdentity
	if err := db.Tx.Select(&groupIdentities, `select distinct group_id, identity_id, membership_id from (
		select group_id, identity_id, membership_id from _group_message_members where bundle_id IS NULL
		UNION
		select group_id, identity_id, membership_id from _private_messages where bundle_id IS NULL
	)`); err != nil {
		return nil, fmt.Errorf("messaging: error getting unbundled identities: %w", err)
	}
	return groupIdentities, nil
}

func (db *database) unbundledGroupMessageMembers(groupID, identityID, membershipID []byte) ([]*groupMessageMember, error) {
	var groupMessageMembers []*groupMessageMember
	if err := db.Tx.Select(&groupMessageMembers, "SELECT * FROM _group_message_members WHERE bundle_id IS NULL AND group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return nil, fmt.Errorf("messaging: error getting unbundled group messages: %w", err)
	}
	return groupMessageMembers, nil
}

func (db *database) groupMessagesForBundle(id []byte) ([]*groupMessage, error) {
	var groupMessages []*groupMessage
	if err := db.Tx.Select(&groupMessages, "SELECT * FROM _group_messages WHERE (group_id, seq) IN (select distinct group_id, seq from _group_message_members WHERE bundle_id = $1)", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting group messages for bundle: %w", err)
	}
	return groupMessages, nil
}

func (db *database) groupMessage(groupID []byte, seq uint64) (*groupMessage, error) {
	gm := groupMessage{}
	if err := db.Tx.Get(&gm, "SELECT * FROM _group_messages WHERE group_id = $1 AND seq = $2", groupID, seq); err != nil {
		return nil, fmt.Errorf("messaging: error getting group messages for bundle: %w", err)
	}
	return &gm, nil
}

func (db *database) unbundledPrivateMessages(groupID, identityID, membershipID []byte) ([]*privateMessage, error) {
	var privateMessages []*privateMessage
	if err := db.Tx.Select(&privateMessages, "SELECT * FROM _private_messages WHERE bundle_id IS NULL AND group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return nil, fmt.Errorf("messaging: error getting unbundled private messages: %w", err)
	}
	return privateMessages, nil
}

func (db *database) privateMessagesForBundle(id []byte) ([]*privateMessage, error) {
	var privateMessages []*privateMessage
	if err := db.Tx.Select(&privateMessages, "SELECT * FROM _private_messages WHERE bundle_id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting private messages for bundle: %w", err)
	}
	return privateMessages, nil
}

func (db *database) upsertMessageBundleEndpoint(mbe *messageBundleEndpoint) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _message_bundle_endpoints (bundle_id, url, priority, state, last_error) VALUES (:bundle_id, :url, :priority, :state, :last_error) ON CONFLICT(bundle_id, url) DO UPDATE SET priority = :priority, state = :state, last_error = :last_error", mbe); err != nil {
		return fmt.Errorf("messaging: error upserting message bundle endpoints: %w", err)
	}
	return nil
}

func (db *database) groups() ([]*group, error) {
	var groups []*group
	if err := db.Tx.Select(&groups, "SELECT * FROM _groups"); err != nil {
		return nil, fmt.Errorf("messaging: error getting groups: %w", err)
	}
	return groups, nil
}

func (db *database) incompleteGroupBackfills() ([]*group, error) {
	var groups []*group
	if err := db.Tx.Select(&groups, "SELECT * FROM _groups WHERE backfill_sink_id IS NOT NULL AND backfill_sink_id NOT IN (select id from _backfill_sinks where group_id = _groups.id)"); err != nil {
		return nil, fmt.Errorf("messaging: error getting incomplete group backfills: %w", err)
	}
	return groups, nil
}

func (db *database) insertGroupDescription(gd *groupDescription) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _group_descriptions (desc, digest) VALUES (:desc, :digest) ON CONFLICT DO NOTHING", gd); err != nil {
		return fmt.Errorf("messaging: error inserting group description: %w", err)
	}
	return nil
}

func (db *database) upsertJpake(jpake *jpakeState) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _jpake (id, other_x1_g, other_x2_g, other_user_id, x1, x2, s, sk, external_id, stage) VALUES (:id, :other_x1_g, :other_x2_g, :other_user_id, :x1, :x2, :s, :sk, :external_id, :stage) ON CONFLICT(id) DO UPDATE SET other_x1_g = :other_x1_g, other_x2_g = :other_x2_g, x1 = :x1, x2 = :x2, s = :s, sk = :sk, other_user_id = :other_user_id, stage = :stage", jpake); err != nil {
		return fmt.Errorf("messaging: error upserting jpake: %w", err)
	}
	return nil
}

func (db *database) group(id []byte) (*group, error) {
	g := group{}
	if err := db.Tx.Get(&g, "SELECT * FROM _groups where id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting group: %w", err)
	}
	return &g, nil
}

func (db *database) sessions(groupID, identityID, membershipID []byte) ([]*session, error) {
	var sessions []*session
	if err := db.Tx.Select(&sessions, "SELECT * FROM _sessions where group_id = $1 AND identity_id = $2 and membership_id = $3", groupID, identityID, membershipID); err != nil {
		return nil, fmt.Errorf("messaging: error getting sessions: %w", err)
	}
	return sessions, nil
}

func (db *database) insertSession(initiator bool, initialKey []byte, session *session, secret []byte) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _sessions (id, group_id, identity_id, membership_id) VALUES (:id, :group_id, :identity_id, :membership_id)", session); err != nil {
		return fmt.Errorf("messaging: error inserting session: %w", err)
	}

	if initiator {
		k := crypto.SliceToKey(initialKey)
		publicKey := scalarmult.Base(k)
		dhPair := dhPairImpl{privateKey: [32]byte(*k), publicKey: *publicKey}
		if _, err := doubleratchet.New(session.ID, secret, dhPair, db.doubleratchetSessionStorage(), doubleratchet.WithCrypto(db.doubleratchetCrypto()), doubleratchet.WithKeysStorage(db.doubleratchetKeysStorage(session.ID))); err != nil {
			return fmt.Errorf("messaging: error initializing doubleratchet: %w", err)
		}
	} else {
		if _, err := doubleratchet.NewWithRemoteKey(session.ID, secret, initialKey[:], db.doubleratchetSessionStorage(), doubleratchet.WithCrypto(db.doubleratchetCrypto()), doubleratchet.WithKeysStorage(db.doubleratchetKeysStorage(session.ID))); err != nil {
			return fmt.Errorf("messaging: error initializing doubleratchet: %w", err)
		}
	}
	return nil
}

func (db *database) updateIntro(i *intro) error {
	if _, err := db.Tx.NamedExec("UPDATE _intros set private_key = :private_key, peer_public_key = :peer_public_key, state_id = :state_id, stage = :stage, type = :type, finished = :finished WHERE id = :id", i); err != nil {
		return fmt.Errorf("messaging: error updating intro: %w", err)
	}
	return nil
}

func (db *database) groupMemberships(id []byte) ([]*groupMembership, error) {
	var gms []*groupMembership
	if err := db.Tx.Select(&gms, "SELECT * FROM _group_memberships where group_id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting group memberships groupid=%x: %w", id, err)
	}
	for _, gm := range gms {
		gm.loadAcks()
	}
	return gms, nil
}

func (db *database) groupMembership(groupID, identityID, membershipID []byte) (*groupMembership, error) {
	gms := &groupMembership{}
	if err := db.Tx.Get(gms, "SELECT * FROM _group_memberships where group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return nil, fmt.Errorf("messaging: error getting group membership %x %x:%x: %w", groupID, identityID, membershipID, err)
	}
	gms.loadAcks()
	return gms, nil
}

func (db *database) groupDescription(id []byte) (*groupDescription, error) {
	gd := &groupDescription{}
	if err := db.Tx.Get(gd, "SELECT * FROM _group_descriptions where digest = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting group description for id=%x: %w", id, err)
	}
	return gd, nil
}

func (db *database) introForStateTypeID(t int, id []byte) (*intro, error) {
	i := &intro{}
	if err := db.Tx.Get(i, "SELECT * FROM _intros where type = $1 AND state_id = $2", t, id); err != nil {
		return nil, fmt.Errorf("messaging: error getting intros for state type: %w", err)
	}
	return i, nil
}

func (db *database) introsForTypeStage(t, stage int) ([]*intro, error) {
	var intros []*intro
	if err := db.Tx.Select(&intros, "SELECT * FROM _intros where type = $1 AND stage = $2", t, stage); err != nil {
		return nil, fmt.Errorf("messaging: error getting intros for state type: %w", err)
	}
	return intros, nil
}

func (db *database) sessionsByURL(url string) ([]*session, error) {
	var sessions []*session
	if err := db.Tx.Select(&sessions, "SELECT * FROM _sessions where id in (select session_id from _session_urls where url = $1)", url); err != nil {
		return nil, fmt.Errorf("messaging: error getting sessions by url: %w", err)
	}
	return sessions, nil
}

func (db *database) readyMessageBundleIDs() ([][]byte, error) {
	ids := make([][]byte, 0)
	if err := db.Tx.Select(&ids, "select bundle_id from _message_bundle_endpoints where state = $1 group by bundle_id", EndpointStateReady); err != nil {
		return nil, fmt.Errorf("messaging: error getting ready message bundle ids: %w", err)
	}
	return ids, nil
}

func (db *database) setURLsForSession(id []byte, urls []string) error {
	if _, err := db.Tx.Exec("DELETE FROM _session_urls where session_id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting session urls: %w", err)
	}
	for _, url := range urls {
		if _, err := db.Tx.Exec("INSERT INTO _session_urls (session_id, url) VALUES ($1, $2)", id, url); err != nil {
			return fmt.Errorf("messaging: error inserting session url: %w", err)
		}
	}
	return nil
}

func (db *database) setURLsForGroupMembership(groupID []byte, identityID []byte, membershipID []byte, urls []string) error {
	if _, err := db.Tx.Exec("DELETE FROM _group_membership_urls where group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return fmt.Errorf("messaging: error deleting group membership urls: %w", err)
	}
	for _, url := range urls {
		if _, err := db.Tx.Exec("INSERT INTO _group_membership_urls (group_id, identity_id, membership_id, url) VALUES ($1, $2, $3, $4)", groupID, identityID, membershipID, url); err != nil {
			return fmt.Errorf("messaging: error inserting group membership url: %w", err)
		}
	}
	return nil
}

func (db *database) jpakeForExternalID(externalID []byte) (*jpakeState, error) {
	jpake := &jpakeState{}
	if err := db.Tx.Get(jpake, "SELECT * FROM _jpake WHERE external_id = $1", externalID); err != nil {
		return nil, fmt.Errorf("messaging: error getting jpake: %w", err)
	}
	if err := jpake.init(); err != nil {
		return nil, fmt.Errorf("messaging: error initing jpake: %w", err)
	}
	return jpake, nil
}

func (db *database) deleteGroupMessageMember(groupID, identityID, membershipID []byte, seq uint64) error {
	if _, err := db.Tx.Exec("DELETE FROM _group_message_members where group_id = $1 AND identity_id = $2 AND membership_id = $3 AND seq = $4", groupID, identityID, membershipID, seq); err != nil {
		return fmt.Errorf("messaging: error deleting group message member: %w", err)
	}
	return nil
}

func (db *database) deleteGroupMessageMembers(groupID, identityID, membershipID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _group_message_members where group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return fmt.Errorf("messaging: error deleting group message member: %w", err)
	}
	return nil
}

func (db *database) deletePrivateMessage(groupID, identityID, membershipID []byte, seq uint64) error {
	if _, err := db.Tx.Exec("DELETE FROM _private_messages where group_id = $1 AND identity_id = $2 AND membership_id = $3 AND seq = $4", groupID, identityID, membershipID, seq); err != nil {
		return fmt.Errorf("messaging: error deleting private message: %w", err)
	}
	return nil
}

func (db *database) deletePrivateMessages(groupID, identityID, membershipID []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _private_messages where group_id = $1 AND identity_id = $2 AND membership_id = $3", groupID, identityID, membershipID); err != nil {
		return fmt.Errorf("messaging: error deleting private message: %w", err)
	}
	return nil
}

func (db *database) guardDeleteMessageBundle(id []byte) error {
	var ids [][]byte
	if err := db.Tx.Select(&ids, "select bundle_id from _group_message_members where bundle_id = $1 UNION select bundle_id from _private_messages where bundle_id = $2", id, id); err != nil {
		return fmt.Errorf("messaging: error deleting message bundle: %w", err)
	}
	if len(ids) != 0 {
		return fmt.Errorf("expected remaining messaging rows to be 0, instead was %d", len(ids))
	}

	if _, err := db.Tx.Exec("DELETE FROM _message_bundles where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting message bundle: %w", err)
	}
	return nil
}

func (db *database) updateMessageBundleEndpointStates(currentState, desiredState int) error {
	if _, err := db.Tx.Exec("UPDATE _message_bundle_endpoints set state = $1 where state = $2", desiredState, currentState); err != nil {
		return fmt.Errorf("messaging: error updating message bundle endpoint states: %w", err)
	}
	return nil
}

func (db *database) makeMessageBundleEndpointsReady(url string) (bool, error) {
	res, err := db.Tx.Exec("UPDATE _message_bundle_endpoints set state = $1 where url = $2 and (state = $3 or state = $4)", EndpointStateReady, url, EndpointStateNotReady, EndpointStatePreflightFailed)
	if err != nil {
		return false, fmt.Errorf("messaging: error updating direct message endpoint states: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows != 0, nil
}

func (db *database) updateDirectMessageEndpointStates(currentState, desiredState int) error {
	if _, err := db.Tx.Exec("UPDATE _direct_message_endpoints set state = $1 where state = $2", desiredState, currentState); err != nil {
		return fmt.Errorf("messaging: error updating direct message endpoint states: %w", err)
	}
	return nil
}

func (db *database) makeDirectMessageEndpointsReady(url string) (bool, error) {
	res, err := db.Tx.Exec("UPDATE _direct_message_endpoints set state = $1 where url = $2 and (state = $3 or state = $4)", EndpointStateReady, url, EndpointStateNotReady, EndpointStatePreflightFailed)
	if err != nil {
		return false, fmt.Errorf("messaging: error updating direct message endpoint states: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows != 0, nil
}

func (db *database) dirtyGroupMemberships() ([]*groupMembership, error) {
	var gms []*groupMembership
	if err := db.Tx.Select(&gms, "SELECT * FROM _group_memberships as gm where send_acks_version_written != send_acks_version_sent AND connection_state = $2", GroupMembershipConnectionStateConnected); err != nil {
		return nil, fmt.Errorf("messaging: error getting dirty group memberships: %w", err)
	}
	for _, gm := range gms {
		gm.loadAcks()
	}
	return gms, nil
}

func (db *database) groupMemberCount(id []byte) (uint, error) {
	counter := &struct {
		Count uint `db:"member_count"`
	}{Count: 0}
	if err := db.Tx.Get(counter, "SELECT count(*) as member_count FROM _group_memberships WHERE group_id = $1", id); err != nil {
		return 0, fmt.Errorf("messaging: error counting group members: %w", err)
	}

	return counter.Count, nil
}

func (db *database) connectedGroupMemberCount(id []byte) (uint, error) {
	counter := &struct {
		Count uint `db:"member_count"`
	}{Count: 0}
	if err := db.Tx.Get(counter, "SELECT count(*) as member_count FROM _group_memberships WHERE group_id = $1 AND connection_state = $2", id, GroupMembershipConnectionStateConnected); err != nil {
		return 0, fmt.Errorf("messaging: error counting connected group members: %w", err)
	}

	return counter.Count, nil
}

func (db *database) unbundledCount(id []byte) (uint, error) {
	counter := &struct {
		Count uint `db:"message_count"`
	}{Count: 0}
	if err := db.Tx.Get(counter, "SELECT count(*) as message_count FROM _group_messages WHERE group_id = $1 AND seq in (select distinct seq from _group_message_members where group_id = _group_messages.group_id and bundle_id IS NULL)", id); err != nil {
		return 0, fmt.Errorf("messaging: error counting unbundled messages: %w", err)
	}
	return counter.Count, nil
}

func (db *database) ackedMemberCount(id []byte, seq uint64) (uint, error) {
	counter := &struct {
		Count uint `db:"group_memberships_count"`
	}{Count: 0}
	if err := db.Tx.Get(counter, "SELECT count(*) as group_memberships_count FROM _group_memberships WHERE group_id = $1 AND (recv_group_ack_sparse = x'' OR recv_group_ack_sparse IS NULL) AND recv_group_ack_seq = $2 AND (recv_private_ack_sparse = x'' OR recv_private_ack_sparse IS NULL) AND recv_private_ack_seq = send_private_seq", id, seq); err != nil {
		return 0, fmt.Errorf("messaging: error counting acked members: %w", err)
	}

	return counter.Count, nil
}

func (db *database) upsertBackfillSink(bs *backfillSink) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _backfill_sinks (id, type, group_id, identity_id, membership_id, started, completed, total, expected_total) VALUES (:id, :type, :group_id, :identity_id, :membership_id, :started, :completed, :total, :expected_total) ON CONFLICT(id) DO UPDATE SET type = :type, group_id = :group_id, identity_id = :identity_id, membership_id = :membership_id, started = :started, completed = :completed, total = :total, expected_total = :expected_total", bs); err != nil {
		return fmt.Errorf("messaging: error upserting backfill sink: %w", err)
	}
	return nil
}

func (db *database) backfillSinkOrNil(id []byte) (*backfillSink, error) {
	bs := &backfillSink{}
	if err := db.Tx.Get(bs, "select * FROM _backfill_sinks where id = $1", id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return bs, nil
}

func (db *database) deleteBackfillSink(id []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _backfill_sinks where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting backfill sink: %w", err)
	}
	return nil
}

func (db *database) upsertBackfillSource(bs *backfillSource) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _backfill_sources (id, request_id, type, from_self, group_id, identity_id, membership_id, max_id, total) VALUES (:id, :request_id, :type, :from_self, :group_id, :identity_id, :membership_id, :max_id, :total) ON CONFLICT(id) DO UPDATE SET request_id = :request_id, type = :type, from_self = :from_self, group_id = :group_id, identity_id = :identity_id, membership_id = :membership_id, max_id = :max_id, total = :total", bs); err != nil {
		return fmt.Errorf("messaging: error upserting backfill source: %w", err)
	}
	return nil
}

func (db *database) backfillSource(id []byte) (*backfillSource, error) {
	bs := &backfillSource{}
	if err := db.Tx.Get(bs, "select * FROM _backfill_sources where id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting backfill source: %w", err)
	}
	return bs, nil
}

func (db *database) readyBackfillSources() ([]*backfillSource, error) {
	var bfs []*backfillSource
	if err := db.Tx.Select(&bfs, "SELECT * FROM _backfill_sources"); err != nil {
		return nil, fmt.Errorf("messaging: error getting ready backfill sources: %w", err)
	}
	return bfs, nil
}

func (db *database) deleteBackfillSource(id []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _backfill_sources where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting backfill source: %w", err)
	}
	return nil
}

func (db *database) groupMembershipsConnectionState(state int) ([]*groupMembership, error) {
	var gms []*groupMembership
	if err := db.Tx.Select(&gms, "SELECT * FROM _group_memberships where connection_state = $1", state); err != nil {
		return nil, fmt.Errorf("messaging: error getting group memberships for state: %w", err)
	}
	for _, gm := range gms {
		gm.loadAcks()
	}
	return gms, nil
}

func (db *database) groupMembershipsBackfillState(state int) ([]*groupMembership, error) {
	var gms []*groupMembership
	if err := db.Tx.Select(&gms, "SELECT * FROM _group_memberships where connection_state = $1 AND backfill_state = $2", GroupMembershipConnectionStateConnected, state); err != nil {
		return nil, fmt.Errorf("messaging: error getting group memberships for backfill state: %w", err)
	}
	for _, gm := range gms {
		gm.loadAcks()
	}
	return gms, nil
}

func (db *database) groupMembershipsForBackfillSinkID(groupID, id []byte) ([]*groupMembership, error) {
	var gms []*groupMembership
	if err := db.Tx.Select(&gms, "SELECT * FROM _group_memberships where group_id = $1 AND backfill_sink_id = $2", groupID, id); err != nil {
		return nil, fmt.Errorf("messaging: error getting group memberships for backfill sink id: %w", err)
	}
	for _, gm := range gms {
		gm.loadAcks()
	}
	return gms, nil
}

func (db *database) upsertDeferredMessage(dm *deferredMessage) error {
	if _, err := db.Tx.NamedExec("INSERT INTO _deferred_messages (id, url, body, ctime_ms) VALUES (:id, :url, :body, :ctime_ms) ON CONFLICT(id) DO UPDATE SET url = :url, body = :body, ctime_ms = :ctime_ms", dm); err != nil {
		return fmt.Errorf("messaging: error upserting deferred message: %w", err)
	}
	return nil
}

func (db *database) trimDeferredMessages(url string) error {
	if _, err := db.Tx.Exec("DELETE FROM _deferred_messages WHERE id NOT IN (SELECT id FROM _deferred_messages WHERE url = $1 ORDER BY url, ctime_ms DESC LIMIT 10)", url); err != nil {
		return fmt.Errorf("messaging: error upserting backfill sink: %w", err)
	}
	if _, err := db.Tx.Exec("DELETE FROM _deferred_messages WHERE id NOT IN (SELECT id FROM _deferred_messages ORDER BY url, ctime_ms DESC LIMIT 50)"); err != nil {
		return fmt.Errorf("messaging: error upserting backfill sink: %w", err)
	}
	return nil
}

func (db *database) deferredMessages(url []string) ([]*deferredMessage, error) {
	var dms []*deferredMessage
	query, vs, err := sqlx.In("SELECT * FROM _deferred_messages WHERE url IN (?)", url)
	if err != nil {
		return dms, err
	}
	if err := db.Tx.Select(&dms, query, vs...); err != nil {
		return nil, fmt.Errorf("messaging: error getting deferred_messages: %w", err)
	}
	return dms, nil
}

func (db *database) deleteDeferredMessage(id []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _deferred_messages where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting deferred_messages: %w", err)
	}
	return nil
}

func (db *database) deleteDirectMessage(id []byte) error {
	if _, err := db.Tx.Exec("DELETE FROM _direct_messages where id = $1", id); err != nil {
		return fmt.Errorf("messaging: error deleting deferred_messages: %w", err)
	}
	return nil
}

func (db *database) guardedDeleteMessages() error {
	if _, err := db.Tx.Exec("DELETE FROM _group_messages AS gm WHERE NOT EXISTS (SELECT 1 FROM _group_message_members gmm WHERE gmm.group_id = gm.group_id AND gmm.seq = gm.seq)"); err != nil {
		return fmt.Errorf("messaging: error deleting group messages: %w", err)
	}
	if _, err := db.Tx.Exec("DELETE FROM _message_bundles AS mb WHERE delete_after_success = 0 AND NOT EXISTS (SELECT 1 FROM _group_message_members gmm WHERE gmm.bundle_id = mb.id UNION SELECT 1 FROM _private_messages pm WHERE pm.bundle_id = mb.id)"); err != nil {
		return fmt.Errorf("messaging: error deleting message bundles: %w", err)
	}
	return nil
}

func (db *database) guardedDeleteGroupDescriptions() error {
	if _, err := db.Tx.Exec(`DELETE FROM _group_descriptions WHERE digest not in (
		select desc_digest from _groups
		union
		select recv_desc_digest from _group_memberships
		union
		select desc_digest from _group_messages
		union
		select prev_send_desc_digest from _message_bundles
		union
		select next_send_desc_digest from _message_bundles
	)`); err != nil {
		return fmt.Errorf("messaging: error deleting group descriptions: %w", err)
	}
	return nil
}

func (db *database) unsetBundleIDForGroupMessageMembers(bundleID []byte) error {
	if _, err := db.Tx.Exec("UPDATE _group_message_members SET bundle_id = NULL where bundle_id = ?", bundleID); err != nil {
		return fmt.Errorf("messaging: error deleting group descriptions: %w", err)
	}
	return nil
}

func (db *database) unsetBundleIDForPrivateMessages(bundleID []byte) error {
	if _, err := db.Tx.Exec("UPDATE _private_messages SET bundle_id = NULL where bundle_id = ?", bundleID); err != nil {
		return fmt.Errorf("messaging: error deleting group descriptions: %w", err)
	}
	return nil
}

func (db *database) pendingMessageCount() (uint, error) {
	type count struct {
		Count uint `db:"message_count"`
	}

	unbundledCounter := &count{Count: 0}
	if err := db.Tx.Get(unbundledCounter, "SELECT count(*) as message_count FROM _group_messages WHERE (group_id, seq) in (select distinct group_id, seq from _group_message_members where group_id = _group_messages.group_id and bundle_id IS NULL)"); err != nil {
		return 0, fmt.Errorf("messaging: error counting unbundled messages: %w", err)
	}

	undeliveredBundleCounter := &count{Count: 0}
	if err := db.Tx.Get(undeliveredBundleCounter, "SELECT count(*) as message_count FROM _message_bundles WHERE state = ?", MessageStateUndelivered); err != nil {
		return 0, fmt.Errorf("messaging: error counting unbundled messages: %w", err)
	}

	undeliveredDirectCounter := &count{Count: 0}
	if err := db.Tx.Get(undeliveredDirectCounter, "SELECT count(*) as message_count FROM _direct_messages WHERE state = ?", MessageStateUndelivered); err != nil {
		return 0, fmt.Errorf("messaging: error counting unbundled messages: %w", err)
	}

	return unbundledCounter.Count + undeliveredBundleCounter.Count + undeliveredDirectCounter.Count, nil
}

func (db *database) updateGroupMembershipAckVersionsForUndelivered() error {
	if _, err := db.Tx.Exec("UPDATE _group_memberships SET send_acks_version_written = send_acks_version_written + 1 where (group_id, identity_id, membership_id) in (select group_id, identity_id, membership_id from _message_bundles where id in (select id from _message_bundle_endpoints where state in ($1, $2)))", EndpointStateDelivering, EndpointStatePreflightFailed); err != nil {
		return fmt.Errorf("messaging: error updating group membership acks for undelivered: %w", err)
	}
	return nil
}

func (db *database) setJpakeEndpoints(jpakeID []byte, endpoints map[string]*EndpointInfo) error {
	if _, err := db.Tx.Exec("DELETE FROM _jpake_endpoints where jpake_id = $1", jpakeID); err != nil {
		return fmt.Errorf("messaging: error deleting jpake endpoints: %w", err)
	}
	for url, ei := range endpoints {
		if _, err := db.Tx.Exec("INSERT INTO _jpake_endpoints (jpake_id, url, priority) VALUES ($1, $2, $3)", jpakeID, url, ei.Priority); err != nil {
			return fmt.Errorf("messaging: error inserting jpake endpoints: %w", err)
		}
	}
	return nil
}

func (db *database) jpakeEndpoints(id []byte) (map[string]*EndpointInfo, error) {
	var es []*jpakeEndpoint
	if err := db.Tx.Select(&es, "SELECT * FROM _jpake_endpoints WHERE jpake_id = $1", id); err != nil {
		return nil, fmt.Errorf("messaging: error getting jpake endpoints for jpake_id id: %w", err)
	}
	endpoints := make(map[string]*EndpointInfo, len(es))
	for _, e := range es {
		endpoints[e.URL] = &EndpointInfo{
			Priority: e.Priority,
		}
	}
	return endpoints, nil
}
