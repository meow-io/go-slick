package slick

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/data/eav"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/test"
	"github.com/meow-io/go-slick/messaging"
	"github.com/meow-io/go-slick/migration"
	"github.com/stretchr/testify/require"
)

var (
	password1 = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
	password2 = []byte{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 30}
)

func TestMain(m *testing.M) {
	test.DeleteAll("s1")
	test.DeleteAll("s2")
	os.Exit(m.Run())
}

func newSlick(p string) *Slick {
	c := config.NewConfig(
		config.WithRootDir(p),
		config.WithLoggingPrefix(p),
		config.WithAckWaitTimeMs(0),
		config.WithGroupMessageWaitTimeMs(0),
		config.WithPrivateMessageWaitTimeMs(0),
	)

	r, err := NewSlick(c)
	if err != nil {
		panic(err)
	}
	return r
}

func teardownSlick(r *Slick) {
	if err := r.Shutdown(); err != nil {
		panic(err)
	}
	test.DeleteAll(r.config.RootDir)
}

func waitSync(r *Slick, groupID ids.ID, numNodes uint) {
	state, err := r.GroupState(groupID)
	if err != nil {
		panic(err)
	}

	if state.GroupState == 2 && state.AckedMemberCount == numNodes {
		return
	}

	for u := range r.Updates() {
		if update, ok := u.(*GroupUpdate); ok {
			if update.AckedMemberCount == numNodes && update.GroupState == 2 {
				return
			}
		}
	}
}

func TestSameSlickTwice(t *testing.T) {
	require := require.New(t)

	s1 := newSlick("s1")
	defer teardownSlick(s1)

	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	s2 := newSlick("s1")
	defer teardownSlick(s2)

	require.True(s2.Initialized())
	require.ErrorContains(s2.Open(password1), "database is locked")
}

func TestTwoPartySlick(t *testing.T) {
	require := require.New(t)

	s1 := newSlick("s1")
	defer teardownSlick(s1)
	s2 := newSlick("s2")
	defer teardownSlick(s2)

	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	require.False(s2.Initialized())
	require.Nil(s2.Initialize(password2))

	id, err := s1.CreateGroup("group1")
	require.Nil(err)

	s1groups, err := s1.Groups()
	require.Nil(err)

	invite, err := s1.Invite(id, "password")
	require.Nil(err)
	_, err = s2.AcceptInvite(invite, "password")
	require.Nil(err)
	require.Nil(s1.DB.Migrate("something", []*migration.Migration{
		{
			Name: "init",
			Func: func(*sql.Tx) error {
				return s1.EAVCreateTable("messages", &eav.TableDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"body": {
							SourceName:   "body",
							ColumnType:   eav.Text,
							DefaultValue: eav.NewBytesValue([]byte("something")),
							Required:     true,
							Nullable:     false,
						},
						"published_at": {
							SourceName:   "published_at",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     true,
							Nullable:     false,
						},
						"read": {
							SourceName:   "_self_read",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     false,
							Nullable:     false,
						},
					},
					Indexes: [][]string{{"published_at"}},
				})
			},
		},
	}))
	waitSync(s1, id, 1)

	writer := s1.EAVWriter(s1groups[0])
	writer.Insert("messages", map[string]interface{}{
		"body":         "hello",
		"published_at": 1,
		"read":         true,
	})
	writer.Insert("messages", map[string]interface{}{
		"body":         "there",
		"published_at": 2,
		"read":         true,
	})
	require.Nil(writer.Execute())
	result1, err := s1.EAVQuery("select id, body, published_at, read from messages WHERE group_id = ? order by published_at", s1groups[0].ID[:])
	require.Nil(err)
	require.Equal(writer.InsertIDs[0][:], result1.Rows[0][0])
	require.Equal("hello", result1.Rows[0][1])
	require.Equal(int64(1), result1.Rows[0][3])
	require.Equal(writer.InsertIDs[1][:], result1.Rows[1][0])
	require.Equal("there", result1.Rows[1][1])
	require.Equal(int64(1), result1.Rows[1][3])

	waitSync(s1, id, 1)

	r2groups, err := s2.Groups()
	require.Nil(err)

	require.Nil(s2.DB.Migrate("something", []*migration.Migration{
		{
			Name: "init",
			Func: func(*sql.Tx) error {
				return s2.EAVCreateTable("messages", &eav.TableDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"body": {
							SourceName:   "body",
							ColumnType:   eav.Text,
							DefaultValue: eav.NewBytesValue([]byte("something")),
							Required:     true,
							Nullable:     false,
						},
						"published_at": {
							SourceName:   "published_at",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     true,
							Nullable:     false,
						},
						"read": {
							SourceName:   "_self_read",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     false,
							Nullable:     false,
						},
					},
					Indexes: [][]string{{"published_at"}},
				})
			},
		},
	}))

	result2, err := s2.EAVQuery("select id, body, published_at, read from messages WHERE group_id = ? order by published_at", r2groups[0].ID[:])
	require.Nil(err)
	require.Equal("hello", result2.Rows[0][1])
	require.Equal(int64(0), result2.Rows[0][3])
	require.Equal("there", result2.Rows[1][1])
	require.Equal(int64(0), result2.Rows[1][3])
}

func TestWrongPassword(t *testing.T) {
	require := require.New(t)

	s1 := newSlick("s1")
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))
	require.Nil(s1.Shutdown())
	s1 = newSlick("s1")
	require.Error(s1.Open(password2))
	defer teardownSlick(s1)
}

func TestRestart(t *testing.T) {
	require := require.New(t)

	s1 := newSlick("s1")
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))
	require.Nil(s1.Shutdown())
	require.Nil(s1.Open(password1))
	require.Nil(s1.DeviceGroup.SetNameType("name1", "type1"))
	defer teardownSlick(s1)
}

func TestLargeBackfill(t *testing.T) {
	require := require.New(t)
	s1 := newSlick("s1")
	defer teardownSlick(s1)
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	_, err := s1.CreateGroup("group1")
	require.Nil(err)

	s1groups, err := s1.Groups()
	require.Nil(err)

	ts := s1.clock.CurrentTimeMicro()

	for i := 0; i != 9; i++ {
		ops := eav.NewOperations()
		id, err := s1.NewID(s1groups[0].AuthorTag)
		require.Nil(err)
		for j := 0; j != 1001; j++ {
			ops.AddString(id, ts, fmt.Sprintf("attr-%d", j), "hi there")
		}
		require.Nil(s1.EAVWrite(s1groups[0].ID, ops))
	}

	r2 := newSlick("s2")
	defer teardownSlick(r2)

	require.False(r2.Initialized())
	require.Nil(r2.Initialize(password2))

	invite, err := s1.Invite(s1groups[0].ID, "password")
	require.Nil(err)
	_, err = r2.AcceptInvite(invite, "password")
	require.Nil(err)

	waitSync(s1, s1groups[0].ID, 1)

	require.Eventually(func() bool {
		r2groups, err := r2.Groups()
		require.Nil(err)
		return len(r2groups) == 1
	}, 2*time.Second, 50*time.Millisecond)

	r2groups, err := r2.Groups()
	require.Nil(err)

	waitSync(r2, r2groups[0].ID, 1)

	result, err := r2.EAVQuery("select count(*) from _eav_data where group_id = ?", r2groups[0].ID[:])
	require.Nil(err)
	require.Equal(int64(9009), result.Rows[0][0])
}

func TestDeviceGroupJoining(t *testing.T) {
	require := require.New(t)

	s1 := newSlick("s1")
	defer teardownSlick(s1)
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	_, err := s1.CreateGroup("group1")
	require.Nil(err)

	s1groups, err := s1.Groups()
	require.Nil(err)

	require.Nil(s1.DB.Migrate("something", []*migration.Migration{
		{
			Name: "init",
			Func: func(*sql.Tx) error {
				return s1.EAVCreateTable("messages", &eav.TableDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"body": {
							SourceName:   "body",
							ColumnType:   eav.Text,
							DefaultValue: eav.NewBytesValue([]byte("something")),
							Required:     true,
							Nullable:     false,
						},
						"published_at": {
							SourceName:   "published_at",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     true,
							Nullable:     false,
						},
						"read": {
							SourceName:   "_self_read",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     false,
							Nullable:     false,
						},
					},
					Indexes: [][]string{{"published_at"}},
				})
			},
		},
	}))

	writer := s1.EAVWriter(s1groups[0])
	writer.Insert("messages", map[string]interface{}{
		"body":         "hello",
		"published_at": 1,
		"read":         true,
	})
	writer.Insert("messages", map[string]interface{}{
		"body":         "there",
		"published_at": 2,
		"read":         true,
	})
	require.Nil(writer.Execute())

	result1, err := s1.EAVQuery("select id, body, published_at, read from messages WHERE group_id = ? order by published_at", s1groups[0].ID[:])
	require.Nil(err)
	require.Equal(writer.InsertIDs[0][:], result1.Rows[0][0])
	require.Equal("hello", result1.Rows[0][1])
	require.Equal(int64(1), result1.Rows[0][3])
	require.Equal(writer.InsertIDs[1][:], result1.Rows[1][0])
	require.Equal("there", result1.Rows[1][1])
	require.Equal(int64(1), result1.Rows[1][3])

	r2 := newSlick("s2")
	r2key, err := r2.NewKey("password")
	require.Nil(err)
	defer teardownSlick(r2)

	require.False(r2.Initialized())
	require.Nil(r2.Initialize(r2key))

	require.Nil(s1.DeviceGroup.SetNameType("name1", "type1"))
	require.Nil(r2.DeviceGroup.SetNameType("name2", "type2"))

	groups, err := r2.Groups()
	require.Nil(err)
	require.Equal(len(groups), 0)

	c1, err := s1.DeviceGroup.Count()
	require.Nil(err)
	require.Equal(0, c1)

	c2, err := r2.DeviceGroup.Count()
	require.Nil(err)
	require.Equal(0, c2)

	link, err := s1.DeviceGroup.GetDeviceLink()
	require.Nil(err)

	require.Nil(r2.DeviceGroup.LinkDevice(link))

	require.Eventually(func() bool {
		d1, err := s1.DeviceGroup.Devices()
		require.Nil(err)
		return len(d1) == 2
	}, 2*time.Second, 50*time.Millisecond)

	require.Eventually(func() bool {
		c1, err = s1.DeviceGroup.Count()
		require.Nil(err)
		return c1 == 1
	}, 2*time.Second, 50*time.Millisecond)

	require.Eventually(func() bool {
		d2, err := r2.DeviceGroup.Devices()
		require.Nil(err)
		return len(d2) == 2
	}, 2*time.Second, 50*time.Millisecond)

	require.Eventually(func() bool {
		c2, err = r2.DeviceGroup.Count()
		require.Nil(err)
		return c2 == 1
	}, 2*time.Second, 50*time.Millisecond)

	require.Eventually(func() bool {
		groups, err := r2.Groups()
		require.Nil(err)
		return len(groups) == 1 && groups[0].State == messaging.GroupStateSynced
	}, 2*time.Second, 50*time.Millisecond)

	r2groups, err := r2.Groups()
	require.Nil(err)

	require.Nil(r2.DB.Migrate("something", []*migration.Migration{
		{
			Name: "init",
			Func: func(*sql.Tx) error {
				return r2.EAVCreateTable("messages", &eav.TableDefinition{
					Columns: map[string]*eav.ColumnDefinition{
						"body": {
							SourceName:   "body",
							ColumnType:   eav.Text,
							DefaultValue: eav.NewBytesValue([]byte("something")),
							Required:     true,
							Nullable:     false,
						},
						"published_at": {
							SourceName:   "published_at",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     true,
							Nullable:     false,
						},
						"read": {
							SourceName:   "_self_read",
							ColumnType:   eav.Int,
							DefaultValue: eav.NewBytesValue([]byte("0")),
							Required:     false,
							Nullable:     false,
						},
					},
					Indexes: [][]string{{"published_at"}},
				})
			},
		},
	}))

	result2, err := r2.EAVQuery("select id, body, published_at, read from messages WHERE group_id = ? order by published_at", r2groups[0].ID[:])
	require.Nil(err)
	require.Equal("hello", result2.Rows[0][1])
	require.Equal(int64(1), result2.Rows[0][3])
	require.Equal("there", result2.Rows[1][1])
	require.Equal(int64(1), result2.Rows[1][3])

	require.Equal(s1groups[0].ID, r2groups[0].ID)
}

func TestDeviceGroupSettingTwice(t *testing.T) {
	require := require.New(t)
	s1 := newSlick("s1")
	defer teardownSlick(s1)
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	require.Nil(s1.DeviceGroup.SetNameType("name1", "type1"))
	c1, err := s1.DeviceGroup.Count()
	require.Nil(err)
	require.Equal(0, c1)
	require.Nil(s1.DeviceGroup.SetNameType("name2", "type2"))
	c1, err = s1.DeviceGroup.Count()
	require.Nil(err)
	require.Equal(0, c1)
	dn, dt, err := s1.DeviceGroup.NameType()
	require.Nil(err)
	require.Equal("name2", dn)
	require.Equal("type2", dt)
}

func TestRoundTripInvite(t *testing.T) {
	require := require.New(t)
	s1 := newSlick("s1")
	defer teardownSlick(s1)
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	id, err := s1.CreateGroup("group1")
	require.Nil(err)

	invite, err := s1.Invite(id, "password")
	require.Nil(err)

	compactInvite, err := SerializeInvite(invite)
	require.Nil(err)

	inviteRoundtrip, err := DeserializeInvite(compactInvite)
	require.Nil(err)

	require.Equal(invite, inviteRoundtrip)
}

func TestRoundTripDeviceInvite(t *testing.T) {
	require := require.New(t)
	s1 := newSlick("s1")
	defer teardownSlick(s1)
	require.False(s1.Initialized())
	require.Nil(s1.Initialize(password1))

	invite, err := s1.DeviceGroup.GetDeviceLink()
	require.Nil(err)

	compactInvite, err := SerializeDeviceInvite(invite)
	require.Nil(err)

	inviteRoundtrip, err := DeserializeDeviceInvite(compactInvite)
	require.Nil(err)

	require.Equal(invite, inviteRoundtrip)
}
