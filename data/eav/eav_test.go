package eav

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"github.com/stretchr/testify/require"
)

const currentTime = uint64(1351700038292387)

var (
	lastTimestampID uint64
	groupID         [16]byte
	password        = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
)

func NewID(authorID [7]byte) ids.ID {
	var id [16]byte
	currentMicro := lastTimestampID
	if currentMicro == lastTimestampID {
		currentMicro++
	}
	lastTimestampID = currentMicro
	binary.BigEndian.PutUint64(id[0:8], currentMicro)
	id[8] = 0
	copy(id[9:], authorID[:])
	return id
}

func TestMain(m *testing.M) {
	os.Remove("test1")
	os.Remove("test2")
	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func newEAV() *EAV {
	groupID = [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7}
	lastTimestampID = currentTime
	config := config.NewConfig()
	db, err := db.NewDatabase(config, clock.NewSystemClock(), "test1")
	if err != nil {
		panic(err)
	}
	if err := db.Initialize(password); err != nil {
		panic(err)
	}
	if err := db.Open(password); err != nil {
		panic(err)
	}

	eavDB, err := NewEAV(config, db, clock.NewSystemClock(), make(chan interface{}, 100))
	if err != nil {
		panic(err)
	}
	return eavDB
}

func shutdownEAV(eav *EAV) {
	if err := eav.db.Shutdown(); err != nil {
		panic(err)
	}
	os.Remove("test1")
	os.Remove("test2")
}

func TestApplyOperationsBeforeSchema(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))
		require.Nil(err)

		result, err := eav.Query("select id, body, age, height, blob from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("hi there", result.Rows[0][1])

		return nil
	}))
}

func TestCreateTableTwice(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		return nil
	}))
}

func TestApplyOperationsAfterSchema(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)

		result, err := eav.Query("select id, body, age, height, blob from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("hi there", result.Rows[0][1])

		return nil
	}))
}

func TestApplyOperationsOnCreate(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)

		_, _, err = eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 1, "message_body", "hi there2").
			AddBytes(entityID, 1, "message_blob", []byte{0, 1, 2, 4}).
			AddInt64(entityID, 1, "message_age", 25).
			AddFloat64(entityID, 1, "message_height", 24.24))
		require.Nil(err)

		// result, err := eav.Query("select id, body, age, height, blob from messages")
		// require.Nil(err)
		// require.Equal(1, len(result.Rows))
		// require.Equal("hi there", result.Rows[0][1])

		return nil
	}))
}

func TestApplyPartialOperation(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)

		result, err := eav.Query("select id, body, age, height, blob from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("hi there", result.Rows[0][1])

		_, _, err = eav.Apply(groupID, Self, NewOperations().AddString(entityID, 1, "message_body", "hi there2"))
		require.Nil(err)
		result, err = eav.Query("select id, body, age, height, blob from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("hi there2", result.Rows[0][1])

		return nil
	}))
}

func TestGeneratedColumns(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, currentTime, "message_body", "hi there"))
		require.Nil(err)
		result, err := eav.Query("select datetime(_ctime, 'unixepoch'), _mtime, _identity_tag, _membership_tag from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("2012-10-31 16:13:58", result.Rows[0][0])
		require.Equal(1351700038.292387, result.Rows[0][1])
		require.Equal([]byte{1, 2, 3, 4}, result.Rows[0][2])
		require.Equal([]byte{5, 6, 7}, result.Rows[0][3])
		_, _, err = eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, currentTime+1000000, "message_body", "hi there more"))
		require.Nil(err)
		result, err = eav.Query("select datetime(_ctime, 'unixepoch'), _mtime, _identity_tag, _membership_tag from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("2012-10-31 16:13:58", result.Rows[0][0])
		require.Equal(1351700039.292387, result.Rows[0][1])
		return nil
	}))
}

func TestApplyPrivateNames(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "_private_message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "_private_message_body", "hi there"))
		require.Nil(err)

		result, err := eav.Query("select id, body from messages")
		require.Nil(err)
		require.Equal(0, len(result.Rows))
		_, _, err = eav.Apply(groupID, Private, NewOperations().
			AddString(entityID, 0, "_private_message_body", "hi there"))
		require.Nil(err)

		result, err = eav.Query("select id, body from messages")
		require.Nil(err)
		require.Equal(1, len(result.Rows))
		require.Equal("hi there", result.Rows[0][1])

		return nil
	}))
}

func TestExtractIDInfo(t *testing.T) {
	require := require.New(t)
	lastTimestampID = currentTime
	id1 := NewID([7]byte{0, 0, 0, 0, 0, 0, 0})
	tsMicro := extractUnixMicro(id1[:])
	require.InDelta(float64(currentTime)/1000000, tsMicro, 0.000001)
}

func TestSelect(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)

		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		type message struct {
			ID       []byte  `db:"id"`
			Body     string  `db:"body"`
			Age      int     `db:"age"`
			Height   float64 `db:"height"`
			Blob     []byte  `db:"blob"`
			CtimeSec uint64  `db:"_ctime"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body, age, height, blob from messages"))
		require.Equal(1, len(messages))
		require.Equal("hi there", messages[0].Body)

		return nil
	}))
}

func TestVisibilitySplitting(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		groupOps, selfOps, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "_private_message_body", "hi there").
			AddBytes(entityID, 0, "_self_message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)
		require.Equal(len(groupOps.nameMap), 2)
		require.Equal(groupOps.OperationMap, map[uint64]map[ids.ID]map[uint64]Value{
			0: {
				entityID: {
					groupOps.nameMap["message_age"]:    NewInt64Value(23),
					groupOps.nameMap["message_height"]: NewFloat64Value(23.23),
				},
			},
		})
		require.Equal(len(selfOps.nameMap), 1)
		require.Equal(selfOps.OperationMap, map[uint64]map[ids.ID]map[uint64]Value{
			0: {
				entityID: {
					selfOps.nameMap["_self_message_blob"]: NewBytesValue([]byte{0, 1, 2, 3}),
				},
			},
		})

		return nil
	}))
}

func TestAlterTable(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddBytes(entityID, 0, "message_blob", []byte{0, 1, 2, 3}).
			AddInt64(entityID, 0, "message_age", 23).
			AddFloat64(entityID, 0, "message_height", 23.23))
		require.Nil(err)
		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"age": {
					SourceName: "message_age",
					ColumnType: Int,
					Required:   true,
					Nullable:   false,
				},
				"blob": {
					SourceName: "message_blob",
					ColumnType: Blob,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: [][]string{{"body"}},
		}))

		type messageWithoutHeight struct {
			ID   []byte `db:"id"`
			Body string `db:"body"`
			Age  int    `db:"age"`
			Blob []byte `db:"blob"`
		}
		var messagesWithoutHeight []*messageWithoutHeight
		require.Nil(eav.Select(&messagesWithoutHeight, "select id, body, age, blob from messages"))
		require.Equal(1, len(messagesWithoutHeight))
		require.Equal("hi there", messagesWithoutHeight[0].Body)
		return nil
	}))
	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.AlterTableAddColumns("messages", map[string]*ColumnDefinition{
			"height": {
				SourceName: "message_height",
				ColumnType: Real,
				Nullable:   true,
			},
		}))
		return nil
	}))

	require.Nil(eav.db.Run("testing", func() error {
		type message struct {
			ID     []byte  `db:"id"`
			Body   string  `db:"body"`
			Age    int     `db:"age"`
			Height float64 `db:"height"`
			Blob   []byte  `db:"blob"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body, age, height, blob from messages"))
		require.Equal(1, len(messages))
		require.Equal("hi there", messages[0].Body)
		require.Equal(23.23, messages[0].Height)

		return nil
	}))
	def, err := eav.loadDefinitions()
	require.Nil(err)
	require.Equal(4, len(def["messages"].Columns))
	require.Equal(1, len(def["messages"].Indexes))
}

func TestUnicodeRoundtrip(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there 🫣"))
		require.Nil(err)

		require.Nil(eav.CreateTable("messages", &TableDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		type message struct {
			ID   []byte `db:"id"`
			Body string `db:"body"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body from messages"))
		require.Equal(1, len(messages))
		require.Equal("hi there 🫣", messages[0].Body)

		return nil
	}))
}
