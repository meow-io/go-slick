package eav

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	"github.com/meow-io/go-slick/internal/test"
	"github.com/stretchr/testify/require"
)

const currentTime = uint64(1351700038292387)

var (
	lastTimestampID uint64
	groupID         [16]byte
)

func microToFloat(ts uint64) float64 {
	return float64(ts) / 1000000
}

func extractUnixMicro(id []byte) float64 {
	return microToFloat(binary.BigEndian.Uint64(id[0:8]))
}

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
	// call flag.Parse() here if TestMain uses flags
	os.Exit(test.DBCleanup(m.Run))
}

func newEAV() *EAV {
	groupID = [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7}
	lastTimestampID = currentTime
	config := config.NewConfig()
	db := test.NewTestDatabase(config)
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
	test.DeleteAll("test-*")
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

func TestDefaultValueOnRequired(t *testing.T) {
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		require.Equal(groupOps.OperationMap, map[uint64]map[ids.ID]map[uint32]Value{
			0: {
				entityID: {
					groupOps.nameMap["message_age"]:    *NewInt64Value(23),
					groupOps.nameMap["message_height"]: *NewFloat64Value(23.23),
				},
			},
		})
		require.Equal(len(selfOps.nameMap), 1)
		require.Equal(selfOps.OperationMap, map[uint64]map[ids.ID]map[uint32]Value{
			0: {
				entityID: {
					selfOps.nameMap["_self_message_blob"]: *NewBytesValue([]byte{0, 1, 2, 3}),
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		createCount, dropCount, err := eav.CreateViewWithCounts("messages", &ViewDefinition{
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
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Nullable:   true,
				},
			},
			Indexes: [][]string{{"body"}, {"height"}},
		})
		require.Nil(err)
		require.Equal(1, createCount)
		require.Equal(0, dropCount)
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
	require.Equal(2, len(def["messages"].Indexes))
}

func TestAlterTablePK(t *testing.T) {
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
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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
		createCount, dropCount, err := eav.CreateViewWithCounts("messages", &ViewDefinition{
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
				"height": {
					SourceName: "message_height",
					ColumnType: Real,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: [][]string{{"body"}, {"height"}},
		})
		require.Nil(err)
		require.Equal(3, createCount)
		require.Equal(2, dropCount)
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
	require.Equal(2, len(def["messages"].Indexes))
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

		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

func TestBeforeInsertEntitySubscriber(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	done := make(chan bool, 1)
	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"emoji": {
					SourceName: "message_emoji",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		eav.SubscribeBeforeEntity(func(viewName string, groupID, id ids.ID) error {
			done <- true
			return nil
		}, true, "messages")

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddString(entityID, 0, "message_emoji", "🤣"))
		require.Nil(err)

		return nil
	}))
	<-done
}

func TestBeforeUpdateEntitySubscriber(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	done := make(chan bool, 1)
	entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"emoji": {
					SourceName: "message_emoji",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		eav.SubscribeBeforeEntity(func(viewName string, groupID, id ids.ID) error {
			done <- true
			return nil
		}, true, "messages")

		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddString(entityID, 0, "message_emoji", "🤣"))
		require.Nil(err)

		return nil
	}))
	<-done
	require.Nil(eav.db.Run("testing", func() error {
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 1, "message_body", "hi there again").
			AddString(entityID, 1, "message_emoji", "📍"))
		return err
	}))
	<-done
}

func TestAfterInsertEntitySubscriber(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	done := make(chan bool, 1)
	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
				"emoji": {
					SourceName: "message_emoji",
					ColumnType: Text,
					Required:   true,
					Nullable:   false,
				},
			},
			Indexes: make([][]string, 0),
		}))

		eav.SubscribeAfterEntity(func(viewName string, groupID, id ids.ID) {
			done <- true
		}, true, "messages")

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddString(entityID, 0, "message_emoji", "🤣"))
		require.Nil(err)

		return nil
	}))
	<-done
}

func TestBeforeInsertEntitySubscriberWithError(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

	require.Error(eav.db.Run("testing", func() error {
		eav.SubscribeBeforeEntity(func(viewName string, groupID, id ids.ID) error {
			return errors.New("nope")
		}, true, "messages")
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there"))
		return err
	}))

	require.Nil(eav.db.Run("testing", func() error {
		type message struct {
			ID   []byte `db:"id"`
			Body string `db:"body"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body from messages"))
		require.Equal(0, len(messages))
		return nil
	}))
}

func TestBeforeInsertViewSubscriber(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)
	count := 0
	done := make(chan bool, 1)
	require.Nil(eav.db.Run("testing", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
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

		eav.SubscribeBeforeView(func(viewName string) error {
			require.Equal(0, count)
			count++
			done <- true
			return nil
		}, true, "messages")

		entityID1 := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		entityID2 := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID1, 0, "message_body", "hi there").
			AddString(entityID2, 0, "message_body", "hi there"))
		require.Nil(err)

		return nil
	}))
	<-done
}

func TestBackfill(t *testing.T) {
	require := require.New(t)
	eav1 := newEAV()
	defer shutdownEAV(eav1)
	eav2 := newEAV()
	defer shutdownEAV(eav2)

	require.Nil(eav1.db.Run("populate data", func() error {
		ops := NewOperations()
		for i := 0; i != 1000; i++ {
			entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
			ops.AddString(entityID, 0, "message_body", "hi there")
		}
		if _, _, err := eav1.Apply(groupID, Self, ops); err != nil {
			return err
		}
		var body []byte
		var err error
		authorTag := [7]byte{0, 0, 0, 0, 0, 0, 0}
		nextID := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		done := false
		for !done {
			body, nextID, done, err = eav1.Backfill(groupID, authorTag, nextID, false, true)
			require.Nil(err)
			require.Nil(eav2.db.Run("process backfill", func() error {
				return eav2.ProcessBackfill(groupID, Self, body)
			}))
		}

		return nil
	}))

	require.Nil(eav2.db.Run("populate data", func() error {
		var count int
		require.Nil(eav2.db.Tx.Get(&count, "select count(*) from _eav_data"))
		require.Equal(1000, count)
		return nil
	}))
}

func TestRequiredNullable(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("test schema", func() error {
		require.Nil(eav.CreateView("messages", &ViewDefinition{
			Columns: map[string]*ColumnDefinition{
				"body": {
					SourceName: "message_body",
					ColumnType: Text,
					Required:   true,
					Nullable:   true,
				},
			},
			Indexes: make([][]string, 0),
		}))

		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddNil(entityID, 0, "message_body"))
		require.Nil(err)

		type message struct {
			ID   []byte  `db:"id"`
			Body *string `db:"body"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body from messages"))
		require.Equal(1, len(messages))
		return nil
	}))
}

func TestRequiredWithDefault(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("test schema", func() error {
		require.ErrorContains(
			eav.CreateView("messages", &ViewDefinition{
				Columns: map[string]*ColumnDefinition{
					"body": {
						SourceName:   "message_body",
						ColumnType:   Text,
						Required:     true,
						DefaultValue: &Value{true, []byte("hello")},
					},
				},
				Indexes: make([][]string, 0),
			}),
			"column body type cannot be required and have a default value")
		return nil
	}))
}

func TestOptionalWithDefault(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("test schema", func() error {
		require.Nil(
			eav.CreateView("messages", &ViewDefinition{
				Columns: map[string]*ColumnDefinition{
					"body": {
						SourceName: "message_body",
						ColumnType: Text,
						Required:   true,
					},
					"reaction": {
						SourceName:   "message_reaction",
						ColumnType:   Text,
						Required:     false,
						DefaultValue: &Value{true, []byte("hello")},
					},
				},
				Indexes: make([][]string, 0),
			}))
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there"))
		require.Nil(err)

		type message struct {
			ID       []byte `db:"id"`
			Body     string `db:"body"`
			Reaction string `db:"reaction"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body, reaction from messages"))
		require.Equal(1, len(messages))
		require.Equal("hello", messages[0].Reaction)
		return nil
	}))
}

func TestOptionalWithDefaultWithExplictlyWrittenNull(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("test schema", func() error {
		require.Nil(
			eav.CreateView("messages", &ViewDefinition{
				Columns: map[string]*ColumnDefinition{
					"body": {
						SourceName: "message_body",
						ColumnType: Text,
						Required:   true,
					},
					"reaction": {
						SourceName:   "message_reaction",
						ColumnType:   Text,
						Required:     false,
						Nullable:     true,
						DefaultValue: &Value{true, []byte("hello")},
					},
				},
				Indexes: make([][]string, 0),
			}))
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there").
			AddNil(entityID, 0, "message_reaction"))
		require.Nil(err)

		type message struct {
			ID       []byte  `db:"id"`
			Body     string  `db:"body"`
			Reaction *string `db:"reaction"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body, reaction from messages"))
		require.Equal(1, len(messages))
		require.Nil(messages[0].Reaction)
		return nil
	}))
}

func TestOptionalWithDefaultWithoutExplictlyWrittenNull(t *testing.T) {
	require := require.New(t)
	eav := newEAV()
	defer shutdownEAV(eav)

	require.Nil(eav.db.Run("test schema", func() error {
		require.Nil(
			eav.CreateView("messages", &ViewDefinition{
				Columns: map[string]*ColumnDefinition{
					"body": {
						SourceName: "message_body",
						ColumnType: Text,
						Required:   true,
					},
					"reaction": {
						SourceName:   "message_reaction",
						ColumnType:   Text,
						Required:     false,
						Nullable:     true,
						DefaultValue: &Value{true, []byte("hello")},
					},
				},
				Indexes: make([][]string, 0),
			}))
		entityID := NewID([7]byte{1, 2, 3, 4, 5, 6, 7})
		_, _, err := eav.Apply(groupID, Self, NewOperations().
			AddString(entityID, 0, "message_body", "hi there"))
		require.Nil(err)

		type message struct {
			ID       []byte  `db:"id"`
			Body     string  `db:"body"`
			Reaction *string `db:"reaction"`
		}
		var messages []*message
		require.Nil(eav.Select(&messages, "select id, body, reaction from messages"))
		require.Equal(1, len(messages))
		require.Equal("hello", *messages[0].Reaction)
		return nil
	}))
}
