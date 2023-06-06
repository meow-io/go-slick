package eav

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/ids"
	db "github.com/meow-io/go-slick/internal/db"
	"github.com/meow-io/go-slick/migration"
	"go.uber.org/zap"
)

const (
	Blob uint8 = iota
	Text
	Int
	Real
)

const (
	Private uint8 = 0
	Self    uint8 = 1
	Other   uint8 = 2
)

const (
	flagsPresent = 1
)

type beforeApplyCallback func(groupID, entityID ids.ID) error

type TableUpdate struct {
	GroupID ids.ID
	Name    string
}

type TableRowUpdate struct {
	GroupID ids.ID
	ID      ids.ID
	Name    string
	Vals    map[string]interface{}
}

type TableRowInsert struct {
	GroupID ids.ID
	ID      ids.ID
	Name    string
	Vals    map[string]interface{}
}

type ColumnDefinition struct {
	SourceName   string
	ColumnType   uint8
	DefaultValue Value
	Required     bool
	Nullable     bool
}

type Indexer interface {
	Start() error
	Index(groupID, id ids.ID, tablename string, values map[string]interface{}) error
}

type TableDefinition struct {
	Columns map[string]*ColumnDefinition
	Indexes [][]string
}

type Value struct {
	Present bool   `bencode:"p"`
	Bytes   []byte `bencode:"b"`
}

type backfill struct {
	Names []string `bencode:"n"`
	Data  [][]byte `bencode:"d"`
}

func microToFloat(ts uint64) float64 {
	return float64(ts) / 1000000
}

func extractUnixMicro(id []byte) float64 {
	return microToFloat(binary.BigEndian.Uint64(id[0:8]))
}

func NewValue(src interface{}) Value {
	v, err := NewValueWithError(src)
	if err != nil {
		panic(err)
	}
	return v
}

func NewBytesValue(v []byte) Value {
	return Value{true, v}
}

func NewFloat64Value(v float64) Value {
	return Value{true, []byte(strconv.FormatFloat(v, 'f', -1, 64))}
}

func NewInt64Value(v int64) Value {
	return Value{true, []byte(strconv.FormatInt(v, 10))}
}

func NewIntValue(v int) Value {
	return NewInt64Value(int64(v))
}

func NewUint64Value(v uint64) Value {
	return Value{true, []byte(strconv.FormatUint(v, 10))}
}

func NewUintValue(v uint) Value {
	return NewUint64Value(uint64(v))
}

func NewStringValue(v string) Value {
	return Value{true, []byte(v)}
}

func NewBoolValue(v bool) Value {
	if v {
		return Value{true, []byte("1")}
	}
	return Value{true, []byte("0")}
}

func NewValueWithError(src interface{}) (Value, error) {
	if src == nil {
		return Value{false, nil}, nil
	}
	switch t := src.(type) {
	case float64:
		return NewFloat64Value(t), nil
	case int64:
		return NewInt64Value(t), nil
	case int:
		return NewIntValue(t), nil
	case uint64:
		return NewUint64Value(t), nil
	case uint:
		return NewUintValue(t), nil
	case string:
		return NewStringValue(t), nil
	case []byte:
		return NewBytesValue(t), nil
	case *[]byte:
		return NewBytesValue(*t), nil
	case bool:
		return NewBoolValue(t), nil
	default:
		return Value{}, fmt.Errorf("unrecognized type %T", src)
	}
}

func (v Value) BytePointer() *[]byte {
	if !v.Present {
		return nil
	}
	b := v.Bytes
	return &b
}

func (v Value) NativeType(c *ColumnDefinition) interface{} {
	if !v.Present {
		return nil
	}

	switch c.ColumnType {
	case Blob:
		return v.Bytes
	case Text:
		return string(v.Bytes)
	case Real:
		f, err := strconv.ParseFloat(string(v.Bytes), 64)
		if err != nil {
			return c.DefaultValue.NativeType(c)
		}
		return f
	case Int:
		i, err := strconv.ParseInt(string(v.Bytes), 10, 64)
		if err != nil {
			return c.DefaultValue.NativeType(c)
		}
		return i
	default:
		panic("unrecognized type")
	}
}

type Operations struct {
	Names        []string                               `bencode:"n"`
	OperationMap map[uint64]map[ids.ID]map[uint64]Value `bencode:"m"`

	fromBackfill bool
	nameMap      map[string]uint64
}

func NewOperations() *Operations {
	return &Operations{
		Names:        make([]string, 0),
		OperationMap: make(map[uint64]map[ids.ID]map[uint64]Value),
		nameMap:      make(map[string]uint64),
	}
}

func (o *Operations) Empty() bool {
	return len(o.OperationMap) == 0
}

func (o *Operations) AddNil(id ids.ID, ts uint64, name string) *Operations {
	return o.Add(id, ts, name, Value{false, nil})
}

func (o *Operations) AddBytes(id ids.ID, ts uint64, name string, val []byte) *Operations {
	return o.Add(id, ts, name, Value{true, val})
}

func (o *Operations) AddString(id ids.ID, ts uint64, name string, val string) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(val)})
}

func (o *Operations) AddInt64(id ids.ID, ts uint64, name string, val int64) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(strconv.FormatInt(val, 10))})
}

func (o *Operations) AddFloat64(id ids.ID, ts uint64, name string, val float64) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(strconv.FormatFloat(val, 'f', -1, 64))})
}

func (o *Operations) Add(id ids.ID, ts uint64, name string, val Value) *Operations {
	if _, ok := o.OperationMap[ts]; !ok {
		o.OperationMap[ts] = make(map[ids.ID]map[uint64]Value)
	}
	if _, ok := o.OperationMap[ts][id]; !ok {
		o.OperationMap[ts][id] = make(map[uint64]Value)
	}
	if _, ok := o.nameMap[name]; !ok {
		nameIdx := len(o.Names)
		o.nameMap[name] = uint64(nameIdx)
		o.Names = append(o.Names, name)
	}
	o.OperationMap[ts][id][o.nameMap[name]] = val
	return o
}

func (o *Operations) AddMap(ts uint64, ops map[ids.ID]map[string]Value) *Operations {
	for id, idMap := range ops {
		for name, val := range idMap {
			o.Add(id, ts, name, val)
		}
	}
	return o
}

func DeserializeOps(b []byte) (*Operations, error) {
	o := &Operations{}
	if err := bencode.Deserialize(b, o); err != nil {
		return nil, err
	}
	return o, nil
}

func SerializeOps(ops *Operations) ([]byte, error) {
	return bencode.Serialize(ops)
}

type Result struct {
	Names []string
	Rows  [][]interface{}
}

type eavColumn struct {
	TableName    string  `db:"table_name"`
	TargetName   string  `db:"target_name"`
	SourceName   string  `db:"source_name"`
	ColumnType   uint8   `db:"column_type"`
	DefaultValue *[]byte `db:"default_value"`
	Required     bool    `db:"required"`
	Nullable     bool    `db:"nullable"`
}

type eavIndex struct {
	TableName string `db:"table_name"`
	IndexJSON string `db:"index_json"`
}

type eavName struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

type eavData struct {
	GroupID        []byte  `db:"group_id"`
	ID             []byte  `db:"id"`
	Name           uint64  `db:"name"`
	NamePermission uint8   `db:"name_permission"`
	Value          *[]byte `db:"value"`
	Ts             uint64  `db:"ts"`
}

type EAV struct {
	log            *zap.SugaredLogger
	config         *config.Config
	db             *db.Database
	clock          clock.Clock
	definitions    map[string]*TableDefinition
	nameCache      map[string]uint64
	sourceTableMap map[string][]string
	updates        chan interface{}
	beforeAppliers map[string][]beforeApplyCallback
}

func NewEAV(c *config.Config, d *db.Database, clock clock.Clock, updates chan interface{}) (*EAV, error) {
	log := c.Logger("data/eav")

	if err := d.MigrateNoLock("_eav", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
CREATE TABLE IF NOT EXISTS _eav_columns (
table_name STRING NOT NULL,
target_name STRING NOT NULL,
source_name STRING NOT NULL,
column_type INTEGER NOT NULL,
default_value BLOB,
required INTEGER NOT NULL,
nullable INTEGER NOT NULL,
PRIMARY KEY(table_name, target_name)
);

CREATE TABLE IF NOT EXISTS _eav_indexes (
table_name STRING NOT NULL,
index_json STRING NOT NULL,
PRIMARY KEY(table_name, index_json)
);

CREATE TABLE IF NOT EXISTS _eav_names (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name STRING NOT NULL
);
CREATE UNIQUE INDEX _eav_names_name on _eav_names (name);

CREATE TABLE IF NOT EXISTS _eav_data (
group_id BLOB NOT NULL,
id BLOB NOT NULL,
identity_tag BLOB AS (substr(id, 10, 4)),
membership_tag BLOB AS (substr(id, 14, 3)),
name INTEGER NOT NULL,
name_permission INTEGER NOT NULL,
value BLOB,
ts INTEGER NOT NULL,
PRIMARY KEY (name, group_id, id)
);
CREATE INDEX _eav_data_id on _eav_data (group_id, id);
				 `)
				return err
			},
		},
	},
	); err != nil {
		return nil, err
	}

	eav := &EAV{
		log:            log,
		config:         c,
		db:             d,
		clock:          clock,
		definitions:    make(map[string]*TableDefinition),
		nameCache:      make(map[string]uint64),
		updates:        updates,
		beforeAppliers: make(map[string][]beforeApplyCallback),
	}

	def, err := eav.loadDefinitions()
	if err != nil {
		return nil, err
	}
	eav.definitions = def

	eav.buildSourceNameTableMap()
	return eav, nil
}

func (eav *EAV) Schema(tablename string) (*TableDefinition, bool) {
	d, ok := eav.definitions[tablename]
	return d, ok
}

func (eav *EAV) AlterTableAddColumns(tableName string, columns map[string]*ColumnDefinition) error {
	schema, ok := eav.definitions[tableName]
	if !ok {
		return fmt.Errorf("unknown table %s", tableName)
	}

	// alter table add columns
	// fill columns
	// save new columns to db

	for name, col := range columns {
		if _, ok := schema.Columns[name]; ok {
			return fmt.Errorf("column exists already %s", name)
		}
		schema.Columns[name] = col
		// set schema writable PRAGMA writable_schema=ON.
		def, err := eav.buildColumnDefinition(name, col, true)
		if err != nil {
			return err
		}
		if _, err := eav.db.Tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD %s", tableName, def)); err != nil {
			return err
		}
	}

	// fill the new columns
	targetNameForID := make(map[uint64]string)
	nameIDs := make([]uint64, len(columns))
	i := 0
	for targetName, col := range columns {
		nameID, err := eav.idForName(col.SourceName)
		if err != nil {
			return err
		}
		nameIDs[i] = nameID
		i++
		targetNameForID[nameID] = targetName
	}

	// var id []byte
	mtime := uint64(0)
	values := make(map[ids.ID]map[string]interface{})
	lastID := make([]byte, 16)
	valueQuery, vs, err := sqlx.In("SELECT group_id, id, name, value, ts from _eav_data where name IN (?) ORDER BY id", nameIDs)
	if err != nil {
		return err
	}
	valueRows, err := eav.db.Tx.Queryx(valueQuery, vs...)
	if err != nil {
		return err
	}
	defer valueRows.Close()
	for valueRows.Next() {
		var v eavData
		err := valueRows.StructScan(&v)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			copy(lastID, v.ID)
		}
		if !bytes.Equal(lastID, v.ID) {
			for groupID, nameValues := range values {
				if err := eav.updateRow(columns, tableName, groupID[:], lastID[:], microToFloat(mtime), nameValues); err != nil {
					return fmt.Errorf("updating row: %w", err)
				}
			}
			values = make(map[ids.ID]map[string]interface{})
			mtime = 0
			copy(lastID, v.ID)
		}
		if mtime < v.Ts {
			mtime = v.Ts
		}
		col := columns[targetNameForID[v.Name]]
		groupID := ids.IDFromBytes(v.GroupID)
		if _, ok := values[groupID]; !ok {
			values[groupID] = make(map[string]interface{})
		}
		values[groupID][targetNameForID[v.Name]] = NewValue(v.Value).NativeType(col)

	}
	if len(values) != 0 {
		for groupID, nameValues := range values {
			if err := eav.updateRow(columns, tableName, groupID[:], lastID[:], microToFloat(mtime), nameValues); err != nil {
				return fmt.Errorf("updating row: %w", err)
			}
		}
	}

	// save new columns
	return eav.saveDefintion(tableName, schema)
}

func (eav *EAV) CreateTable(tableName string, schema *TableDefinition) error {
	if _, ok := eav.definitions[tableName]; ok {
		return nil
		// return fmt.Errorf("table definition already exists for %s", tableName)
	}

	eav.log.Debugf("rebuilding table for %s", tableName)
	nextTableName := fmt.Sprintf("%s_next", tableName)
	if _, err := eav.db.Tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", nextTableName)); err != nil {
		return err
	}

	columnDefinition, err := eav.buildColumnDefinitions(schema.Columns, false)
	if err != nil {
		return err
	}
	createTableStatement := fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (group_id, id))", nextTableName, columnDefinition)
	if _, err := eav.db.Tx.Exec(createTableStatement); err != nil {
		return err
	}

	targetNameForID := make(map[uint64]string)
	nameIDs := make([]uint64, len(schema.Columns))
	i := 0
	for targetName, col := range schema.Columns {
		nameID, err := eav.idForName(col.SourceName)
		if err != nil {
			return err
		}
		nameIDs[i] = nameID
		i++
		targetNameForID[nameID] = targetName
	}

	// var id []byte
	mtime := uint64(0)
	values := make(map[ids.ID]map[string]interface{})
	lastID := make([]byte, 16)
	valueQuery, vs, err := sqlx.In("SELECT group_id, id, name, value, ts from _eav_data where name IN (?) ORDER BY id", nameIDs)
	if err != nil {
		return err
	}
	valueRows, err := eav.db.Tx.Queryx(valueQuery, vs...)
	if err != nil {
		return err
	}
	defer valueRows.Close()
	for valueRows.Next() {
		var v eavData
		err := valueRows.StructScan(&v)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			copy(lastID, v.ID)
		}
		if !bytes.Equal(lastID, v.ID) {
			for groupID, nameValues := range values {
				if err := eav.insertRow(schema, nextTableName, groupID[:], lastID[:], microToFloat(mtime), nameValues); err != nil {
					return err
				}
			}
			values = make(map[ids.ID]map[string]interface{})
			mtime = 0
			copy(lastID, v.ID)
		}
		if mtime < v.Ts {
			mtime = v.Ts
		}
		col := schema.Columns[targetNameForID[v.Name]]
		groupID := ids.IDFromBytes(v.GroupID)
		if _, ok := values[groupID]; !ok {
			values[groupID] = make(map[string]interface{})
		}
		values[groupID][targetNameForID[v.Name]] = NewValue(v.Value).NativeType(col)

	}
	if len(values) != 0 {
		for groupID, nameValues := range values {
			if err := eav.insertRow(schema, nextTableName, groupID[:], lastID[:], microToFloat(mtime), nameValues); err != nil {
				return err
			}
		}
	}

	if err := eav.saveDefintion(tableName, schema); err != nil {
		return err
	}

	if _, err := eav.db.Tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		return err
	}

	if _, err := eav.db.Tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", nextTableName, tableName)); err != nil {
		return err
	}

	for _, index := range schema.Indexes {
		eav.log.Debugf("creating index %s on %s", strings.Join(index, "_"), tableName)
		statement := fmt.Sprintf("CREATE INDEX %s_%s_idx on %s (%s)", tableName, strings.Join(index, "_"), tableName, strings.Join(index, ", "))
		_, err := eav.db.Tx.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (eav *EAV) Apply(groupID ids.ID, applier uint8, ops *Operations) (*Operations, *Operations, error) {
	groupOperations := NewOperations()
	selfOperations := NewOperations()
	updatedTables := make(map[string]bool)
	for ts, idMap := range ops.OperationMap {
		for id, pairs := range idMap {
			applyTables := make(map[string]bool)
			names := make(map[string]uint64)
			for nameID, value := range pairs {
				name := ops.Names[nameID]
				if !eav.checkNameAccess(applier, name) {
					continue
				}
				names[name] = nameID
				applied, err := eav.apply(groupID, id, ts, name, value)
				if err != nil {
					return nil, nil, err
				}
				if applied {
					extractedType := eav.extractNamePermission(name)
					switch extractedType {
					case Private:
						// do nothing
					case Self:
						selfOperations.Add(id, ts, name, value)
					case Other:
						groupOperations.Add(id, ts, name, value)
					}
					if tables, ok := eav.sourceTableMap[name]; ok {
						for _, t := range tables {
							applyTables[t] = true
						}
					}
				}
			}
			for table := range applyTables {
				if applied, err := eav.applyViews(table, groupID, id, ts, names, pairs, ops.fromBackfill); err != nil {
					return nil, nil, err
				} else if applied {
					updatedTables[table] = true
				}
			}
		}
	}

	for t := range updatedTables {
		eav.updates <- &TableUpdate{groupID, t}
	}

	return groupOperations, selfOperations, nil
}

// Get a single row with a SQL query.
func (eav *EAV) Get(dest interface{}, statement string, args ...interface{}) error {
	eav.log.Debugf("querying %s with args %#v", statement, args)
	return eav.db.Tx.Get(dest, statement, args...)
}

// Get a slice of rows with a SQL query.
func (eav *EAV) Select(dest interface{}, statement string, args ...interface{}) error {
	eav.log.Debugf("querying %s with args %#v", statement, args)
	return eav.db.Tx.Select(dest, statement, args...)
}

// Query via SQL.
func (eav *EAV) Query(statement string, args ...interface{}) (*Result, error) {
	eav.log.Debugf("querying %s with args %#v", statement, args)
	rows, err := eav.db.Tx.Query(statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := Result{Names: make([]string, len(cols))}
	for i, col := range cols {
		result.Names[i] = col.Name()
	}

	scanArgs := make([]interface{}, len(cols))
	for rows.Next() {
		values := make([]interface{}, len(cols))

		for i := range values {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		for i := range values {
			if values[i] == nil {
				continue
			}
		}

		result.Rows = append(result.Rows, values)
	}
	return &result, nil
}

func (eav *EAV) Backfill(groupID ids.ID, authorTag [7]byte, startFrom [16]byte, partial, fromSelf bool) ([]byte, [16]byte, bool, error) {
	nextID := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	values := []interface{}{groupID[:], startFrom[:]}
	statement := "SELECT id, name, value, ts from _eav_data where group_id = ? AND ID >= ?"
	if partial {
		statement += " AND identity_tag = ? AND membership_tag = ?"
		values = append(values, authorTag[0:4], authorTag[4:])
	}
	statement += " AND name_permission >= ?"
	if fromSelf {
		values = append(values, Self)
	} else {
		values = append(values, Other)
	}
	statement += " ORDER BY id limit 1000"

	eav.log.Debugf("Running backfill query statement=%s args=%#v", statement, values)
	valueRows, err := eav.db.Tx.Queryx(statement, values...)
	if err != nil {
		return nil, nextID, false, err
	}
	defer valueRows.Close()
	data := make([]*eavData, 0, 1000)
	names := make(map[uint64]int)
	backfillNames := make([]string, 0)

	// need to also encode the names
	var rowNames []uint64
	lastID := make([]byte, 16)
	for valueRows.Next() {
		var v eavData

		if err := valueRows.StructScan(&v); err != nil {
			return nil, nextID, false, err
		}
		if !bytes.Equal(v.ID, lastID) {
			copy(lastID, v.ID[0:16])
			rowNames = make([]uint64, 0)
		}
		if bytes.Compare(nextID[:], v.ID) == -1 {
			copy(nextID[:], v.ID)
		}
		rowNames = append(rowNames, v.Name)
		if _, ok := names[v.Name]; !ok {
			nameStr, err := eav.nameForID(v.Name)
			if err != nil {
				return nil, nextID, false, err
			}
			names[v.Name] = len(backfillNames)
			backfillNames = append(backfillNames, nameStr)
		}
		data = append(data, &v)
	}

	atEnd := len(data) < 1000

	if len(data) != 0 {
		remainderQuery, vs, err := sqlx.In("SELECT id, name, value, ts from _eav_data where name NOT IN (?)", rowNames)
		if err != nil {
			return nil, nextID, false, err
		}
		remainderQuery += " AND group_id = ? AND id = ?"
		vs = append(vs, groupID[:])
		vs = append(vs, lastID)
		if partial {
			remainderQuery += " AND identity_tag = ? AND membership_tag = ?"
			vs = append(vs, authorTag[0:4], authorTag[4:])
		}
		remainderQuery += " AND name_permission >= ?"
		if fromSelf {
			vs = append(vs, Self)
		} else {
			vs = append(vs, Other)
		}

		remainderRows, err := eav.db.Tx.Queryx(remainderQuery, vs...)
		if err != nil {
			return nil, nextID, false, err
		}
		defer remainderRows.Close()
		for remainderRows.Next() {
			var v eavData

			if err := remainderRows.StructScan(&v); err != nil {
				return nil, nextID, false, err
			}
			data = append(data, &v)
			if _, ok := names[v.Name]; !ok {
				nameStr, err := eav.nameForID(v.Name)
				if err != nil {
					return nil, nextID, false, err
				}
				names[v.Name] = len(backfillNames)
				backfillNames = append(backfillNames, nameStr)
			}
		}
		copy(nextID[:], lastID)
		nextIDInt := binary.BigEndian.Uint64(nextID[:]) + 1
		binary.BigEndian.PutUint64(nextID[:], nextIDInt)
	}

	packedData := make([][]byte, len(data))
	for i, d := range data {
		l := 29
		if d.Value != nil {
			l += len(*d.Value)
		}
		row := make([]byte, l)
		copy(row[0:16], d.ID)
		binary.BigEndian.PutUint64(row[16:24], d.Ts)
		binary.BigEndian.PutUint32(row[24:28], uint32(names[d.Name]))
		if d.Value != nil {
			row[28] = flagsPresent
			copy(row[29:], *d.Value)
		} else {
			row[28] = 0
		}
		packedData[i] = row
	}

	backfillBytes, err := bencode.Serialize(&backfill{
		Names: backfillNames,
		Data:  packedData,
	})
	if err != nil {
		return nil, nextID, false, err
	}
	return backfillBytes, nextID, atEnd, nil
}

func (eav *EAV) ProcessBackfill(groupID ids.ID, applier uint8, body []byte) error {
	backfill := backfill{}
	if err := bencode.Deserialize(body, &backfill); err != nil {
		return err
	}

	ops := NewOperations()
	ops.fromBackfill = true
	for _, v := range backfill.Data {
		id := ids.IDFromBytes(v[0:16])
		ts := binary.BigEndian.Uint64(v[16:24])
		nameIdx := binary.BigEndian.Uint32(v[24:28])
		name := backfill.Names[nameIdx]
		if !eav.checkNameAccess(applier, name) {
			continue
		}
		flags := v[28]
		if flags&flagsPresent == 0 {
			if len(v) != 29 {
				return fmt.Errorf("expected v to be 28 bytes long, got %d", len(v))
			}
			ops.AddNil(id, ts, name)
		} else {
			ops.AddBytes(id, ts, name, v[29:])
		}
	}
	if _, _, err := eav.Apply(groupID, applier, ops); err != nil {
		return err
	}
	return nil
}

func (eav *EAV) BeforeApplyView(table string, cb beforeApplyCallback) {
	if _, ok := eav.beforeAppliers[table]; !ok {
		eav.beforeAppliers[table] = []beforeApplyCallback{cb}
		return
	}

	eav.beforeAppliers[table] = append(eav.beforeAppliers[table], cb)
}

func (eav *EAV) applyViews(table string, groupID, id ids.ID, mtime uint64, names map[string]uint64, pairs map[uint64]Value, fromBackfill bool) (bool, error) {
	def := eav.definitions[table]

	insertNames := make([]string, 0, len(def.Columns))
	insertVarNames := make([]string, 0, len(def.Columns))
	updateNames := make([]string, 0)
	vals := make(map[string]interface{})
	canInsert := true

	for targetName := range def.Columns {
		colDef := def.Columns[targetName]
		insertNames = append(insertNames, targetName)
		insertVarNames = append(insertVarNames, ":"+targetName)
		sourceIndex, ok := names[colDef.SourceName]
		if ok {
			updateNames = append(updateNames, fmt.Sprintf("%s = :%s", targetName, targetName))

			val, ok := pairs[sourceIndex]
			if ok && val.Present {
				vals[targetName] = val.NativeType(colDef)
			} else if ok && !val.Present && colDef.Nullable {
				vals[targetName] = nil
			} else {
				vals[targetName] = colDef.DefaultValue.NativeType(colDef)
			}
		} else {
			val := colDef.DefaultValue.NativeType(colDef)
			vals[targetName] = val
			if val == nil && !colDef.Nullable {
				canInsert = false
			}
		}
	}

	if _, ok := eav.beforeAppliers[table]; ok {
		eav.db.BeforeCommit(func() error {
			for _, cb := range eav.beforeAppliers[table] {
				if err := cb(groupID, id); err != nil {
					return err
				}
			}
			return nil
		})
	}

	vals["group_id"] = groupID[:]
	vals["id"] = id[:]
	vals["_ctime"] = extractUnixMicro(id[:])
	vals["_mtime"] = microToFloat(mtime)
	vals["_wtime"] = microToFloat(eav.clock.CurrentTimeMicro())

	if canInsert {
		insertQuery := fmt.Sprintf(
			`INSERT INTO %s (group_id, id, _ctime, _mtime, _wtime, %s) VALUES(:group_id, :id, :_ctime, :_mtime, :_wtime, %s)
			ON CONFLICT DO NOTHING`,
			table,
			strings.Join(insertNames, ", "),
			strings.Join(insertVarNames, ", "),
		)
		result, err := eav.db.Tx.NamedExec(insertQuery, vals)
		if err != nil {
			return false, err
		}
		i, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		// perform update
		if i == 0 {
			query := fmt.Sprintf(
				`UPDATE %s SET _mtime = MAX(_mtime, :_mtime), %s WHERE group_id = :group_id AND id = :id`,
				table,
				strings.Join(updateNames, ", "),
			)
			result, err := eav.db.Tx.NamedExec(query, vals)
			if err != nil {
				return false, err
			}
			i, err := result.RowsAffected()
			if err != nil {
				return false, err
			}

			if !fromBackfill && i != 0 {
				eav.updates <- &TableRowUpdate{groupID, id, table, vals}
			}

			return i != 0, nil
		}
		if !fromBackfill {
			eav.updates <- &TableRowInsert{groupID, id, table, vals}
		}
		return true, nil
	}
	query := fmt.Sprintf(
		`UPDATE %s SET _mtime = MAX(_mtime, :_mtime), %s WHERE group_id = :group_id AND id = :id`,
		table,
		strings.Join(updateNames, ", "),
	)
	result, err := eav.db.Tx.NamedExec(query, vals)
	if err != nil {
		return false, err
	}
	i, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return i != 0, nil
}

func (eav *EAV) idForName(n string) (uint64, error) {
	if id, ok := eav.nameCache[n]; ok {
		return id, nil
	}

	if _, err := eav.db.Tx.Exec("INSERT INTO _eav_names (name) VALUES (?) ON CONFLICT DO NOTHING", n); err != nil {
		return 0, fmt.Errorf("messaging: error inserting name: %w", err)
	}

	var name eavName
	if err := eav.db.Tx.Get(&name, "SELECT * FROM _eav_names WHERE name = ?", n); err != nil {
		return 0, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	eav.nameCache[name.Name] = name.ID
	return name.ID, nil
}

func (eav *EAV) nameForID(id uint64) (string, error) {
	var name eavName
	if err := eav.db.Tx.Get(&name, "SELECT * FROM _eav_names WHERE id = ?", id); err != nil {
		return "", fmt.Errorf("messaging: error counting keys: %w", err)
	}
	return name.Name, nil
}

func (eav *EAV) insertRow(def *TableDefinition, tableName string, groupID, id []byte, mtime float64, values map[string]interface{}) error {
	query := fmt.Sprintf("INSERT INTO %s", tableName)
	names := []string{"group_id", "id", "_ctime", "_wtime", "_mtime"}
	placeholders := []string{}
	ts := extractUnixMicro(id)
	args := []interface{}{groupID, id, ts, eav.clock.CurrentTimeMicro(), mtime}
	for columnName := range def.Columns {
		colDef := def.Columns[columnName]
		names = append(names, columnName)
		placeholders = append(placeholders, "?")
		if val, ok := values[columnName]; ok {
			if !colDef.Nullable && val == nil {
				args = append(args, colDef.DefaultValue.NativeType(colDef))
			} else {
				args = append(args, val)
			}
		} else {
			args = append(args, colDef.DefaultValue.NativeType(colDef))
		}
	}

	query += " (" + strings.Join(names, ",") + ") VALUES (?, ?, ?, ?, ?, " + strings.Join(placeholders, ", ") + ")"
	_, err := eav.db.Tx.Exec(query, args...)
	return err
}

func (eav *EAV) updateRow(cols map[string]*ColumnDefinition, tableName string, groupID, id []byte, mtime float64, values map[string]interface{}) error {
	query := fmt.Sprintf("UPDATE %s SET ", tableName)
	pairs := []string{"_mtime = MAX(_mtime, ?)"}
	args := []interface{}{mtime}
	for columnName, colDef := range cols {
		pairs = append(pairs, fmt.Sprintf("%s = ?", columnName))
		v, ok := values[columnName]
		if ok {
			args = append(args, v)
			continue
		}
		if !colDef.Nullable {
			args = append(args, colDef.DefaultValue.NativeType(colDef))
			continue
		}
		args = append(args, nil)
	}

	query += strings.Join(pairs, ", ") + " WHERE group_id = ? and id = ?"
	args = append(args, groupID)
	args = append(args, id)
	_, err := eav.db.Tx.Exec(query, args...)
	return err
}

func (eav *EAV) buildColumnDefinitions(columns map[string]*ColumnDefinition, alter bool) (string, error) {
	defs := []string{
		"id BLOB NOT NULL",
		"group_id BLOB NOT NULL",
		"_identity_tag BLOB AS (substr(id, 10, 4))",
		"_membership_tag BLOB AS (substr(id, 14, 3))",
		"_ctime REAL NOT NULL",
		"_mtime REAL NOT NULL",
		"_wtime REAL NOT NULL",
	}
	for name, column := range columns {
		def, err := eav.buildColumnDefinition(name, column, alter)
		if err != nil {
			return "", err
		}
		defs = append(defs, def)
	}
	return strings.Join(defs, ", "), nil
}

func (eav *EAV) buildColumnDefinition(name string, column *ColumnDefinition, alter bool) (string, error) {
	if alter && !column.Nullable && !column.DefaultValue.Present {
		return "", fmt.Errorf("column %s cannot be not nullable and have no default value", name)
	}

	def := name
	switch column.ColumnType {
	case Blob:
		def += " BLOB"
	case Int:
		def += " INTEGER"
	case Text:
		def += " TEXT"
	case Real:
		def += " REAL"
	}
	if !column.Nullable {
		def += " NOT NULL"
	}
	if column.DefaultValue.Present {
		def += fmt.Sprintf(" DEFAULT x'%x'", column.DefaultValue.Bytes)
	}
	return def, nil

}

func (eav *EAV) loadDefinitions() (map[string]*TableDefinition, error) {
	def := make(map[string]*TableDefinition)
	var columns []*eavColumn
	if err := eav.db.Conn.Select(&columns, "SELECT * FROM _eav_columns"); err != nil {
		return def, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	var indexes []*eavIndex
	if err := eav.db.Conn.Select(&indexes, "SELECT * FROM _eav_indexes"); err != nil {
		return def, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	for _, column := range columns {
		if _, ok := def[column.TableName]; !ok {
			def[column.TableName] = &TableDefinition{
				Columns: make(map[string]*ColumnDefinition),
				Indexes: make([][]string, 0),
			}
		}

		var defaultValue Value
		if column.DefaultValue != nil {
			defaultValue = NewValue(column.DefaultValue)
		} else {
			defaultValue = Value{false, nil}
		}
		def[column.TableName].Columns[column.TargetName] = &ColumnDefinition{
			SourceName:   column.SourceName,
			ColumnType:   column.ColumnType,
			DefaultValue: defaultValue,
			Required:     column.Required,
			Nullable:     column.Nullable,
		}
	}

	for _, index := range indexes {
		var loadedIndexes []string
		if err := json.Unmarshal([]byte(index.IndexJSON), &loadedIndexes); err != nil {
			return def, err
		}
		def[index.TableName].Indexes = append(def[index.TableName].Indexes, loadedIndexes)
	}
	return def, nil
}

func (eav *EAV) saveDefintion(tableName string, newDefinition *TableDefinition) error {
	if _, err := eav.db.Tx.Exec("DELETE FROM _eav_columns WHERE table_name = ?", tableName); err != nil {
		return err
	}
	if _, err := eav.db.Tx.Exec("DELETE FROM _eav_indexes WHERE table_name = ?", tableName); err != nil {
		return err
	}

	for targetName, columnDef := range newDefinition.Columns {
		col := eavColumn{
			TableName:    tableName,
			TargetName:   targetName,
			SourceName:   columnDef.SourceName,
			ColumnType:   columnDef.ColumnType,
			DefaultValue: columnDef.DefaultValue.BytePointer(),
			Required:     columnDef.Required,
			Nullable:     columnDef.Nullable,
		}

		if _, err := eav.db.Tx.NamedExec("INSERT INTO _eav_columns (table_name, target_name, source_name, column_type, default_value, required, nullable) VALUES (:table_name, :target_name, :source_name, :column_type, :default_value, :required, :nullable)", col); err != nil {
			return fmt.Errorf("messaging: error inserting column: %w", err)
		}
	}

	for _, indexDef := range newDefinition.Indexes {
		indexJSON, err := json.Marshal(indexDef)
		if err != nil {
			return err
		}
		idx := eavIndex{
			TableName: tableName,
			IndexJSON: string(indexJSON),
		}

		if _, err := eav.db.Tx.NamedExec("INSERT INTO _eav_indexes (table_name, index_json) VALUES (:table_name, :index_json)", idx); err != nil {
			return fmt.Errorf("messaging: error inserting index: %w", err)
		}
	}
	eav.definitions[tableName] = newDefinition
	eav.buildSourceNameTableMap()
	return nil
}

func (eav *EAV) apply(groupID, id ids.ID, ts uint64, name string, val Value) (bool, error) {
	applied := false
	nameID, err := eav.idForName(name)
	if err != nil {
		return applied, err
	}
	namePermission := eav.extractNamePermission(name)

	result, err := eav.db.Tx.Exec("INSERT INTO _eav_data (group_id, id, name, name_permission, value, ts) VALUES (?, ?,?,?,?,?) ON CONFLICT(group_id, id, name) DO NOTHING", groupID[:], id[:], nameID, namePermission, val.BytePointer(), ts)
	if err != nil {
		return applied, err
	}
	i, err := result.RowsAffected()
	if err != nil {
		return applied, err
	}
	if i == 0 {
		result, err := eav.db.Tx.Exec("UPDATE _eav_data SET value = ? WHERE group_id = ? and id = ? and name = ? and ts < ?", val.BytePointer(), groupID[:], id[:], nameID, ts)
		if err != nil {
			return applied, err
		}
		i, err := result.RowsAffected()
		if err != nil {
			return applied, err
		}
		if i != 0 {
			applied = true
		}
	} else {
		applied = true
	}
	return applied, nil
}

func (eav *EAV) buildSourceNameTableMap() {
	eav.sourceTableMap = make(map[string][]string)
	for tableName, def := range eav.definitions {
		for _, col := range def.Columns {
			if _, ok := eav.sourceTableMap[col.SourceName]; !ok {
				eav.sourceTableMap[col.SourceName] = make([]string, 0)
			}
			eav.sourceTableMap[col.SourceName] = append(eav.sourceTableMap[col.SourceName], tableName)
		}
	}
}

func (eav *EAV) checkNameAccess(applier uint8, name string) bool {
	if strings.HasPrefix(name, "_private_") && applier != Private {
		return false
	} else if strings.HasPrefix(name, "_self_") && applier == Other {
		return false
	}
	return true
}

func (eav *EAV) extractNamePermission(name string) uint8 {
	if strings.HasPrefix(name, "_private_") {
		return Private
	} else if strings.HasPrefix(name, "_self_") {
		return Self
	}
	return Other
}
