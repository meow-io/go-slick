package eav

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	Names []string                                 `bencode:"n"`
	Data  map[[16]byte]map[uint64]map[int][][]byte `bencode:"d"`
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
	OperationMap map[uint64]map[ids.ID]map[uint32]Value `bencode:"m"`

	fromBackfill bool
	nameMap      map[string]uint32
}

func NewOperations() *Operations {
	return &Operations{
		Names:        make([]string, 0),
		OperationMap: make(map[uint64]map[ids.ID]map[uint32]Value),
		nameMap:      make(map[string]uint32),
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
		o.OperationMap[ts] = make(map[ids.ID]map[uint32]Value)
	}
	if _, ok := o.OperationMap[ts][id]; !ok {
		o.OperationMap[ts][id] = make(map[uint32]Value)
	}
	if _, ok := o.nameMap[name]; !ok {
		nameIdx := len(o.Names)
		o.nameMap[name] = uint32(nameIdx)
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
	ID   uint32 `db:"id"`
	Name string `db:"name"`
}

type eavData struct {
	GroupID []byte `db:"group_id"`
	ID      []byte `db:"id"`
	Value   []byte `db:"value"`
}

type EAV struct {
	log            *zap.SugaredLogger
	config         *config.Config
	db             *db.Database
	clock          clock.Clock
	definitions    map[string]*TableDefinition
	nameMap        map[string]uint32
	sourceTableMap map[string][]string
	updates        chan interface{}
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
	value BLOB,
	PRIMARY KEY (group_id, id)
);
				 `)
				return err
			},
		},
	},
	); err != nil {
		return nil, err
	}

	eav := &EAV{
		log:         log,
		config:      c,
		db:          d,
		clock:       clock,
		definitions: make(map[string]*TableDefinition),
		nameMap:     make(map[string]uint32),
		updates:     updates,
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

func (eav *EAV) CreateView(tableName string, schema *TableDefinition) error {
	// drop existing view
	eav.log.Debugf("rebuilding view for %s", tableName)
	if _, err := eav.db.Tx.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", tableName)); err != nil {
		return err
	}

	indexPrefix := fmt.Sprintf("_idx_eav_%s__", tableName)

	// drop existing indexes related to view
	var indexes []string
	if err := eav.db.Tx.Select(&indexes, "select name from sqlite_master where type = ? and name like ?", "index", indexPrefix+"%"); err != nil {
		return err
	}

	for _, name := range indexes {
		if _, err := eav.db.Tx.Exec(fmt.Sprintf("drop index %s", name)); err != nil {
			return err
		}
	}

	viewWhereClause, err := eav.buildViewWhereClause(schema.Columns)
	if err != nil {
		return err
	}

	// create the "primary key" view index
	eav.db.Tx.Exec(fmt.Sprintf("create index %s on (group_id, id) WHERE %s", indexPrefix+"_pk", viewWhereClause))

	// create any secondary indexes
	for _, index := range schema.Indexes {
		eav.log.Debugf("creating index %s on %s", indexPrefix+strings.Join(index, "_"), tableName)
		indexValues, err := eav.buildIndexValues(index)
		if err != nil {
			return err
		}
		statement := fmt.Sprintf("CREATE INDEX %s%s_%s_idx on _eav_data (%s) WHERE %s", indexPrefix, tableName, strings.Join(index, "_"), indexValues, viewWhereClause)
		if _, err := eav.db.Tx.Exec(statement); err != nil {
			return err
		}
	}

	// create the view itself
	columnNames, columnDefinitions, err := eav.buildColumnDefinitions(schema.Columns)
	if err != nil {
		return err
	}

	createViewStatement := fmt.Sprintf(`CREATE VIEW %s (
		group_id, id, _identity_tag, _membership_tag, _ctime, _mtime, _wtime, %s)
		AS SELECT group_id, id, identity_tag, membership_tag, eav_ctime(id), eav_mtime(value), eav_wtime(value), %s from _eav_data
	  WHERE %s`,
		tableName,
		strings.Join(columnNames, ","),
		strings.Join(columnDefinitions, ","),
		viewWhereClause,
	)
	if _, err := eav.db.Tx.Exec(createViewStatement); err != nil {
		return err
	}

	if err := eav.saveDefintion(tableName, schema); err != nil {
		return err
	}

	return nil
}

func (eav *EAV) buildColumnDefinitions(columns map[string]*ColumnDefinition) ([]string, []string, error) {
	names := make([]string, 0, len(columns))
	defs := make([]string, 0, len(columns))
	for name, def := range columns {
		var t string
		switch def.ColumnType {
		case Blob:
			t = " BLOB"
		case Int:
			t = " INTEGER"
		case Text:
			t = " TEXT"
		case Real:
			t = " REAL"
		}

		id, err := eav.idForName(def.SourceName)
		if err != nil {
			return nil, nil, err
		}
		defs = append(defs, fmt.Sprintf("CAST(eav_get(value, %d) AS %s)", id, t))
		names = append(names, name)
	}
	return names, defs, nil
}

func (eav *EAV) buildViewWhereClause(columns map[string]*ColumnDefinition) (string, error) {
	where := make([]string, 0, len(columns))
	for _, def := range columns {
		if !def.Required {
			continue
		}
		nameId, err := eav.idForName(def.SourceName)
		if err != nil {
			return "", err
		}
		where = append(where, fmt.Sprintf("%d", nameId))
	}
	return fmt.Sprintf("eav_has(value, %s)", strings.Join(where, ", ")), nil
}

func (eav *EAV) buildIndexValues(indexColumns []string) (string, error) {
	vals := make([]string, len(indexColumns))
	for i, col := range indexColumns {
		var c string
		switch col {
		case "id":
		case "group_id":
		case "identity_tag":
		case "membership_tag":
			c = col
		case "_wtime":
			c = "eav_wtime(value)"
		case "_mtime":
			c = "eav_mtime(value)"
		case "_ctime":
			c = "eav_ctime(id)"
		default:
			idx, err := eav.idForName(col)
			if err != nil {
				return "", err
			}
			c = fmt.Sprintf("eav_get(value, %d)", idx)
		}
		vals[i] = c
	}
	return strings.Join(vals, ", "), nil
}

func (eav *EAV) Apply(groupID ids.ID, applier uint8, ops *Operations) (*Operations, *Operations, error) {
	groupOperations := NewOperations()
	selfOperations := NewOperations()
	// updatedTables := make(map[string]bool)
	for ts, idMap := range ops.OperationMap {
		for id, pairs := range idMap {
			for nameID, value := range pairs {
				name := ops.Names[nameID]
				if !eav.checkNameAccess(applier, name) {
					continue
				}

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
				}
			}
		}
	}

	// for t := range updatedTables {
	// 	eav.updates <- &TableUpdate{groupID, t}
	// }

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
	statement += " ORDER BY id limit 1000"

	eav.log.Debugf("Running backfill query statement=%s args=%#v", statement, values)
	valueRows, err := eav.db.Tx.Queryx(statement, values...)
	if err != nil {
		return nil, nextID, false, err
	}
	applier := Other
	if fromSelf {
		applier = Private
	}
	defer valueRows.Close()
	names := make(map[uint32]int)
	backfillNames := make([]string, 0)
	data := map[[16]byte]map[uint64]map[int][][]byte{}
	i := 0
	// need to also encode the names
	var v eavData
	for valueRows.Next() {
		i++
		if err := valueRows.StructScan(&v); err != nil {
			return nil, nextID, false, err
		}
		values, err := db.EAVExtractNameValues(v.Value)
		if err != nil {
			return nil, nextID, false, err
		}

		for nameIdx, val := range values {
			nameStr, err := eav.nameForID(nameIdx)
			if err != nil {
				return nil, nextID, false, err
			}
			if !eav.checkNameAccess(applier, nameStr) {
				continue
			}

			if _, ok := names[nameIdx]; !ok {
				names[nameIdx] = len(backfillNames)
				backfillNames = append(backfillNames, nameStr)
			}

			data[ids.IDFromBytes(v.ID)][val.Time][names[nameIdx]] = [][]byte{}
			if val.Flag&db.DeletedFlag == 0 {
				data[ids.IDFromBytes(v.ID)][val.Time][names[nameIdx]] = append(data[ids.IDFromBytes(v.ID)][val.Time][names[nameIdx]], val.Val)
			}
		}
	}

	backfillBytes, err := bencode.Serialize(&backfill{
		Names: backfillNames,
		Data:  data,
	})
	if err != nil {
		return nil, nextID, false, err
	}
	return backfillBytes, [16]byte(v.ID), i != 1000, nil
}

func (eav *EAV) ProcessBackfill(groupID ids.ID, applier uint8, body []byte) error {
	backfill := backfill{}
	if err := bencode.Deserialize(body, &backfill); err != nil {
		return err
	}

	ops := NewOperations()
	ops.fromBackfill = true
	for id, v := range backfill.Data {
		for ts, valuesMap := range v {
			for nameIdx, values := range valuesMap {
				name := backfill.Names[nameIdx]
				if !eav.checkNameAccess(applier, name) {
					continue
				}
				switch len(values) {
				case 0:
					ops.AddNil(id, ts, name)
				case 1:
					ops.AddBytes(id, ts, name, values[0])
				default:
					return fmt.Errorf("expected v to be 28 bytes long, got %d", len(v))
				}
			}
		}
	}
	if _, _, err := eav.Apply(groupID, applier, ops); err != nil {
		return err
	}
	return nil
}

func (eav *EAV) idForName(n string) (uint32, error) {
	if id, ok := eav.nameMap[n]; ok {
		return id, nil
	}

	if _, err := eav.db.Tx.Exec("INSERT INTO _eav_names (name) VALUES (?) ON CONFLICT DO NOTHING", n); err != nil {
		return 0, fmt.Errorf("messaging: error inserting name: %w", err)
	}

	var name eavName
	if err := eav.db.Tx.Get(&name, "SELECT * FROM _eav_names WHERE name = ?", n); err != nil {
		return 0, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	eav.nameMap[name.Name] = name.ID
	return name.ID, nil
}

func (eav *EAV) nameForID(id uint32) (string, error) {
	var name eavName
	if err := eav.db.Tx.Get(&name, "SELECT * FROM _eav_names WHERE id = ?", id); err != nil {
		return "", fmt.Errorf("messaging: error counting keys: %w", err)
	}
	return name.Name, nil
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

	// namePermission := eav.extractNamePermission(name)
	eavDB := db.EAV{eav.clock}

	packed := db.EAVPack(nameID, ts, !val.Present, val.Bytes)
	newRec, err := eavDB.MakeRecord(packed)
	if err != nil {
		return false, err
	}
	// namePermission, val.BytePointer(), ts
	result, err := eav.db.Tx.Exec(
		`INSERT INTO _eav_data (group_id, id, value) VALUES (?, ?, ?)
		ON CONFLICT(group_id, id) DO UPDATE SET value = eav_set(value, ?)`,
		groupID[:], id[:], newRec, packed)
	if err != nil {
		return applied, err
	}
	i, err := result.RowsAffected()
	if err != nil {
		return applied, err
	}
	return i != 0, nil
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
