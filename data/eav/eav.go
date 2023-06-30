package eav

import (
	"database/sql"
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
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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

type BeforeEntityCallback func(viewName string, groupID, id ids.ID) error
type BeforeViewCallback func(viewName string) error
type AfterEntityCallback func(viewName string, groupID, id ids.ID)
type AfterViewCallback func(viewName string)

type callback[T any] struct {
	includeBackfill bool
	cb              T
}

type ColumnDefinition struct {
	SourceName   string
	ColumnType   uint8
	DefaultValue Value
	Required     bool
	Nullable     bool
}

type ViewDefinition struct {
	Columns map[string]*ColumnDefinition
	Indexes [][]string
}

type Value struct {
	Present bool   `bencode:"p"`
	Bytes   []byte `bencode:"b"`
}

type backfill struct {
	Names []string                                    `bencode:"n"`
	Data  map[[16]byte]map[uint64]map[uint32][][]byte `bencode:"d"`
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

type Result struct {
	Names []string
	Rows  [][]interface{}
}

type eavColumn struct {
	ViewName     string  `db:"view_name"`
	TargetName   string  `db:"target_name"`
	SourceName   string  `db:"source_name"`
	ColumnType   uint8   `db:"column_type"`
	DefaultValue *[]byte `db:"default_value"`
	Required     bool    `db:"required"`
	Nullable     bool    `db:"nullable"`
}

type eavIndex struct {
	ViewName  string `db:"view_name"`
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
	log                     *zap.SugaredLogger
	config                  *config.Config
	db                      *db.Database
	clock                   clock.Clock
	definitions             map[string]*ViewDefinition
	nameMap                 map[string]uint32
	updates                 chan interface{}
	beforeEntitySubscribers map[string][]callback[BeforeEntityCallback]
	beforeViewSubscribers   map[string][]callback[BeforeViewCallback]
	afterEntitySubscribers  map[string][]callback[AfterEntityCallback]
	afterViewSubscribers    map[string][]callback[AfterViewCallback]
	entitiesAffected        map[string]map[ids.ID]map[ids.ID]bool
	viewNameTesters         map[string]string
}

func NewEAV(c *config.Config, d *db.Database, clock clock.Clock, updates chan interface{}) (*EAV, error) {
	log := c.Logger("data/eav")

	if err := d.MigrateNoLock("_eav", []*migration.Migration{
		{
			Name: "Create initial tables",
			Func: func(tx *sql.Tx) error {
				_, err := tx.Exec(`
CREATE TABLE IF NOT EXISTS _eav_columns (
	view_name STRING NOT NULL,
	target_name STRING NOT NULL,
	source_name STRING NOT NULL,
	column_type INTEGER NOT NULL,
	default_value BLOB,
	required INTEGER NOT NULL,
	nullable INTEGER NOT NULL,
	PRIMARY KEY(view_name, target_name)
);

CREATE TABLE IF NOT EXISTS _eav_indexes (
	view_name STRING NOT NULL,
	index_json STRING NOT NULL,
	PRIMARY KEY(view_name, index_json)
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
		log:                     log,
		config:                  c,
		db:                      d,
		clock:                   clock,
		definitions:             make(map[string]*ViewDefinition),
		nameMap:                 make(map[string]uint32),
		updates:                 updates,
		beforeEntitySubscribers: make(map[string][]callback[BeforeEntityCallback]),
		beforeViewSubscribers:   make(map[string][]callback[BeforeViewCallback]),
		afterEntitySubscribers:  make(map[string][]callback[AfterEntityCallback]),
		afterViewSubscribers:    make(map[string][]callback[AfterViewCallback]),
		entitiesAffected:        make(map[string]map[ids.ID]map[ids.ID]bool),
		viewNameTesters:         make(map[string]string),
	}

	def, err := eav.loadDefinitions()
	if err != nil {
		return nil, err
	}
	eav.definitions = def
	return eav, nil
}

func (eav *EAV) SubscribeAfterEntity(cb AfterEntityCallback, includeBackfill bool, views ...string) error {
	for _, v := range views {
		if _, ok := eav.afterEntitySubscribers[v]; !ok {
			eav.afterEntitySubscribers[v] = make([]callback[AfterEntityCallback], 0, 1)
		}
		eav.afterEntitySubscribers[v] = append(eav.afterEntitySubscribers[v], callback[AfterEntityCallback]{includeBackfill, cb})
		viewWhereClause, err := eav.buildViewWhereClause(eav.definitions[v].Columns)
		if err != nil {
			return err
		}
		eav.viewNameTesters[v] = fmt.Sprintf("WHEN %s THEN '%s'", viewWhereClause, v)
	}
	return nil
}

func (eav *EAV) SubscribeAfterView(cb AfterViewCallback, includeBackfill bool, views ...string) error {
	for _, v := range views {
		if _, ok := eav.afterViewSubscribers[v]; !ok {
			eav.afterViewSubscribers[v] = make([]callback[AfterViewCallback], 0, 1)
		}
		eav.afterViewSubscribers[v] = append(eav.afterViewSubscribers[v], callback[AfterViewCallback]{includeBackfill, cb})
		viewWhereClause, err := eav.buildViewWhereClause(eav.definitions[v].Columns)
		if err != nil {
			return err
		}
		eav.viewNameTesters[v] = fmt.Sprintf("WHEN %s THEN '%s'", viewWhereClause, v)
	}
	return nil
}

func (eav *EAV) SubscribeBeforeEntity(cb BeforeEntityCallback, includeBackfill bool, views ...string) error {
	for _, v := range views {
		if _, ok := eav.beforeEntitySubscribers[v]; !ok {
			eav.beforeEntitySubscribers[v] = make([]callback[BeforeEntityCallback], 0, 1)
		}
		eav.beforeEntitySubscribers[v] = append(eav.beforeEntitySubscribers[v], callback[BeforeEntityCallback]{includeBackfill, cb})
		viewWhereClause, err := eav.buildViewWhereClause(eav.definitions[v].Columns)
		if err != nil {
			return err
		}
		eav.viewNameTesters[v] = fmt.Sprintf("WHEN %s THEN '%s'", viewWhereClause, v)
	}
	return nil
}

func (eav *EAV) SubscribeBeforeView(cb BeforeViewCallback, includeBackfill bool, views ...string) error {
	for _, v := range views {
		if _, ok := eav.beforeViewSubscribers[v]; !ok {
			eav.beforeViewSubscribers[v] = make([]callback[BeforeViewCallback], 0, 1)
		}
		eav.beforeViewSubscribers[v] = append(eav.beforeViewSubscribers[v], callback[BeforeViewCallback]{includeBackfill, cb})
		viewWhereClause, err := eav.buildViewWhereClause(eav.definitions[v].Columns)
		if err != nil {
			return err
		}
		eav.viewNameTesters[v] = fmt.Sprintf("WHEN %s THEN '%s'", viewWhereClause, v)
	}
	return nil
}

func (eav *EAV) Schema(viewName string) (*ViewDefinition, bool) {
	d, ok := eav.definitions[viewName]
	return d, ok
}

func (eav *EAV) CreateView(viewName string, schema *ViewDefinition) error {
	// drop existing view
	eav.log.Debugf("rebuilding view for %s", viewName)
	if _, err := eav.db.Tx.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)); err != nil {
		return err
	}

	indexPrefix := fmt.Sprintf("_idx_eav_%s__", viewName)

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
	if _, err := eav.db.Tx.Exec(fmt.Sprintf("create index %s on _eav_data (group_id, id) WHERE %s", indexPrefix+"_pk", viewWhereClause)); err != nil {
		return err
	}

	// create any secondary indexes
	for _, index := range schema.Indexes {
		eav.log.Debugf("creating index %s on %s", indexPrefix+strings.Join(index, "_"), viewName)
		indexValues, err := eav.buildIndexValues(index)
		if err != nil {
			return err
		}
		statement := fmt.Sprintf("CREATE INDEX %s%s_%s_idx on _eav_data (%s) WHERE %s", indexPrefix, viewName, strings.Join(index, "_"), indexValues, viewWhereClause)
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
		viewName,
		strings.Join(columnNames, ","),
		strings.Join(columnDefinitions, ","),
		viewWhereClause,
	)

	if _, err := eav.db.Tx.Exec(createViewStatement); err != nil {
		return err
	}

	return eav.saveDefintion(viewName, schema)
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
		if def.DefaultValue.Present {
			defaultValue := fmt.Sprintf("x'%x'", def.DefaultValue.Bytes)
			defs = append(defs, fmt.Sprintf("CAST(COALESCE(eav_get(value, %d), %s) AS %s)", id, defaultValue, t))
		} else {
			defs = append(defs, fmt.Sprintf("CAST(eav_get(value, %d) AS %s)", id, t))
		}

		names = append(names, name)
	}
	return names, defs, nil
}

func (eav *EAV) buildViewWhereClause(columns map[string]*ColumnDefinition) (string, error) {
	nameIDs := make([]uint32, 0, len(columns))
	for _, def := range columns {
		if !def.Required {
			continue
		}
		nameID, err := eav.idForName(def.SourceName)
		if err != nil {
			return "", err
		}
		nameIDs = append(nameIDs, nameID)
	}
	slices.Sort(nameIDs)
	where := make([]string, len(nameIDs))
	for i, nameID := range nameIDs {
		where[i] = fmt.Sprintf("%d", nameID)
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
	return eav.apply(groupID, applier, ops, false)
}

func (eav *EAV) apply(groupID ids.ID, applier uint8, ops *Operations, backfilling bool) (*Operations, *Operations, error) {
	groupOperations := NewOperations()
	selfOperations := NewOperations()
	entities := []ids.ID{}
	for ts, idMap := range ops.OperationMap {
		for id, pairs := range idMap {
			entities = append(entities, id)
			packed := [][]byte{}
			for nameID, value := range pairs {
				name := ops.Names[nameID]
				if !eav.checkNameAccess(applier, name) {
					continue
				}
				extractedType := eav.extractNamePermission(name)
				switch extractedType {
				case Private:
					// do nothing
				case Self:
					selfOperations.Add(id, ts, name, value)
				case Other:
					groupOperations.Add(id, ts, name, value)
				}

				nameID, err := eav.idForName(name)
				if err != nil {
					return nil, nil, err
				}
				packed = append(packed, db.EAVPack(nameID, ts, !value.Present, value.Bytes))
			}

			if len(packed) == 0 {
				continue
			}
			packedHex := make([]string, len(packed))
			for i, p := range packed {
				packedHex[i] = fmt.Sprintf("x'%x'", p)
			}

			// do this in two parts to avoid generating useless empty records and calling eav_set too much
			result, err := eav.db.Tx.Exec(
				fmt.Sprintf(`UPDATE _eav_data SET value = eav_set(value, %s) WHERE group_id = ? AND id = ?`, strings.Join(packedHex, ",")),
				groupID[:], id[:])
			if err != nil {
				return nil, nil, fmt.Errorf("error during update: %w", err)
			}
			i, err := result.RowsAffected()
			if err != nil {
				return nil, nil, err
			}
			if i == 0 {
				newRec, err := eav.db.EAVHandler.MakeRecord(packed...)
				if err != nil {
					return nil, nil, err
				}
				result, err = eav.db.Tx.Exec(
					fmt.Sprintf(`INSERT INTO _eav_data (group_id, id, value) VALUES (?, ?, x'%x')`, newRec),
					groupID[:], id[:])
				if err != nil {
					return nil, nil, err
				}
				i, err = result.RowsAffected()
				if err != nil {
					return nil, nil, err
				}
				if i != 1 {
					panic("should have affected a row")
				}
			}
		}
	}

	if len(eav.viewNameTesters) != 0 {
		entitiesAffected := make(map[ids.ID]bool)
		viewsAffected := make(map[string]bool)
		for idIndex := range entities {
			id := entities[idIndex]
			if entitiesAffected[id] {
				continue
			}

			viewName := ""
			if err := eav.db.Tx.Get(&viewName, fmt.Sprintf(`select (case
				%s
				ELSE ''
			end) from _eav_data where group_id = ? and id = ?`, strings.Join(maps.Values(eav.viewNameTesters), "\n")), groupID[:], id[:]); err != nil {
				return nil, nil, err
			}
			if viewName == "" {
				continue
			}
			entitiesAffected[id] = true
			viewsAffected[viewName] = true
			if len(eav.beforeEntitySubscribers[viewName]) != 0 {
				for i := range eav.beforeEntitySubscribers[viewName] {
					cb := eav.beforeEntitySubscribers[viewName][i]
					if backfilling && !cb.includeBackfill {
						continue
					}
					eav.db.BeforeCommit(func() error {
						return cb.cb(viewName, groupID, id)
					})
				}
			}
			if len(eav.afterEntitySubscribers[viewName]) != 0 {
				for i := range eav.afterEntitySubscribers[viewName] {
					cb := eav.afterEntitySubscribers[viewName][i]
					if backfilling && !cb.includeBackfill {
						continue
					}
					eav.db.AfterCommit(func() {
						cb.cb(viewName, groupID, id)
					})
				}
			}
		}

		for viewName := range viewsAffected {
			if len(eav.beforeViewSubscribers[viewName]) != 0 {
				for i := range eav.beforeViewSubscribers[viewName] {
					cb := eav.beforeViewSubscribers[viewName][i]
					if backfilling && !cb.includeBackfill {
						continue
					}
					eav.db.BeforeCommit(func() error {
						return cb.cb(viewName)
					})
				}
			}

			if len(eav.afterViewSubscribers[viewName]) != 0 {
				for i := range eav.afterViewSubscribers[viewName] {
					cb := eav.afterViewSubscribers[viewName][i]
					if backfilling && !cb.includeBackfill {
						continue
					}
					eav.db.AfterCommit(func() {
						cb.cb(viewName)
					})
				}
			}
		}
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
	nextID := startFrom
	values := []interface{}{groupID[:], startFrom[:]}
	statement := "SELECT id, value from _eav_data where group_id = ? AND ID >= ?"
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
	defer valueRows.Close()
	applier := Other
	if fromSelf {
		applier = Private
	}
	names := make(map[uint32]uint32)
	backfillNames := make([]string, 0)
	data := map[[16]byte]map[uint64]map[uint32][][]byte{}
	i := 0
	// need to also encode the names
	var v eavData
	for valueRows.Next() {
		i++
		copy(nextID[:], v.ID)

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
				names[nameIdx] = uint32(len(backfillNames))
				backfillNames = append(backfillNames, nameStr)
			}

			if _, ok := data[ids.IDFromBytes(v.ID)]; !ok {
				data[ids.IDFromBytes(v.ID)] = make(map[uint64]map[uint32][][]byte)
			}
			if _, ok := data[ids.IDFromBytes(v.ID)][val.Time]; !ok {
				data[ids.IDFromBytes(v.ID)][val.Time] = make(map[uint32][][]byte)
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
	return backfillBytes, nextID, i != 1000, nil
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
					return fmt.Errorf("expected values to be 0 or 1 long, is %d", len(values))
				}
			}
		}
	}
	if _, _, err := eav.apply(groupID, applier, ops, true); err != nil {
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

func (eav *EAV) loadDefinitions() (map[string]*ViewDefinition, error) {
	def := make(map[string]*ViewDefinition)
	var columns []*eavColumn
	if err := eav.db.Conn.Select(&columns, "SELECT * FROM _eav_columns"); err != nil {
		return def, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	var indexes []*eavIndex
	if err := eav.db.Conn.Select(&indexes, "SELECT * FROM _eav_indexes"); err != nil {
		return def, fmt.Errorf("messaging: error counting keys: %w", err)
	}

	for _, column := range columns {
		if _, ok := def[column.ViewName]; !ok {
			def[column.ViewName] = &ViewDefinition{
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
		def[column.ViewName].Columns[column.TargetName] = &ColumnDefinition{
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
		def[index.ViewName].Indexes = append(def[index.ViewName].Indexes, loadedIndexes)
	}
	return def, nil
}

func (eav *EAV) saveDefintion(viewName string, newDefinition *ViewDefinition) error {
	if _, err := eav.db.Tx.Exec("DELETE FROM _eav_columns WHERE view_name = ?", viewName); err != nil {
		return err
	}
	if _, err := eav.db.Tx.Exec("DELETE FROM _eav_indexes WHERE view_name = ?", viewName); err != nil {
		return err
	}

	for targetName, columnDef := range newDefinition.Columns {
		col := eavColumn{
			ViewName:     viewName,
			TargetName:   targetName,
			SourceName:   columnDef.SourceName,
			ColumnType:   columnDef.ColumnType,
			DefaultValue: columnDef.DefaultValue.BytePointer(),
			Required:     columnDef.Required,
			Nullable:     columnDef.Nullable,
		}

		if _, err := eav.db.Tx.NamedExec("INSERT INTO _eav_columns (view_name, target_name, source_name, column_type, default_value, required, nullable) VALUES (:view_name, :target_name, :source_name, :column_type, :default_value, :required, :nullable)", col); err != nil {
			return fmt.Errorf("messaging: error inserting column: %w", err)
		}
	}

	for _, indexDef := range newDefinition.Indexes {
		indexJSON, err := json.Marshal(indexDef)
		if err != nil {
			return err
		}
		idx := eavIndex{
			ViewName:  viewName,
			IndexJSON: string(indexJSON),
		}

		if _, err := eav.db.Tx.NamedExec("INSERT INTO _eav_indexes (view_name, index_json) VALUES (:view_name, :index_json)", idx); err != nil {
			return fmt.Errorf("messaging: error inserting index: %w", err)
		}
	}
	eav.definitions[viewName] = newDefinition
	return nil
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
