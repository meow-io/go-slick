// This package defines a SQLCipher database. It provides some default setup options and provides an interface
// for running functions before and after a transaction.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	// adds sqlcipher support
	"github.com/meow-io/go-slick/clock"
	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/migration"
	sqlite3 "github.com/meow-io/go-sqlcipher"
	"go.uber.org/zap"

	"github.com/jmoiron/sqlx"
)

const (
	stateNew = iota
	stateInitialized
	stateRunning
	stateClosing
	stateClosed
)

type RunnerFunc func() error

type Database struct {
	Log        *zap.SugaredLogger
	Conn       *sqlx.DB
	Tx         *sqlx.Tx
	EAVHandler *EAV

	config                *config.Config
	state                 int
	lock                  *sync.Mutex
	path                  string
	callbacks             []func()
	beforeCommitCallbacks []func() error
	ctx                   context.Context
	cancelFn              context.CancelFunc
}

var currentDB *Database

func NewDatabase(c *config.Config, cl clock.Clock, path string) (*Database, error) {
	log := c.Logger("db")
	log.Debugf("making database at %s", path)

	var state int

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			state = stateNew
		} else {
			return nil, err
		}
	} else {
		state = stateInitialized
	}

	ctx, cancelFn := context.WithCancel(context.TODO())
	db := &Database{
		Conn: nil,
		Log:  log,
		EAVHandler: &EAV{
			Clock: cl,
			callback: func(viewName string, phase int, groupID, id []byte) error {
				return nil
			},
		},
		lock:     &sync.Mutex{},
		config:   c,
		path:     path,
		state:    state,
		ctx:      ctx,
		cancelFn: cancelFn,
	}
	currentDB = db
	db.registerDriver()
	return db, nil
}

func (db *Database) Initialize(key []byte) error {
	if db.state != stateNew {
		return fmt.Errorf("wrong state, expected %d got %d", stateNew, db.state)
	}
	if len(key) != 32 {
		return fmt.Errorf("expected key of length 32, got %d", len(key))
	}

	conn, err := db.setupConnection(key)
	if err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	db.state = stateInitialized
	return nil
}

func (db *Database) Vacuum() error {
	return db.Lock("vacuuming", func() error {
		if _, err := db.Conn.Exec("VACUUM"); err != nil {
			return err
		}
		return nil
	})
}

func (db *Database) Initialized() bool {
	return db.state == stateInitialized
}

func (db *Database) DB() *sql.DB {
	return db.Conn.DB
}

func (db *Database) Open(key []byte) error {
	if db.state != stateInitialized {
		return fmt.Errorf("wrong state, expected %d got %d", stateInitialized, db.state)
	}
	if len(key) != 32 {
		return fmt.Errorf("expected key of length 32, got %d", len(key))
	}

	conn, err := db.setupConnection(key)
	if err != nil {
		return err
	}
	db.Conn = conn
	db.state = stateRunning
	return nil
}

func (db *Database) Shutdown() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.cancelFn()
	if db.Conn == nil {
		return nil
	}
	if err := db.Conn.Close(); err != nil {
		return err
	}
	ctx, cancelFn := context.WithCancel(context.TODO())
	db.ctx = ctx
	db.cancelFn = cancelFn
	db.state = stateInitialized
	return nil
}

func (db *Database) Migrate(name string, migrations []*migration.Migration) error {
	m, err := newMigrator(db.config, db, name, migrations, true)
	if err != nil {
		return err
	}
	return m.migrate()
}

func (db *Database) MigrateNoLock(name string, migrations []*migration.Migration) error {
	m, err := newMigrator(db.config, db, name, migrations, false)
	if err != nil {
		return err
	}
	return m.migrate()
}

func (db *Database) AfterCommit(f func()) {
	if db.Tx == nil {
		panic("db: expected tx to be not nil")
	}

	db.callbacks = append(db.callbacks, f)
}

func (db *Database) BeforeCommit(f RunnerFunc) {
	if db.Tx == nil {
		panic("db: expected tx to be not nil")
	}

	db.beforeCommitCallbacks = append(db.beforeCommitCallbacks, f)
}

func (db *Database) Lock(label string, runner RunnerFunc) error {
	start := time.Now()
	db.Log.Debugf("Starting %s", label)
	db.lock.Lock()
	obtained := time.Now()
	db.Log.Debugf("Obtained lock %s", label)
	defer func() {
		db.Log.Debugf("Completed lock %s wait=%s exec=%s", label, obtained.Sub(start), time.Since(obtained))
		db.lock.Unlock()
	}()
	return runner()
}

func (db *Database) RunTx(label string, txOptions *sql.TxOptions, runner RunnerFunc) error {
	if db.Tx != nil {
		panic("db: expected tx to be nil")
	}

	defer func() {
		db.Tx = nil
	}()

	var err error
	db.Tx, err = db.Conn.BeginTxx(db.ctx, txOptions)
	if err != nil {
		db.Tx = nil
		return fmt.Errorf("db: error starting transaction for %s: %w", label, err)
	}
	if _, err = db.Tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
		db.Tx = nil
		return fmt.Errorf("db: error enabling defer_foreign_keys: %w", err)
	}

	db.callbacks = make([]func(), 0)
	db.beforeCommitCallbacks = make([]func() error, 0)
	runerr := runner()
	if runerr == nil {
		for _, c := range db.beforeCommitCallbacks {
			runerr = c()
			if runerr != nil {
				break
			}
		}
	}

	if runerr != nil {
		db.Log.Warnf("rolling back %s due to %#v", label, runerr)
		if err := db.Tx.Rollback(); err != nil {
			db.Log.Debugf("rrror while rolling back %s with %#v", label, err)
		}
		return fmt.Errorf("error during %s: %w", label, runerr)
	}
	db.Log.Debugf("committing %s", label)
	if err := db.Tx.Commit(); err != nil {
		db.Log.Warnf("error while committing %s with %#v '%s'", label, err, err.Error())
	} else {
		for _, f := range db.callbacks {
			go f()
		}
		db.callbacks = nil
	}
	return nil
}

func (db *Database) Run(label string, runner RunnerFunc) error {
	return db.Lock(label, func() error {
		return db.RunTx(label, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false}, runner)
	})
}

func (db *Database) RunReadOnly(label string, runner RunnerFunc) error {
	return db.Lock(label, func() error {
		return db.RunTx(label, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: true}, runner)
	})
}

func (db *Database) setupConnection(key []byte) (*sqlx.DB, error) {
	formattedPath := fmt.Sprintf("file:%s?_locking_mode=EXCLUSIVE&_busy_timeout=100&_secure_delete=on&_journal_mode=WAL&_auto_vacuum=2&_synchronous=3&cache=private&mode=rwc&_pragma_key=x'%x'", url.PathEscape(db.path), key)
	conn, err := sqlx.Open("sqlite3_slick", formattedPath)
	if err != nil {
		return nil, fmt.Errorf("db: error opening %s %w", db.path, err)
	}

	conn.DB.SetMaxOpenConns(1)

	if _, err := conn.Exec("SELECT name FROM sqlite_master limit 1"); err != nil {
		return nil, fmt.Errorf("db: unable to read from database: %w", err)
	}
	if _, err := conn.Exec("pragma busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("db: error setting foreign_keys to ON: %w", err)
	}
	if _, err := conn.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("db: error setting foreign_keys to ON: %w", err)
	}
	if _, err := conn.Exec("PRAGMA temp_store = 2"); err != nil {
		return nil, fmt.Errorf("db: error setting foreign_keys to ON: %w", err)
	}
	return conn, nil
}

func (db *Database) registerDriver() {
	drivers := sql.Drivers()
	for _, d := range drivers {
		if d == "sqlite3_slick" {
			return
		}
	}
	sql.Register("sqlite3_slick",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("eav_get", currentDB.EAVHandler.eavGet, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("eav_set", currentDB.EAVHandler.eavSet, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("eav_has", currentDB.EAVHandler.eavHas, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("eav_mtime", currentDB.EAVHandler.eavMtime, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("eav_ctime", currentDB.EAVHandler.eavCtime, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("eav_wtime", currentDB.EAVHandler.eavWtime, true); err != nil {
					return err
				}
				return conn.RegisterFunc("eav_cb", func(viewName string, phase int, groupID, id []byte) string {
					if err := currentDB.EAVHandler.callback(viewName, phase, groupID, id); err != nil {
						return err.Error()
					}
					return ""
				}, true)
			},
		})
}
