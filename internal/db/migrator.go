package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/meow-io/go-slick/config"
	"github.com/meow-io/go-slick/migration"
	"go.uber.org/zap"
)

// migrator is the migrator implementation
type migrator struct {
	db         *Database
	name       string
	tableName  string
	log        *zap.SugaredLogger
	migrations []*migration.Migration
	lock       bool
}

// New creates a new migrator instance
func newMigrator(c *config.Config, db *Database, name string, migrations []*migration.Migration, lock bool) (*migrator, error) {
	m := &migrator{
		db:         db,
		log:        c.Logger(name),
		name:       name,
		tableName:  fmt.Sprintf("_migrations_%s", name),
		migrations: migrations,
		lock:       lock,
	}

	return m, nil
}

// Migrate applies all available migrations
func (m *migrator) migrate() error {
	var count int
	if err := m.run(fmt.Sprintf("prepare %s migrator", m.name), func() error {
		// create migrations table if doesn't exist
		_, err := m.db.Tx.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT8 NOT NULL,
			version VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`, m.tableName))
		if err != nil {
			return err
		}

		// count applied migrations
		count, err = m.countApplied()
		if err != nil {
			return err
		}

		if count > len(m.migrations) {
			return errors.New("migrator: applied migration number on db cannot be greater than the defined migration list")
		}
		return nil
	}); err != nil {
		return err
	}

	// plan migrations
	for idx, migration := range m.migrations[count:len(m.migrations)] {
		insertVersion := fmt.Sprintf("INSERT INTO %s (id, version) VALUES (%d, '%s')", m.tableName, idx+count, migration.String())
		if err := m.performMigration(insertVersion, migration); err != nil {
			return fmt.Errorf("migrator: error while running migrations: %v", err)
		}
	}
	return nil
}

func (m *migrator) countApplied() (int, error) {
	// count applied migrations
	var count int
	rows, err := m.db.Tx.Query(fmt.Sprintf("SELECT count(*) FROM %s", m.tableName))
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func (m *migrator) performMigration(insertVersion string, migration *migration.Migration) error {
	return m.run(migration.String(), func() error {
		m.log.Debugf("applying migration named '%s'...", migration.Name)
		if err := migration.Func(m.db.Tx.Tx); err != nil {
			return fmt.Errorf("error executing golang migration: %s", err)
		}
		if _, err := m.db.Tx.Exec(insertVersion); err != nil {
			return fmt.Errorf("error updating migration versions: %s", err)
		}
		m.log.Debugf("applied migration named '%s'", migration.Name)
		return nil
	})
}

func (m *migrator) run(label string, f runnerFunc) error {
	if m.lock {
		return m.db.Run(label, f)
	}
	return m.db.runTx(label, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false}, f)
}
