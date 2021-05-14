package db

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/starkandwayne/goutils/log"
)

type DB struct {
	Driver string
	DSN    string

	exclusive  sync.Mutex
	connection *sql.DB
	cache      map[string]*sql.Stmt
}

func (db *DB) Copy() *DB {
	return &DB{
		Driver: db.Driver,
		DSN:    db.DSN,
	}
}

// Are we connected?
func (db *DB) Connected() bool {
	if db.connection == nil {
		return false
	}
	return true
}

// Connect to the backend database
func (db *DB) Connect() error {
	connection, err := sql.Open(db.Driver, db.DSN)
	if err != nil {
		return err
	}

	db.connection = connection
	if db.cache == nil {
		db.cache = make(map[string]*sql.Stmt)
	}
	return nil
}

// Disconnect from the backend database
func (db *DB) Disconnect() error {
	if db.connection != nil {
		if err := db.connection.Close(); err != nil {
			return err
		}
		db.connection = nil
		db.cache = make(map[string]*sql.Stmt)
	}
	return nil
}

// Execute a named, non-data query (INSERT, UPDATE, DELETE, etc.)
func (db *DB) Exec(sql string, args ...interface{}) error {
	db.exclusive.Lock()
	defer db.exclusive.Unlock()

	s, err := db.statement(sql)
	if err != nil {
		return err
	}

	log.Debugf("Parameters: %v", args)
	_, err = s.Exec(args...)
	if err != nil {
		return err
	}

	return nil
}

// Execute a named, data query (SELECT)
func (db *DB) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	db.exclusive.Lock()
	defer db.exclusive.Unlock()

	s, err := db.statement(sql)
	if err != nil {
		return nil, err
	}

	log.Debugf("Parameters: %v", args)
	r, err := s.Query(args...)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Execute a data query (SELECT) and return how many rows were returned
func (db *DB) Count(sql string, args ...interface{}) (uint, error) {
	r, err := db.Query(sql, args...)
	if err != nil {
		return 0, err
	}

	var n uint = 0
	for r.Next() {
		n++
	}
	r.Close()
	return n, nil
}

// Return the prepared Statement for a given SQL query
func (db *DB) statement(sql string) (*sql.Stmt, error) {
	if db.connection == nil {
		return nil, fmt.Errorf("Not connected to database")
	}

	log.Debugf("Executing SQL: %s", sql)

	q, ok := db.cache[sql]
	if !ok {
		stmt, err := db.connection.Prepare(sql)
		if err != nil {
			return nil, err
		}
		db.cache[sql] = stmt
	}

	q, _ = db.cache[sql]
	return q, nil
}
