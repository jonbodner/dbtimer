package dbtimer

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"
)

func init() {
	sql.Register("timer", &Driver{})
}

type TimerInfo struct {
	Method string
	Query  string
	Start  time.Time
	End    time.Time
	Args   []driver.Value
	Err    error
}

type TimerLogger interface {
	Log(TimerInfo)
}

var timerLogger TimerLogger

func SetTimerLogger(tl TimerLogger) {
	timerLogger = tl
}

type TimerLoggerFunc func(TimerInfo)

func (tlf TimerLoggerFunc) Log(ti TimerInfo) {
	tlf(ti)
}

func SetTimerLoggerFunc(lf TimerLoggerFunc) {
	timerLogger = lf
}

func doTiming(method string, query string, args []driver.Value, c func() error) {
	var s time.Time
	if timerLogger != nil {
		s = time.Now()
	}
	err := c()
	if timerLogger != nil {
		e := time.Now()
		timerLogger.Log(TimerInfo{
			Method: method,
			Query:  query,
			Start:  s,
			End:    e,
			Err:    err,
			Args:   args,
		})
	}
}

type Driver struct {
	driverName       string
	connectionString string
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if d.driverName == "" {
		parts := strings.SplitN(name, " ", 2)
		if len(parts) != 2 {
			return nil, errors.New("Invalid format for timer ")
		}
		d.driverName = parts[0]
		d.connectionString = parts[1]
	}
	var err error
	var c driver.Conn
	doTiming("driver.Open", name, nil, func() error {
		var db *sql.DB
		db, err = sql.Open(d.driverName, d.connectionString)
		if err != nil {
			return err
		}
		c, err = db.Driver().Open(d.connectionString)
		if _, ok := c.(driver.Execer); ok {
			c = &Conn{c}
		} else {
			c = &NoExecConn{c}
		}
		return err
	})
	return c, err
}

type Conn struct {
	c driver.Conn
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	var err error
	var s driver.Stmt
	doTiming("conn.Prepare", query, nil, func() error {
		s, err = c.c.Prepare(query)
		s = &Stmt{s, query}
		return err
	})
	return s, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	var err error
	var r driver.Result
	doTiming("conn.Exec", query, args, func() error {
		r, err = c.c.(driver.Execer).Exec(query, args)
		return err
	})
	return r, err
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() error {
	var err error
	doTiming("conn.Close", "", nil, func() error {
		err = c.c.Close()
		return err
	})
	return err
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	var tx driver.Tx
	var err error
	doTiming("conn.Begin", "", nil, func() error {
		tx, err = c.c.Begin()
		tx = &Tx{tx}
		return err
	})
	return tx, err
}

type NoExecConn struct {
	c driver.Conn
}

// Prepare returns a prepared statement, bound to this connection.
func (c *NoExecConn) Prepare(query string) (driver.Stmt, error) {
	var err error
	var s driver.Stmt
	doTiming("conn.Prepare", query, nil, func() error {
		s, err = c.c.Prepare(query)
		s = &Stmt{s, query}
		return err
	})
	return s, err
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *NoExecConn) Close() error {
	var err error
	doTiming("conn.Close", "", nil, func() error {
		err = c.c.Close()
		return err
	})
	return err
}

// Begin starts and returns a new transaction.
func (c *NoExecConn) Begin() (driver.Tx, error) {
	var tx driver.Tx
	var err error
	doTiming("conn.Begin", "", nil, func() error {
		tx, err = c.c.Begin()
		tx = &Tx{tx}
		return err
	})
	return tx, err
}

type Stmt struct {
	s     driver.Stmt
	query string
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() error {
	var err error
	doTiming("stmt.Close", "", nil, func() error {
		err = s.s.Close()
		return err
	})
	return err
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	return s.s.NumInput()
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	var r driver.Result
	var err error
	doTiming("stmt.Exec", s.query, args, func() error {
		r, err = s.s.Exec(args)
		return err
	})
	return r, err
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var r driver.Rows
	var err error
	doTiming("stmt.Query", s.query, args, func() error {
		r, err = s.s.Query(args)
		return err
	})
	return r, err
}

type Tx struct {
	tx driver.Tx
}

func (t *Tx) Commit() error {
	var err error
	doTiming("tx.Commit", "", nil, func() error {
		err = t.tx.Commit()
		return err
	})
	return err
}

func (t *Tx) Rollback() error {
	var err error
	doTiming("tx.Rollback", "", nil, func() error {
		err = t.tx.Rollback()
		return err
	})
	return err
}
