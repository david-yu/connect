// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package db2 provides a database/sql driver for IBM DB2 implemented without
// CGO. The driver is named "db2-cli" and registered via database/sql.Register
// in the package's init function.
//
// The driver loads the IBM DB2 CLI shared library at runtime using
// github.com/ebitengine/purego (dlopen/LoadLibrary), so no C compiler or CGO
// toolchain is required at build time. The CLI (ODBC-compatible C API) call
// sequence is: SQLAllocHandle → SQLDriverConnect → SQLPrepare →
// SQLBindParameter → SQLExecute → SQLFetch → SQLGetData → SQLFreeHandle.
//
// Parameter binding uses SQL_C_CHAR / SQL_VARCHAR for all types. CDC stream
// queries embed binary CSN values as hex literals (X'...') to avoid needing
// SQL_C_BINARY for CHAR FOR BIT DATA columns.
//
// Memory safety: bindParams returns both the data buffers (bufs) and the
// indicator lengths (inds). Both must be kept alive via runtime.KeepAlive until
// after SQLExecute returns. The Go GC does not track C-side pointer references,
// so any local variable whose address is passed to SQLBindParameter must be
// heap-allocated (via a slice) and explicitly retained.
package db2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"unsafe"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
)

// db2Driver implements database/sql/driver.Driver using purego DB2 CLI bindings.
type db2Driver struct{}

func init() {
	sql.Register("db2-cli", &db2Driver{})
}

func (*db2Driver) Open(dsn string) (driver.Conn, error) {
	if err := db2cli.LoadLibrary(); err != nil {
		return nil, fmt.Errorf("loading DB2 CLI library: %w", err)
	}

	var henv db2cli.SQLHENV
	ret := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(db2cli.SQL_NULL_HENV), (*db2cli.SQLHANDLE)(&henv))
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return nil, errors.New("allocating environment handle")
	}

	ret = db2cli.SQLSetEnvAttr(henv, db2cli.SQL_ATTR_ODBC_VERSION, db2cli.SQL_OV_ODBC3, 0)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, errors.New("setting ODBC version")
	}

	var hdbc db2cli.SQLHDBC
	ret = db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(henv), (*db2cli.SQLHANDLE)(&hdbc))
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, errors.New("allocating connection handle")
	}

	_, ret = db2cli.SQLDriverConnect(hdbc, dsn)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(hdbc))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(hdbc))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, fmt.Errorf("connecting: %w", err)
	}

	return &db2Conn{henv: henv, hdbc: hdbc}, nil
}

// db2Conn implements driver.Conn.
type db2Conn struct {
	henv db2cli.SQLHENV
	hdbc db2cli.SQLHDBC
}

func (c *db2Conn) Prepare(query string) (driver.Stmt, error) {
	var hstmt db2cli.SQLHSTMT
	ret := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(c.hdbc), (*db2cli.SQLHANDLE)(&hstmt))
	if ret != db2cli.SQL_SUCCESS {
		return nil, errors.New("allocating statement handle")
	}

	ret = db2cli.SQLPrepare(hstmt, query)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(hstmt))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(hstmt))
		return nil, fmt.Errorf("preparing statement: %w", err)
	}

	return &db2Stmt{hstmt: hstmt, conn: c}, nil
}

func (c *db2Conn) Close() error {
	if c.hdbc != 0 {
		db2cli.SQLDisconnect(c.hdbc)
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(c.hdbc))
		c.hdbc = 0
	}
	if c.henv != 0 {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(c.henv))
		c.henv = 0
	}
	return nil
}

func (c *db2Conn) Begin() (driver.Tx, error) {
	ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_OFF, 0)
	if ret != db2cli.SQL_SUCCESS {
		return nil, errors.New("beginning transaction")
	}
	return &db2Tx{conn: c}, nil
}

// BeginTx implements driver.ConnBeginTx so that sql.TxOptions.Isolation is honoured.
// Without this, database/sql falls back to Begin() and silently drops the isolation level.
func (c *db2Conn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		isoConst, err := isolationLevelToDB2(sql.IsolationLevel(opts.Isolation))
		if err != nil {
			return nil, err
		}
		ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_TXN_ISOLATION, isoConst, 0)
		if ret != db2cli.SQL_SUCCESS {
			return nil, fmt.Errorf("setting transaction isolation level: %w",
				db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(c.hdbc)))
		}
	}
	ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_OFF, 0)
	if ret != db2cli.SQL_SUCCESS {
		return nil, errors.New("beginning transaction")
	}
	return &db2Tx{conn: c}, nil
}

// isolationLevelToDB2 maps a sql.IsolationLevel to the corresponding DB2 CLI constant.
func isolationLevelToDB2(level sql.IsolationLevel) (uintptr, error) {
	switch level {
	case sql.LevelReadUncommitted:
		return db2cli.SQL_TXN_READ_UNCOMMITTED, nil
	case sql.LevelReadCommitted:
		return db2cli.SQL_TXN_READ_COMMITTED, nil
	case sql.LevelRepeatableRead:
		return db2cli.SQL_TXN_REPEATABLE_READ, nil
	case sql.LevelSerializable:
		return db2cli.SQL_TXN_SERIALIZABLE, nil
	default:
		return 0, fmt.Errorf("unsupported transaction isolation level: %v", level)
	}
}

// db2Tx implements driver.Tx.
type db2Tx struct {
	conn *db2Conn
}

func (tx *db2Tx) Commit() error {
	ret := db2cli.SQLEndTran(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc), db2cli.SQL_COMMIT)
	if ret != db2cli.SQL_SUCCESS {
		return errors.New("committing transaction")
	}
	db2cli.SQLSetConnectAttr(tx.conn.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_ON, 0)
	return nil
}

func (tx *db2Tx) Rollback() error {
	ret := db2cli.SQLEndTran(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc), db2cli.SQL_ROLLBACK)
	if ret != db2cli.SQL_SUCCESS {
		return errors.New("rolling back transaction")
	}
	db2cli.SQLSetConnectAttr(tx.conn.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_ON, 0)
	return nil
}

// db2Stmt implements driver.Stmt.
type db2Stmt struct {
	hstmt db2cli.SQLHSTMT
	conn  *db2Conn
}

func (s *db2Stmt) Close() error {
	if s.hstmt != 0 {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		s.hstmt = 0
	}
	return nil
}

func (*db2Stmt) NumInput() int {
	return -1
}

// bindParams binds each driver.Value to the prepared statement as SQL_C_CHAR.
// Returns (bufs, inds): both slices must stay alive via runtime.KeepAlive until
// after SQLExecute returns. bufs holds the null-terminated data buffers;
// inds holds the length indicators. The Go GC does not track C-side pointer
// references, so local variables whose addresses are passed to SQLBindParameter
// must live in heap-allocated slices that are explicitly retained.
func (s *db2Stmt) bindParams(args []driver.Value) (bufs [][]byte, inds []db2cli.SQLLEN, err error) {
	bufs = make([][]byte, len(args))
	inds = make([]db2cli.SQLLEN, len(args))

	for i, arg := range args {
		pos := db2cli.SQLUSMALLINT(i + 1)

		if arg == nil {
			inds[i] = db2cli.SQLLEN(db2cli.SQL_NULL_DATA)
			ret := db2cli.SQLBindParameter(
				s.hstmt, pos,
				db2cli.SQL_PARAM_INPUT, db2cli.SQL_C_CHAR, db2cli.SQL_VARCHAR,
				0, 0, nil, 0, &inds[i],
			)
			if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
				return nil, nil, fmt.Errorf("binding NULL parameter %d: %w", i+1,
					db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt)))
			}
			continue
		}

		strVal := fmt.Sprintf("%v", arg)
		// Null-terminate so DB2 CLI can treat it as a C string.
		buf := append([]byte(strVal), 0)
		bufs[i] = buf

		inds[i] = db2cli.SQLLEN(len(strVal))
		ret := db2cli.SQLBindParameter(
			s.hstmt, pos,
			db2cli.SQL_PARAM_INPUT, db2cli.SQL_C_CHAR, db2cli.SQL_VARCHAR,
			db2cli.SQLULEN(len(strVal)), 0,
			db2cli.SQLPOINTER(unsafe.Pointer(&buf[0])),
			db2cli.SQLLEN(len(buf)),
			&inds[i],
		)
		if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
			return nil, nil, fmt.Errorf("binding parameter %d: %w", i+1,
				db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt)))
		}
	}

	return bufs, inds, nil
}

func (s *db2Stmt) Exec(args []driver.Value) (driver.Result, error) {
	bufs, inds, err := s.bindParams(args)
	if err != nil {
		return nil, fmt.Errorf("binding parameters: %w", err)
	}

	ret := db2cli.SQLExecute(s.hstmt)
	runtime.KeepAlive(bufs) // keep data buffers alive until SQLExecute returns
	runtime.KeepAlive(inds) // keep indicator lengths alive until SQLExecute returns

	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		return nil, fmt.Errorf("executing statement: %w", err)
	}

	var rowCount db2cli.SQLLEN
	if r := db2cli.SQLRowCount(s.hstmt, &rowCount); r != db2cli.SQL_SUCCESS {
		rowCount = 0
	}

	return &db2Result{rowsAffected: int64(rowCount)}, nil
}

func (s *db2Stmt) Query(args []driver.Value) (driver.Rows, error) {
	bufs, inds, err := s.bindParams(args)
	if err != nil {
		return nil, fmt.Errorf("binding parameters: %w", err)
	}

	ret := db2cli.SQLExecute(s.hstmt)
	runtime.KeepAlive(bufs) // keep data buffers alive until SQLExecute returns
	runtime.KeepAlive(inds) // keep indicator lengths alive until SQLExecute returns

	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		return nil, fmt.Errorf("executing query: %w", err)
	}

	var colCount db2cli.SQLSMALLINT
	if r := db2cli.SQLNumResultCols(s.hstmt, &colCount); r != db2cli.SQL_SUCCESS {
		return nil, errors.New("getting column count")
	}

	return &db2Rows{stmt: s, colCount: int(colCount)}, nil
}

// db2Result implements driver.Result.
type db2Result struct {
	rowsAffected int64
}

func (*db2Result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported")
}

func (r *db2Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// db2Rows implements driver.Rows.
type db2Rows struct {
	stmt     *db2Stmt
	colCount int
}

func (r *db2Rows) Columns() []string {
	cols := make([]string, r.colCount)
	for i := 0; i < r.colCount; i++ {
		colName := make([]byte, 256)
		var nameLen db2cli.SQLSMALLINT
		var dataType db2cli.SQLSMALLINT
		var colSize db2cli.SQLULEN
		var decimalDigits db2cli.SQLSMALLINT
		var nullable db2cli.SQLSMALLINT

		ret := db2cli.SQLDescribeCol(
			r.stmt.hstmt,
			db2cli.SQLUSMALLINT(i+1),
			(*db2cli.SQLCHAR)(&colName[0]),
			256,
			&nameLen,
			&dataType,
			&colSize,
			&decimalDigits,
			&nullable,
		)

		if ret == db2cli.SQL_SUCCESS || ret == db2cli.SQL_SUCCESS_WITH_INFO {
			cols[i] = string(colName[:nameLen])
		} else {
			cols[i] = fmt.Sprintf("col%d", i+1)
		}
	}
	return cols
}

func (r *db2Rows) Close() error {
	ret := db2cli.SQLCloseCursor(r.stmt.hstmt)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return errors.New("closing cursor")
	}
	return nil
}

func (r *db2Rows) Next(dest []driver.Value) error {
	ret := db2cli.SQLFetch(r.stmt.hstmt)
	if ret == db2cli.SQL_NO_DATA {
		return io.EOF
	}
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(r.stmt.hstmt))
		return fmt.Errorf("fetching row: %w", err)
	}

	for i := 0; i < r.colCount; i++ {
		val, isNull, err := r.readColumnValue(db2cli.SQLUSMALLINT(i + 1))
		if err != nil || isNull {
			dest[i] = nil
		} else {
			dest[i] = val
		}
	}

	return nil
}

// readColumnValue drains one column by calling SQLGetData in a loop.
// When a value exceeds the initial buffer DB2 returns SQL_SUCCESS_WITH_INFO and
// sets indicator > bufSize; subsequent calls retrieve the remainder. This avoids
// both the silent truncation and the buf[:indicator] out-of-bounds panic that a
// single fixed-size call produces for large VARCHAR/CLOB/BLOB columns.
func (r *db2Rows) readColumnValue(colIdx db2cli.SQLUSMALLINT) (string, bool, error) {
	const bufSize = 32 * 1024 // 32 KB covers most VARCHAR values in a single call
	buf := make([]byte, bufSize)
	var sb strings.Builder

	for {
		var indicator db2cli.SQLLEN
		ret := db2cli.SQLGetData(
			r.stmt.hstmt, colIdx,
			db2cli.SQL_C_CHAR,
			db2cli.SQLPOINTER(&buf[0]),
			db2cli.SQLLEN(len(buf)),
			&indicator,
		)

		switch {
		case ret == db2cli.SQL_NO_DATA:
			// All data was drained in a previous iteration.
			return sb.String(), false, nil
		case ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO:
			return "", false, db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(r.stmt.hstmt))
		case indicator == db2cli.SQL_NULL_DATA:
			return "", true, nil
		}

		// Determine how many bytes DB2 wrote into buf.
		// If indicator >= bufSize the value was truncated; DB2 wrote exactly bufSize bytes
		// (possibly including a null-terminator at buf[bufSize-1]).
		// If indicator < 0 (SQL_NO_TOTAL) consume until the null-terminator.
		var written int
		if indicator >= 0 && int(indicator) < len(buf) {
			written = int(indicator)
		} else {
			// Truncated or SQL_NO_TOTAL: scan for null-terminator.
			written = len(buf)
			for written > 0 && buf[written-1] == 0 {
				written--
			}
		}
		sb.Write(buf[:written])

		if ret == db2cli.SQL_SUCCESS {
			// Full value retrieved in one shot.
			return sb.String(), false, nil
		}
		// SQL_SUCCESS_WITH_INFO → more data pending; loop.
	}
}
