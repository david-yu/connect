// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Function pointers for DB2 CLI functions
// These are registered at library load time

// Connection Management Functions
var (
	sqlAllocHandle    func(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN
	sqlFreeHandle     func(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN
	sqlConnect        func(hdbc SQLHDBC, dsn *SQLCHAR, dsnLen SQLSMALLINT, uid *SQLCHAR, uidLen SQLSMALLINT, pwd *SQLCHAR, pwdLen SQLSMALLINT) SQLRETURN
	sqlDriverConnect  func(hdbc SQLHDBC, hwnd uintptr, connStr *SQLCHAR, connStrLen SQLSMALLINT, outConnStr *SQLCHAR, outConnStrMax SQLSMALLINT, outConnStrLen *SQLSMALLINT, driverCompletion SQLUSMALLINT) SQLRETURN
	sqlDisconnect     func(hdbc SQLHDBC) SQLRETURN
	sqlSetEnvAttr     func(henv SQLHENV, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlSetConnectAttr func(hdbc SQLHDBC, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlGetConnectAttr func(hdbc SQLHDBC, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
	sqlGetInfo        func(hdbc SQLHDBC, infoType SQLUSMALLINT, infoValue SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN
)

// Statement Functions
var (
	sqlExecDirect  func(hstmt SQLHSTMT, sql *SQLCHAR, sqlLen SQLINTEGER) SQLRETURN
	sqlPrepare     func(hstmt SQLHSTMT, sql *SQLCHAR, sqlLen SQLINTEGER) SQLRETURN
	sqlExecute     func(hstmt SQLHSTMT) SQLRETURN
	sqlFetch       func(hstmt SQLHSTMT) SQLRETURN
	sqlFetchScroll func(hstmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN
	sqlCloseCursor func(hstmt SQLHSTMT) SQLRETURN
	sqlFreeStmt    func(hstmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN
	sqlSetStmtAttr func(hstmt SQLHSTMT, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlGetStmtAttr func(hstmt SQLHSTMT, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
	sqlRowCount    func(hstmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN
	sqlMoreResults func(hstmt SQLHSTMT) SQLRETURN
)

// Result Set Functions
var (
	sqlNumResultCols func(hstmt SQLHSTMT, colCount *SQLSMALLINT) SQLRETURN
	sqlDescribeCol   func(hstmt SQLHSTMT, colNum SQLUSMALLINT, colName *SQLCHAR, colNameMax SQLSMALLINT, colNameLen, dataType *SQLSMALLINT, colSize *SQLULEN, decimalDigits, nullable *SQLSMALLINT) SQLRETURN
	sqlColAttribute  func(hstmt SQLHSTMT, colNum, fieldID SQLUSMALLINT, charAttr SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT, numAttr *SQLLEN) SQLRETURN
	sqlBindCol       func(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
	sqlGetData       func(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
)

// Parameter Functions
var (
	sqlBindParameter func(hstmt SQLHSTMT, paramNum SQLUSMALLINT, inputOutputType, valueType, paramType SQLSMALLINT, colSize SQLULEN, decimalDigits SQLSMALLINT, paramValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
	sqlNumParams     func(hstmt SQLHSTMT, paramCount *SQLSMALLINT) SQLRETURN
)

// Transaction Functions
var (
	sqlEndTran func(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN
)

// Diagnostic Functions
var (
	sqlGetDiagRec   func(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN
	sqlGetDiagField func(handleType SQLSMALLINT, handle SQLHANDLE, recNum, diagID SQLSMALLINT, diagInfo SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN
	sqlError        func(henv SQLHENV, hdbc SQLHDBC, hstmt SQLHSTMT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN
)

// registerConnectionFunctions registers connection-related DB2 CLI functions
func registerConnectionFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlAllocHandle, "SQLAllocHandle"},
		{&sqlFreeHandle, "SQLFreeHandle"},
		{&sqlConnect, "SQLConnect"},
		{&sqlDriverConnect, "SQLDriverConnect"},
		{&sqlDisconnect, "SQLDisconnect"},
		{&sqlSetEnvAttr, "SQLSetEnvAttr"},
		{&sqlSetConnectAttr, "SQLSetConnectAttr"},
		{&sqlGetConnectAttr, "SQLGetConnectAttr"},
		{&sqlGetInfo, "SQLGetInfo"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerStatementFunctions registers statement-related DB2 CLI functions
func registerStatementFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlExecDirect, "SQLExecDirect"},
		{&sqlPrepare, "SQLPrepare"},
		{&sqlExecute, "SQLExecute"},
		{&sqlFetch, "SQLFetch"},
		{&sqlFetchScroll, "SQLFetchScroll"},
		{&sqlCloseCursor, "SQLCloseCursor"},
		{&sqlFreeStmt, "SQLFreeStmt"},
		{&sqlSetStmtAttr, "SQLSetStmtAttr"},
		{&sqlGetStmtAttr, "SQLGetStmtAttr"},
		{&sqlRowCount, "SQLRowCount"},
		{&sqlMoreResults, "SQLMoreResults"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerResultFunctions registers result set-related DB2 CLI functions
func registerResultFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlNumResultCols, "SQLNumResultCols"},
		{&sqlDescribeCol, "SQLDescribeCol"},
		{&sqlColAttribute, "SQLColAttribute"},
		{&sqlBindCol, "SQLBindCol"},
		{&sqlGetData, "SQLGetData"},
		{&sqlBindParameter, "SQLBindParameter"},
		{&sqlNumParams, "SQLNumParams"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerTransactionFunctions registers transaction-related DB2 CLI functions
func registerTransactionFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlEndTran, "SQLEndTran"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerDiagnosticFunctions registers diagnostic-related DB2 CLI functions
func registerDiagnosticFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlGetDiagRec, "SQLGetDiagRec"},
		{&sqlGetDiagField, "SQLGetDiagField"},
		{&sqlError, "SQLError"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// Public wrapper functions that call the registered function pointers
// These ensure the library is loaded before calling

// SQLAllocHandle allocates an environment, connection, or statement handle
func SQLAllocHandle(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		panic(fmt.Sprintf("DB2 CLI not loaded: %v", err))
	}
	return sqlAllocHandle(handleType, inputHandle, outputHandle)
}

// SQLFreeHandle frees a handle
func SQLFreeHandle(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFreeHandle(handleType, handle)
}

// SQLConnect establishes a connection to a data source
func SQLConnect(hdbc SQLHDBC, dsn, uid, pwd string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	dsnBytes := append([]byte(dsn), 0)
	uidBytes := append([]byte(uid), 0)
	pwdBytes := append([]byte(pwd), 0)

	return sqlConnect(
		hdbc,
		(*SQLCHAR)(unsafe.Pointer(&dsnBytes[0])), SQLSMALLINT(len(dsn)),
		(*SQLCHAR)(unsafe.Pointer(&uidBytes[0])), SQLSMALLINT(len(uid)),
		(*SQLCHAR)(unsafe.Pointer(&pwdBytes[0])), SQLSMALLINT(len(pwd)),
	)
}

// SQLDriverConnect establishes a connection using a connection string
func SQLDriverConnect(hdbc SQLHDBC, connStr string) (string, SQLRETURN) {
	if err := LoadLibrary(); err != nil {
		return "", SQL_ERROR
	}

	connStrBytes := append([]byte(connStr), 0)
	outConnStr := make([]byte, 1024)
	var outConnStrLen SQLSMALLINT

	ret := sqlDriverConnect(
		hdbc,
		0, // No window handle
		(*SQLCHAR)(unsafe.Pointer(&connStrBytes[0])), SQLSMALLINT(len(connStr)),
		(*SQLCHAR)(unsafe.Pointer(&outConnStr[0])), 1024,
		&outConnStrLen,
		0, // SQL_DRIVER_NOPROMPT
	)

	return string(outConnStr[:outConnStrLen]), ret
}

// SQLDisconnect closes a connection
func SQLDisconnect(hdbc SQLHDBC) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDisconnect(hdbc)
}

// SQLSetEnvAttr sets an environment attribute
func SQLSetEnvAttr(henv SQLHENV, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetEnvAttr(henv, attr, value, strLen)
}

// SQLSetConnectAttr sets a connection attribute
func SQLSetConnectAttr(hdbc SQLHDBC, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetConnectAttr(hdbc, attr, value, strLen)
}

// SQLGetInfo retrieves general information about the driver and data source
func SQLGetInfo(hdbc SQLHDBC, infoType SQLUSMALLINT, infoValue SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetInfo(hdbc, infoType, infoValue, bufLen, strLen)
}

// SQLExecDirect executes a SQL statement directly
func SQLExecDirect(hstmt SQLHSTMT, sql string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	sqlBytes := append([]byte(sql), 0)
	return sqlExecDirect(hstmt, (*SQLCHAR)(unsafe.Pointer(&sqlBytes[0])), SQLINTEGER(len(sql)))
}

// SQLPrepare prepares a SQL statement for execution
func SQLPrepare(hstmt SQLHSTMT, sql string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	sqlBytes := append([]byte(sql), 0)
	return sqlPrepare(hstmt, (*SQLCHAR)(unsafe.Pointer(&sqlBytes[0])), SQLINTEGER(len(sql)))
}

// SQLExecute executes a prepared statement
func SQLExecute(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlExecute(hstmt)
}

// SQLFetch fetches the next row from the result set
func SQLFetch(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFetch(hstmt)
}

// SQLFetchScroll fetches a rowset from the result set
func SQLFetchScroll(hstmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFetchScroll(hstmt, fetchOrientation, fetchOffset)
}

// SQLCloseCursor closes a cursor
func SQLCloseCursor(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlCloseCursor(hstmt)
}

// SQLFreeStmt frees a statement handle or unbinds columns
func SQLFreeStmt(hstmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFreeStmt(hstmt, option)
}

// SQLNumResultCols returns the number of columns in the result set
func SQLNumResultCols(hstmt SQLHSTMT, colCount *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlNumResultCols(hstmt, colCount)
}

// SQLDescribeCol describes a column in the result set
func SQLDescribeCol(hstmt SQLHSTMT, colNum SQLUSMALLINT, colName *SQLCHAR, colNameMax SQLSMALLINT, colNameLen, dataType *SQLSMALLINT, colSize *SQLULEN, decimalDigits, nullable *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDescribeCol(hstmt, colNum, colName, colNameMax, colNameLen, dataType, colSize, decimalDigits, nullable)
}

// SQLGetData retrieves data from a column in the result set
func SQLGetData(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetData(hstmt, colNum, targetType, targetValue, bufLen, indicator)
}

// SQLBindCol binds a column to a program variable
func SQLBindCol(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlBindCol(hstmt, colNum, targetType, targetValue, bufLen, indicator)
}

// SQLRowCount returns the number of rows affected by an UPDATE, INSERT, or DELETE
func SQLRowCount(hstmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlRowCount(hstmt, rowCount)
}

// SQLEndTran commits or rolls back a transaction
func SQLEndTran(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlEndTran(handleType, handle, completionType)
}

// SQLGetDiagRec retrieves diagnostic information
func SQLGetDiagRec(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetDiagRec(handleType, handle, recNum, sqlState, nativeError, messageText, bufLen, textLen)
}

// SQLBindParameter binds a parameter marker in a SQL statement.
func SQLBindParameter(hstmt SQLHSTMT, paramNum SQLUSMALLINT, inputOutputType, valueType, paramType SQLSMALLINT, colSize SQLULEN, decimalDigits SQLSMALLINT, paramValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlBindParameter(hstmt, paramNum, inputOutputType, valueType, paramType, colSize, decimalDigits, paramValue, bufLen, indicator)
}
