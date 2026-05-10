// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"errors"
	"fmt"
	"unsafe"
)

// DB2Error represents a DB2 CLI error with SQLSTATE and native error code
type DB2Error struct {
	SQLState    string
	NativeError int32
	Message     string
	HandleType  SQLSMALLINT
	Handle      SQLHANDLE
}

// Error implements the error interface
func (e *DB2Error) Error() string {
	return fmt.Sprintf("DB2 Error [%s] (Native: %d): %s", e.SQLState, e.NativeError, e.Message)
}

// IsConnectionError returns true if the error is a connection-related error
func (e *DB2Error) IsConnectionError() bool {
	// SQLSTATE class 08 = Connection Exception
	return len(e.SQLState) >= 2 && e.SQLState[:2] == "08"
}

// IsIntegrityConstraintViolation returns true if the error is a constraint violation
func (e *DB2Error) IsIntegrityConstraintViolation() bool {
	// SQLSTATE class 23 = Integrity Constraint Violation
	return len(e.SQLState) >= 2 && e.SQLState[:2] == "23"
}

// IsDeadlock returns true if the error is a deadlock
func (e *DB2Error) IsDeadlock() bool {
	// SQLSTATE 40001 = Serialization failure (deadlock)
	return e.SQLState == "40001"
}

// IsTimeout returns true if the error is a timeout
func (e *DB2Error) IsTimeout() bool {
	// SQLSTATE HYT00 = Timeout expired
	// SQLSTATE HYT01 = Connection timeout expired
	return e.SQLState == "HYT00" || e.SQLState == "HYT01"
}

// GetDiagnostics retrieves all diagnostic records for a handle
func GetDiagnostics(handleType SQLSMALLINT, handle SQLHANDLE) []DB2Error {
	var errors []DB2Error

	for recNum := SQLSMALLINT(1); ; recNum++ {
		sqlState := make([]byte, 6)
		messageText := make([]byte, 1024)
		var nativeError SQLINTEGER
		var textLen SQLSMALLINT

		ret := SQLGetDiagRec(
			handleType,
			handle,
			recNum,
			(*SQLCHAR)(unsafe.Pointer(&sqlState[0])),
			&nativeError,
			(*SQLCHAR)(unsafe.Pointer(&messageText[0])),
			1024,
			&textLen,
		)

		if ret == SQL_NO_DATA {
			break
		}

		if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
			break
		}

		// Null-terminate and convert to string
		sqlStateStr := string(sqlState[:5]) // SQLSTATE is always 5 characters
		messageStr := string(messageText[:textLen])

		errors = append(errors, DB2Error{
			SQLState:    sqlStateStr,
			NativeError: int32(nativeError),
			Message:     messageStr,
			HandleType:  handleType,
			Handle:      handle,
		})
	}

	return errors
}

// GetLastError retrieves the most recent diagnostic record
func GetLastError(handleType SQLSMALLINT, handle SQLHANDLE) error {
	diags := GetDiagnostics(handleType, handle)
	if len(diags) == 0 {
		return errors.New("unknown DB2 error (no diagnostics available)")
	}
	return &diags[0]
}

// CheckReturn checks a SQLRETURN value and returns an error if it indicates failure
func CheckReturn(ret SQLRETURN, handleType SQLSMALLINT, handle SQLHANDLE, operation string) error {
	switch ret {
	case SQL_SUCCESS:
		return nil
	case SQL_SUCCESS_WITH_INFO:
		// Success with warnings - not an error, but diagnostics may be available
		return nil
	case SQL_NO_DATA:
		// No data is not an error for fetch operations
		return nil
	case SQL_INVALID_HANDLE:
		return fmt.Errorf("%s: invalid handle", operation)
	case SQL_ERROR:
		err := GetLastError(handleType, handle)
		return fmt.Errorf("%s: %w", operation, err)
	case SQL_STILL_EXECUTING:
		return fmt.Errorf("%s: operation still executing", operation)
	default:
		return fmt.Errorf("%s: unexpected return code %d", operation, ret)
	}
}

// Common SQLSTATE values for reference
const (
	// Class 00: Success
	SQLSTATE_SUCCESS = "00000"

	// Class 01: Warning
	SQLSTATE_WARNING = "01000"

	// Class 02: No Data
	SQLSTATE_NO_DATA = "02000"

	// Class 08: Connection Exception
	SQLSTATE_CONNECTION_EXCEPTION      = "08000"
	SQLSTATE_CONNECTION_DOES_NOT_EXIST = "08003"
	SQLSTATE_CONNECTION_FAILURE        = "08006"
	SQLSTATE_CONNECTION_NAME_IN_USE    = "08002"
	SQLSTATE_TRANSACTION_RESOLUTION    = "08007"

	// Class 21: Cardinality Violation
	SQLSTATE_CARDINALITY_VIOLATION = "21000"

	// Class 22: Data Exception
	SQLSTATE_DATA_EXCEPTION          = "22000"
	SQLSTATE_STRING_DATA_RIGHT_TRUNC = "22001"
	SQLSTATE_NUMERIC_VALUE_OUT_RANGE = "22003"
	SQLSTATE_INVALID_DATETIME_FORMAT = "22007"
	SQLSTATE_DIVISION_BY_ZERO        = "22012"

	// Class 23: Integrity Constraint Violation
	SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION = "23000"
	SQLSTATE_UNIQUE_VIOLATION               = "23505"
	SQLSTATE_FOREIGN_KEY_VIOLATION          = "23503"

	// Class 24: Invalid Cursor State
	SQLSTATE_INVALID_CURSOR_STATE = "24000"

	// Class 25: Invalid Transaction State
	SQLSTATE_INVALID_TRANSACTION_STATE = "25000"

	// Class 40: Transaction Rollback
	SQLSTATE_SERIALIZATION_FAILURE = "40001" // Deadlock
	SQLSTATE_TRANSACTION_ROLLBACK  = "40000"

	// Class 42: Syntax Error or Access Rule Violation
	SQLSTATE_SYNTAX_ERROR            = "42000"
	SQLSTATE_TABLE_OR_VIEW_NOT_FOUND = "42S02"
	SQLSTATE_COLUMN_NOT_FOUND        = "42S22"
	SQLSTATE_DUPLICATE_COLUMN        = "42S21"
	SQLSTATE_PERMISSION_DENIED       = "42501"

	// Class HY: CLI-specific errors
	SQLSTATE_GENERAL_ERROR           = "HY000"
	SQLSTATE_MEMORY_ALLOCATION_ERROR = "HY001"
	SQLSTATE_INVALID_HANDLE_TYPE     = "HY092"
	SQLSTATE_TIMEOUT_EXPIRED         = "HYT00"
	SQLSTATE_CONNECTION_TIMEOUT      = "HYT01"
)
