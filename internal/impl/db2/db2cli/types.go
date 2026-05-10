// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import "unsafe"

// DB2 CLI Type Definitions
// These types map to the DB2 CLI C types without requiring CGO

// SQLHANDLE represents a generic handle (void pointer in C)
type SQLHANDLE uintptr

// SQLHENV represents an environment handle
type SQLHENV SQLHANDLE

// SQLHDBC represents a connection handle
type SQLHDBC SQLHANDLE

// SQLHSTMT represents a statement handle
type SQLHSTMT SQLHANDLE

// SQLHDESC represents a descriptor handle
type SQLHDESC SQLHANDLE

// SQLRETURN represents the return code from DB2 CLI functions
type SQLRETURN int16

// SQLSMALLINT represents a 16-bit integer
type SQLSMALLINT int16

// SQLUSMALLINT represents an unsigned 16-bit integer
type SQLUSMALLINT uint16

// SQLINTEGER represents a 32-bit integer
type SQLINTEGER int32

// SQLUINTEGER represents an unsigned 32-bit integer
type SQLUINTEGER uint32

// SQLLEN represents a length value (platform-specific)
// On 64-bit systems this is 64-bit, on 32-bit it's 32-bit
type SQLLEN int64

// SQLULEN represents an unsigned length value
type SQLULEN uint64

// SQLPOINTER represents a generic pointer
type SQLPOINTER unsafe.Pointer

// SQLCHAR represents a single byte character
type SQLCHAR byte

// SQLWCHAR represents a wide character (2 bytes)
type SQLWCHAR uint16

// SQLBigInt represents a 64-bit integer
type SQLBigInt int64

// SQLUBigInt represents an unsigned 64-bit integer
type SQLUBigInt uint64

// SQLReal represents a single-precision float
type SQLReal float32

// SQLDouble represents a double-precision float
type SQLDouble float64

// SQLDate represents a date structure
type SQLDate struct {
	Year  SQLSMALLINT
	Month SQLUSMALLINT
	Day   SQLUSMALLINT
}

// SQLTime represents a time structure
type SQLTime struct {
	Hour   SQLUSMALLINT
	Minute SQLUSMALLINT
	Second SQLUSMALLINT
}

// SQLTimestamp represents a timestamp structure
type SQLTimestamp struct {
	Year     SQLSMALLINT
	Month    SQLUSMALLINT
	Day      SQLUSMALLINT
	Hour     SQLUSMALLINT
	Minute   SQLUSMALLINT
	Second   SQLUSMALLINT
	Fraction SQLUINTEGER // nanoseconds
}

// SQLNumeric represents a numeric/decimal value
type SQLNumeric struct {
	Precision SQLCHAR
	Scale     SQLCHAR
	Sign      SQLCHAR // 1 = positive, 0 = negative
	Val       [16]SQLCHAR
}
