// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

// DB2 CLI Constants
// These constants are used throughout the DB2 CLI API

// Handle Types
const (
	SQL_HANDLE_ENV  = 1
	SQL_HANDLE_DBC  = 2
	SQL_HANDLE_STMT = 3
	SQL_HANDLE_DESC = 4
)

// Return Codes
const (
	SQL_SUCCESS           SQLRETURN = 0
	SQL_SUCCESS_WITH_INFO SQLRETURN = 1
	SQL_NO_DATA           SQLRETURN = 100
	SQL_ERROR             SQLRETURN = -1
	SQL_INVALID_HANDLE    SQLRETURN = -2
	SQL_STILL_EXECUTING   SQLRETURN = 2
	SQL_NEED_DATA         SQLRETURN = 99
)

// NULL Handle
const (
	SQL_NULL_HANDLE SQLHANDLE = 0
	SQL_NULL_HENV   SQLHENV   = 0
	SQL_NULL_HDBC   SQLHDBC   = 0
	SQL_NULL_HSTMT  SQLHSTMT  = 0
	SQL_NULL_HDESC  SQLHDESC  = 0
)

// Special Length Values
const (
	SQL_NTS           = -3 // Null-terminated string
	SQL_NULL_DATA     = -1 // NULL value indicator
	SQL_DATA_AT_EXEC  = -2 // Data at execution
	SQL_NO_TOTAL      = -4 // Total length unknown
	SQL_DEFAULT_PARAM = -5 // Default parameter value
	SQL_IGNORE        = -6 // Ignore this parameter
)

// Environment Attributes
const (
	SQL_ATTR_ODBC_VERSION       = 200
	SQL_ATTR_CONNECTION_POOLING = 201
	SQL_ATTR_CP_MATCH           = 202
	SQL_ATTR_OUTPUT_NTS         = 10001
	SQL_OV_ODBC3                = 3
	SQL_OV_ODBC3_80             = 380
)

// Connection Attributes
const (
	SQL_ATTR_ACCESS_MODE        = 101
	SQL_ATTR_AUTOCOMMIT         = 102
	SQL_ATTR_CONNECTION_TIMEOUT = 113
	SQL_ATTR_CURRENT_CATALOG    = 109
	SQL_ATTR_LOGIN_TIMEOUT      = 103
	SQL_ATTR_TXN_ISOLATION      = 108
	SQL_AUTOCOMMIT_OFF          = 0
	SQL_AUTOCOMMIT_ON           = 1
)

// Statement Attributes
const (
	SQL_ATTR_APP_ROW_DESC       = 10010
	SQL_ATTR_APP_PARAM_DESC     = 10011
	SQL_ATTR_IMP_ROW_DESC       = 10012
	SQL_ATTR_IMP_PARAM_DESC     = 10013
	SQL_ATTR_CURSOR_SCROLLABLE  = -1
	SQL_ATTR_CURSOR_SENSITIVITY = -2
	SQL_ATTR_QUERY_TIMEOUT      = 0
	SQL_ATTR_MAX_ROWS           = 1
	SQL_ATTR_NOSCAN             = 2
	SQL_ATTR_MAX_LENGTH         = 3
	SQL_ATTR_ASYNC_ENABLE       = 4
	SQL_ATTR_ROW_BIND_TYPE      = 5
	SQL_ATTR_CURSOR_TYPE        = 6
	SQL_ATTR_CONCURRENCY        = 7
	SQL_ATTR_ROW_ARRAY_SIZE     = 27
	SQL_ATTR_ROW_STATUS_PTR     = 25
	SQL_ATTR_ROWS_FETCHED_PTR   = 26
)

// Transaction Completion Types
const (
	SQL_COMMIT   = 0
	SQL_ROLLBACK = 1
)

// Isolation Levels
const (
	SQL_TXN_READ_UNCOMMITTED = 1
	SQL_TXN_READ_COMMITTED   = 2
	SQL_TXN_REPEATABLE_READ  = 4
	SQL_TXN_SERIALIZABLE     = 8
)

// C Data Types (used in SQLBindCol, SQLGetData)
const (
	SQL_C_CHAR      = 1
	SQL_C_LONG      = 4
	SQL_C_SHORT     = 5
	SQL_C_FLOAT     = 7
	SQL_C_DOUBLE    = 8
	SQL_C_NUMERIC   = 2
	SQL_C_DEFAULT   = 99
	SQL_C_DATE      = 9
	SQL_C_TIME      = 10
	SQL_C_TIMESTAMP = 11
	SQL_C_BINARY    = -2
	SQL_C_BIT       = -7
	SQL_C_SBIGINT   = -25
	SQL_C_UBIGINT   = -27
	SQL_C_TINYINT   = -6
	SQL_C_SLONG     = -16
	SQL_C_SSHORT    = -15
	SQL_C_STINYINT  = -26
	SQL_C_ULONG     = -18
	SQL_C_USHORT    = -17
	SQL_C_UTINYINT  = -28
	SQL_C_WCHAR     = -8
)

// SQL Data Types (used in column descriptions)
const (
	SQL_CHAR           = 1
	SQL_NUMERIC        = 2
	SQL_DECIMAL        = 3
	SQL_INTEGER        = 4
	SQL_SMALLINT       = 5
	SQL_FLOAT          = 6
	SQL_REAL           = 7
	SQL_DOUBLE         = 8
	SQL_VARCHAR        = 12
	SQL_DATE           = 9
	SQL_TIME           = 10
	SQL_TIMESTAMP      = 11
	SQL_LONGVARCHAR    = -1
	SQL_BINARY         = -2
	SQL_VARBINARY      = -3
	SQL_LONGVARBINARY  = -4
	SQL_BIGINT         = -5
	SQL_TINYINT        = -6
	SQL_BIT            = -7
	SQL_WCHAR          = -8
	SQL_WVARCHAR       = -9
	SQL_WLONGVARCHAR   = -10
	SQL_GUID           = -11
	SQL_TYPE_DATE      = 91
	SQL_TYPE_TIME      = 92
	SQL_TYPE_TIMESTAMP = 93
	SQL_BLOB           = -98
	SQL_CLOB           = -99
	SQL_DBCLOB         = -350
	SQL_XML            = -370
	SQL_DECFLOAT       = -360
	SQL_BOOLEAN        = 16
)

// Nullable Information
const (
	SQL_NO_NULLS         = 0
	SQL_NULLABLE         = 1
	SQL_NULLABLE_UNKNOWN = 2
)

// Fetch Orientation
const (
	SQL_FETCH_NEXT     = 1
	SQL_FETCH_FIRST    = 2
	SQL_FETCH_LAST     = 3
	SQL_FETCH_PRIOR    = 4
	SQL_FETCH_ABSOLUTE = 5
	SQL_FETCH_RELATIVE = 6
)

// SQLGetInfo Information Types
const (
	SQL_DBMS_NAME            = 17
	SQL_DBMS_VER             = 18
	SQL_DATABASE_NAME        = 16
	SQL_DRIVER_NAME          = 6
	SQL_DRIVER_VER           = 7
	SQL_SERVER_NAME          = 13
	SQL_MAX_CATALOG_NAME_LEN = 34
	SQL_MAX_COLUMN_NAME_LEN  = 30
	SQL_MAX_CURSOR_NAME_LEN  = 31
	SQL_MAX_SCHEMA_NAME_LEN  = 32
	SQL_MAX_TABLE_NAME_LEN   = 35
	SQL_TXN_CAPABLE          = 46
	SQL_GETDATA_EXTENSIONS   = 81
)

// Diagnostic Fields
const (
	SQL_DIAG_RETURNCODE       = 1
	SQL_DIAG_NUMBER           = 2
	SQL_DIAG_ROW_COUNT        = 3
	SQL_DIAG_SQLSTATE         = 4
	SQL_DIAG_NATIVE           = 5
	SQL_DIAG_MESSAGE_TEXT     = 6
	SQL_DIAG_DYNAMIC_FUNCTION = 7
	SQL_DIAG_CLASS_ORIGIN     = 8
	SQL_DIAG_SUBCLASS_ORIGIN  = 9
	SQL_DIAG_CONNECTION_NAME  = 10
	SQL_DIAG_SERVER_NAME      = 11
)

// Column Description Fields
const (
	SQL_DESC_COUNT                  = 1001
	SQL_DESC_TYPE                   = 1002
	SQL_DESC_LENGTH                 = 1003
	SQL_DESC_OCTET_LENGTH_PTR       = 1004
	SQL_DESC_PRECISION              = 1005
	SQL_DESC_SCALE                  = 1006
	SQL_DESC_DATETIME_INTERVAL_CODE = 1007
	SQL_DESC_NULLABLE               = 1008
	SQL_DESC_INDICATOR_PTR          = 1009
	SQL_DESC_DATA_PTR               = 1010
	SQL_DESC_NAME                   = 1011
	SQL_DESC_UNNAMED                = 1012
	SQL_DESC_OCTET_LENGTH           = 1013
	SQL_DESC_ALLOC_TYPE             = 1099
)

// Cursor Concurrency
const (
	SQL_CONCUR_READ_ONLY = 1
	SQL_CONCUR_LOCK      = 2
	SQL_CONCUR_ROWVER    = 3
	SQL_CONCUR_VALUES    = 4
)

// Cursor Types
const (
	SQL_CURSOR_FORWARD_ONLY  = 0
	SQL_CURSOR_KEYSET_DRIVEN = 1
	SQL_CURSOR_DYNAMIC       = 2
	SQL_CURSOR_STATIC        = 3
)

// Free Statement Options
const (
	SQL_CLOSE        = 0
	SQL_DROP         = 1
	SQL_UNBIND       = 2
	SQL_RESET_PARAMS = 3
)

// Parameter Types
const (
	SQL_PARAM_INPUT        = 1
	SQL_PARAM_INPUT_OUTPUT = 2
	SQL_PARAM_OUTPUT       = 4
)
