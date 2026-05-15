// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CSN represents a DB2 Commit Sequence Number — the binary log position stored
// in the IBMSNAP_COMMITSEQ column (type CHAR(10) FOR BIT DATA) of every change
// table. The 10-byte big-endian value is held internally as a uint64 for
// efficient comparison and arithmetic; the upper 2 bytes are unused in practice.
//
// CSN values are monotonically increasing: a higher value means a later
// committed transaction. Events with the same CSN belong to the same DB2
// transaction and are ordered by IBMSNAP_INTENTSEQ.
//
// String representation: "CSN:<16-digit-hex>", e.g. "CSN:000000000000C350".
// When embedding in a SQL WHERE clause use the 20-hex-char literal form
// X'%020X' to match the CHAR(10) column without a type cast.
//
// The null CSN (returned by NullCSN) marks snapshot events that have no
// associated log position.
type CSN struct {
	value    uint64
	isNull   bool
	rawBytes []byte // Original bytes from DB2
}

// NewCSN creates a CSN from a uint64 value
func NewCSN(value uint64) CSN {
	return CSN{
		value:  value,
		isNull: false,
	}
}

// NullCSN returns an unset CSN used to mark snapshot events that were read
// outside the CDC log stream and therefore have no commit sequence position.
func NullCSN() CSN {
	return CSN{isNull: true}
}

// NewCSNFromBytes creates a CSN from raw binary bytes (big-endian).
// For values received from the DB2 CLI driver, use NewCSNFromDBValue instead,
// since the driver returns CHAR(n) FOR BIT DATA as ASCII hex strings.
func NewCSNFromBytes(data []byte) CSN {
	if len(data) == 0 {
		return CSN{isNull: true}
	}

	// Convert bytes to uint64 (big-endian)
	var value uint64
	for i := 0; i < len(data) && i < 8; i++ {
		value = (value << 8) | uint64(data[i])
	}

	return CSN{
		value:    value,
		isNull:   false,
		rawBytes: data,
	}
}

// NewCSNFromDBValue creates a CSN from a value as returned by the DB2 CLI driver.
//
// The DB2 CLI library (SQL_C_CHAR) returns CHAR(n) FOR BIT DATA columns as
// uppercase ASCII hex strings — e.g. a 10-byte LSN yields a 20-character
// string like "0000000000001714AB00". Scanning that into []byte gives the
// ASCII bytes of the hex string, not raw binary. This function hex-decodes the
// value before constructing the CSN. If the bytes are not valid hex (e.g. raw
// bytes from internal construction), they are interpreted as raw big-endian.
func NewCSNFromDBValue(data []byte) CSN {
	if len(data) == 0 {
		return CSN{isNull: true}
	}
	// Try hex decoding (DB2 CLI SQL_C_CHAR path).
	if len(data)%2 == 0 {
		isHex := true
		for _, b := range data {
			if (b < '0' || b > '9') && (b < 'A' || b > 'F') && (b < 'a' || b > 'f') {
				isHex = false
				break
			}
		}
		if isHex {
			if decoded, err := hex.DecodeString(string(data)); err == nil {
				return NewCSNFromBytes(decoded)
			}
		}
	}
	return NewCSNFromBytes(data)
}

// NewCSNFromHex creates a CSN from a hex string (e.g., "0000000000001A2B")
func NewCSNFromHex(hexStr string) (CSN, error) {
	// Remove any "0x" prefix
	hexStr = strings.TrimPrefix(hexStr, "0x")
	hexStr = strings.TrimPrefix(hexStr, "0X")

	// Decode hex to bytes
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return CSN{}, fmt.Errorf("invalid hex CSN: %w", err)
	}

	return NewCSNFromBytes(data), nil
}

// ParseCSN parses a CSN from a string (handles multiple formats)
// Formats: "CSN:<hex>", "<decimal>", "0x<hex>"
func ParseCSN(s string) (CSN, error) {
	if s == "" {
		return CSN{isNull: true}, nil
	}

	// Handle "CSN:" prefix
	if hexStr, ok := strings.CutPrefix(s, "CSN:"); ok {
		return NewCSNFromHex(hexStr)
	}

	// Handle hex with "0x" prefix
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return NewCSNFromHex(s)
	}

	// Try parsing as decimal
	value, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return CSN{}, fmt.Errorf("invalid CSN format: %s", s)
	}

	return NewCSN(value), nil
}

// String returns the string representation of the CSN.
//
// When rawBytes is present (CSN was read directly from DB2), all bytes are
// encoded so that ParseCSN(c.String()).Equal(c) holds for 16-byte DB2 12.1 CSNs.
// The %016X form truncates to 8 bytes and would lose bytes 8-15 of a 16-byte LSN.
func (c CSN) String() string {
	if c.isNull {
		return ""
	}
	if len(c.rawBytes) > 0 {
		return "CSN:" + strings.ToUpper(hex.EncodeToString(c.rawBytes))
	}
	return fmt.Sprintf("CSN:%016X", c.value)
}

// Uint64 returns the numeric value of the CSN
func (c CSN) Uint64() uint64 {
	return c.value
}

// IsNull returns true if the CSN is null/unset
func (c CSN) IsNull() bool {
	return c.isNull
}

// Compare compares two CSNs.
// Returns: -1 if c < other, 0 if c == other, 1 if c > other
//
// When both CSNs have rawBytes (i.e. were read directly from DB2), a full
// byte-by-byte comparison is used. This is required for DB2 12.1 which uses
// 16-byte LSNs: nearby transactions on the same log page share the same
// first-8-byte uint64 value but differ only in the trailing bytes. Comparing only
// the uint64 (bytes 0–7) would treat such CSNs as equal, preventing the streamer
// from advancing past the initial SYNCHPOINT.
//
// Shorter rawBytes are left-aligned (trailing positions treated as zero), matching
// DB2 ≤11.x's CHAR(10) layout where bytes 8–9 are always zero.
func (c CSN) Compare(other CSN) int {
	if c.isNull && other.isNull {
		return 0
	}
	if c.isNull {
		return -1
	}
	if other.isNull {
		return 1
	}

	// Full raw-bytes comparison when both CSNs were read from DB2.
	// DB2 ≤11.x stores the uint64 in bytes 0-7 with trailing zero bytes (8-9).
	// Left-align shorter rawBytes: the missing trailing bytes are treated as zero,
	// which is correct because DB2 trailing bytes are always zero for 10-byte CSNs.
	if len(c.rawBytes) > 0 && len(other.rawBytes) > 0 {
		n := max(len(c.rawBytes), len(other.rawBytes))
		for i := 0; i < n; i++ {
			var cb, ob byte
			if i < len(c.rawBytes) {
				cb = c.rawBytes[i]
			}
			if i < len(other.rawBytes) {
				ob = other.rawBytes[i]
			}
			if cb < ob {
				return -1
			}
			if cb > ob {
				return 1
			}
		}
		return 0
	}

	// Fall back to uint64 for synthetic CSNs (checkpoints, unit tests).
	if c.value < other.value {
		return -1
	}
	if c.value > other.value {
		return 1
	}
	return 0
}

// Less returns true if c < other
func (c CSN) Less(other CSN) bool {
	return c.Compare(other) < 0
}

// Equal returns true if c == other
func (c CSN) Equal(other CSN) bool {
	return c.Compare(other) == 0
}

// Greater returns true if c > other
func (c CSN) Greater(other CSN) bool {
	return c.Compare(other) > 0
}

// Next returns the next CSN (increment by 1)
func (c CSN) Next() CSN {
	if c.isNull {
		return NewCSN(1)
	}
	return NewCSN(c.value + 1)
}

// SQLHex returns an uppercase hex string for embedding in X'...' SQL literals.
//
// lsnByteLen is the byte length of the target CHAR(n) FOR BIT DATA column
// (10 for DB2 ≤ 11.x, 16 for DB2 12.1+).
//
// DB2 ≤11.x stores the CSN as a uint64 in bytes 0–7 (big-endian) followed by
// 2 trailing zero bytes (bytes 8–9) in the CHAR(10) column. rawBytes is
// left-aligned in buf: copy to buf[0:], trailing bytes remain zero.
//
// When only the uint64 value is available (e.g. restored from checkpoint),
// the value is written big-endian into bytes 0–7 of buf; trailing bytes are zero.
func (c CSN) SQLHex(lsnByteLen int) string {
	if lsnByteLen <= 0 {
		lsnByteLen = 10
	}
	buf := make([]byte, lsnByteLen)
	if len(c.rawBytes) > 0 {
		// Left-align: rawBytes at buf[0:], trailing positions stay zero.
		n := len(c.rawBytes)
		if n > lsnByteLen {
			n = lsnByteLen
		}
		copy(buf, c.rawBytes[:n])
	} else {
		// uint64 big-endian in bytes 0-7; bytes 8+ are zero padding.
		limit := min(lsnByteLen, 8)
		v := c.value
		for i := limit - 1; i >= 0; i-- {
			buf[i] = byte(v)
			v >>= 8
		}
	}
	return strings.ToUpper(hex.EncodeToString(buf))
}

// OpType represents a CDC operation type.
type OpType string

// OpType values for CDC operations.
const (
	OpTypeRead   OpType = "read"   // Snapshot read (initial data load)
	OpTypeInsert OpType = "insert" // INSERT operation
	OpTypeDelete OpType = "delete" // DELETE operation

	// OpTypeUpdate is emitted by pairOpcodeEvents when a LEAD/LAG window query
	// detects a D+I pair sharing the same IBMSNAP_COMMITSEQ. BeforeData holds
	// the pre-update row and Data holds the post-update row.
	OpTypeUpdate OpType = "update"
)

// opTypeUpdateBefore and opTypeUpdateAfter are internal-only intermediate
// values emitted by the LEAD/LAG SQL query. They are never exported as part
// of a ChangeEvent; pairOpcodeEvents merges them into OpTypeUpdate.
const (
	opTypeUpdateBefore OpType = "_update_before" // IBMSNAP_OPCODE=3
	opTypeUpdateAfter  OpType = "_update_after"  // IBMSNAP_OPCODE=4
)

// OpTypeHeartbeat is emitted periodically when no CDC changes are available.
// Used to keep downstream consumers alive on low-traffic tables.
const OpTypeHeartbeat OpType = "heartbeat"

// OpTypeSchemaChange is emitted when a new table is added to SQL Replication
// (i.e., when ASNCDC.ADDTABLE is called while the connector is running).
const OpTypeSchemaChange OpType = "schema_change"

// FromDB2Op converts a DB2 IBMSNAP_OPERATION code to an OpType.
//
// DB2 LUW SQL Replication uses three operation codes:
//   - 'I' (INSERT): a new row was inserted.
//   - 'D' (DELETE): an existing row was deleted.
//   - 'U' (UPDATE): an existing row was modified. Note that UPDATE events are
//     NOT encoded as 'B' (before-image) + 'U' pairs — that encoding is used by
//     IBM Q-Replication only. In LUW SQL Replication each UPDATE produces a 'D'
//     record (the before-image) followed immediately by an 'I' record (the
//     after-image) with the same IBMSNAP_COMMITSEQ value.
//
// Returns an error for empty strings or unrecognised codes.
func FromDB2Op(dbOp string) (OpType, error) {
	if len(dbOp) == 0 {
		return "", errors.New("empty operation type")
	}

	switch dbOp[0] {
	case 'I', 'i':
		return OpTypeInsert, nil
	case 'U', 'u':
		return OpTypeUpdate, nil
	case 'D', 'd':
		return OpTypeDelete, nil
	default:
		return "", fmt.Errorf("unknown DB2 operation: %s", dbOp)
	}
}

// fromOpcodeInt maps an IBMSNAP_OPCODE integer (from the LEAD/LAG subquery)
// to an OpType. Returns an error for unknown values.
func fromOpcodeInt(code int64) (OpType, error) {
	switch code {
	case 1:
		return OpTypeDelete, nil
	case 2:
		return OpTypeInsert, nil
	case 3:
		return opTypeUpdateBefore, nil
	case 4:
		return opTypeUpdateAfter, nil
	default:
		return "", fmt.Errorf("unknown IBMSNAP_OPCODE %d", code)
	}
}

// Version represents a DB2 version
type Version struct {
	Major int
	Minor int
	Mod   int
	Fix   int
	Raw   string
}

// ParseVersion parses a DB2 version string.
// Examples: "SQL11050", "DB2 v11.5.0.0", "11.5"
// All known DB2 versions have Major >= 9; a zero Major indicates a parse anomaly
// and produces an "unknown version" error from SupportsCDC callers.
func ParseVersion(versionStr string) (Version, error) {
	v := Version{Raw: versionStr}

	// Handle "SQL11050" format
	if rest, ok := strings.CutPrefix(versionStr, "SQL"); ok {
		versionStr = rest
		if len(versionStr) >= 5 {
			major, _ := strconv.Atoi(versionStr[0:2])
			minor, _ := strconv.Atoi(versionStr[2:4])
			mod, _ := strconv.Atoi(versionStr[4:5])
			v.Major = major
			v.Minor = minor
			v.Mod = mod
			return v, nil
		}
	}

	// Handle "DB2 v11.5.0.0" format
	versionStr = strings.TrimPrefix(versionStr, "DB2 ")
	versionStr = strings.TrimPrefix(versionStr, "v")

	// Parse dotted version "11.5.0.0"
	parts := strings.Split(versionStr, ".")
	if len(parts) >= 1 {
		v.Major, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	}
	if len(parts) >= 2 {
		v.Minor, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
	}
	if len(parts) >= 3 {
		v.Mod, _ = strconv.Atoi(strings.TrimSpace(parts[2]))
	}
	if len(parts) >= 4 {
		v.Fix, _ = strconv.Atoi(strings.TrimSpace(parts[3]))
	}

	if v.Major == 0 {
		return v, fmt.Errorf("parsing DB2 version: %s", versionStr)
	}

	return v, nil
}

// String returns a string representation of the version
func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.Major, v.Minor, v.Mod, v.Fix)
}

// Compare compares two versions
// Returns: -1 if v < other, 0 if v == other, 1 if v > other
func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}
	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}
	if v.Mod != other.Mod {
		if v.Mod < other.Mod {
			return -1
		}
		return 1
	}
	if v.Fix != other.Fix {
		if v.Fix < other.Fix {
			return -1
		}
		return 1
	}
	return 0
}

// AtLeast returns true if v >= other
func (v Version) AtLeast(major, minor int) bool {
	if v.Major > major {
		return true
	}
	if v.Major == major && v.Minor >= minor {
		return true
	}
	return false
}

// SupportsCDC returns true if the version supports SQL Replication CDC
func (v Version) SupportsCDC() bool {
	// DB2 10.1+ has CDC support
	return v.AtLeast(10, 1)
}

// ChangeEvent is a single CDC event produced by the DB2 SQL Replication capture
// daemon and emitted as a Redpanda Connect message.
//
// For snapshot events (Operation == OpTypeRead) CSN is the null CSN and
// BeforeData is nil. For streaming events CSN carries the
// IBMSNAP_COMMITSEQ value; IntentSeq (IBMSNAP_INTENTSEQ) orders rows within
// the same transaction.
//
// For update events (Operation == OpTypeUpdate), pairOpcodeEvents merges the
// D+I pair detected by the LEAD/LAG window query: BeforeData holds the
// pre-update row and Data holds the post-update row.
type ChangeEvent struct {
	Schema     string         `json:"schema"`                // DB2 source schema (TABSCHEMA)
	Table      string         `json:"table"`                 // DB2 source table name
	Operation  OpType         `json:"operation"`             // insert / delete / read / update
	CSN        CSN            `json:"csn"`                   // log position; NullCSN for snapshot rows
	IntentSeq  int64          `json:"intent_seq"`            // IBMSNAP_INTENTSEQ, tie-breaks within a CSN
	Timestamp  time.Time      `json:"timestamp"`             // IBMSNAP_LOGMARKER from the change table
	Data       map[string]any `json:"data"`                  // row column values; for INSERT/UPDATE: new row; for DELETE: deleted row
	BeforeData map[string]any `json:"before_data,omitempty"` // pre-update row for OpTypeUpdate; nil for all other operations
}

// String returns a JSON representation of the event
func (e ChangeEvent) String() string {
	data, err := json.Marshal(e)
	if err != nil {
		// Fallback to basic representation if JSON marshaling fails
		return fmt.Sprintf(`{"schema":"%s","table":"%s","operation":"%s","csn":"%s","intent_seq":%d}`,
			e.Schema, e.Table, e.Operation, e.CSN, e.IntentSeq)
	}
	return string(data)
}

// MarshalJSON implements json.Marshaler for proper JSON serialization
func (e ChangeEvent) MarshalJSON() ([]byte, error) {
	type Alias ChangeEvent
	return json.Marshal(&struct {
		CSN       string `json:"csn"`
		Timestamp string `json:"timestamp,omitempty"`
		*Alias
	}{
		CSN:       e.CSN.String(),
		Timestamp: formatTimestamp(e.Timestamp),
		Alias:     (*Alias)(&e),
	})
}

// formatTimestamp formats a timestamp for JSON output
func formatTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
