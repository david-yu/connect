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

// String returns the string representation of the CSN
func (c CSN) String() string {
	if c.isNull {
		return ""
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

// Compare compares two CSNs
// Returns: -1 if c < other, 0 if c == other, 1 if c > other
//
// When both CSNs have rawBytes (i.e. were read directly from DB2), a full
// byte-by-byte comparison is used. This is required for DB2 12.1 which uses
// 16-byte LSNs: nearby transactions on the same log page share the same
// first-8-byte value but differ only in the trailing bytes. Comparing only
// the uint64 (bytes 0–7) would treat such CSNs as equal and prevent the
// streamer from advancing past the initial SYNCHPOINT.
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
	if len(c.rawBytes) > 0 && len(other.rawBytes) > 0 {
		n := max(len(c.rawBytes), len(other.rawBytes))
		for i := range n {
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
// When rawBytes is available (CSN was read directly from DB2), the first
// lsnByteLen bytes are used verbatim. When only the uint64 value is available
// (e.g. restored from a checkpoint string), the value is written big-endian
// into the first 8 bytes and the remainder is zero-padded.
func (c CSN) SQLHex(lsnByteLen int) string {
	if lsnByteLen <= 0 {
		lsnByteLen = 10
	}
	buf := make([]byte, lsnByteLen)
	if len(c.rawBytes) > 0 {
		copy(buf, c.rawBytes) // zero-pads if rawBytes shorter than lsnByteLen
	} else {
		v := c.value
		limit := min(lsnByteLen, 8)
		for i := limit - 1; i >= 0; i-- {
			buf[i] = byte(v)
			v >>= 8
		}
	}
	return strings.ToUpper(hex.EncodeToString(buf))
}

// OpType represents a CDC operation type.
//
// This connector emits "read", "insert", and "delete" for all normal operation.
// DB2 LUW SQL Replication encodes an UPDATE as a DELETE+INSERT pair sharing the
// same IBMSNAP_COMMITSEQ, so "update" is defined but never emitted.
type OpType string

// OpType values for CDC operations.
const (
	OpTypeRead   OpType = "read"   // Snapshot read (initial data load)
	OpTypeInsert OpType = "insert" // INSERT operation
	OpTypeDelete OpType = "delete" // DELETE operation

	// OpTypeUpdate is defined for completeness but is never emitted by this connector.
	// DB2 LUW SQL Replication encodes every UPDATE as a D+I pair (DELETE of the old
	// row followed by INSERT of the new row, both sharing the same IBMSNAP_COMMITSEQ).
	// Consumers should correlate consecutive delete+insert events on the same CSN and
	// primary key to reconstruct the before/after image of an update.
	OpTypeUpdate OpType = "update"
)

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

// Version represents a DB2 version
type Version struct {
	Major int
	Minor int
	Mod   int
	Fix   int
	Raw   string
}

// ParseVersion parses a DB2 version string
// Examples: "SQL11050", "DB2 v11.5.0.0", "11.5"
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

// SupportsEventStore returns true if the version supports Event Store (newer CDC)
func (v Version) SupportsEventStore() bool {
	// DB2 11.5+ has Event Store
	return v.AtLeast(11, 5)
}

// ChangeEvent is a single CDC event produced by the DB2 SQL Replication capture
// daemon and emitted as a Redpanda Connect message.
//
// For snapshot events (Operation == OpTypeRead) CSN is the null CSN and
// BeforeData is always nil. For streaming events CSN carries the
// IBMSNAP_COMMITSEQ value; IntentSeq (IBMSNAP_INTENTSEQ) orders rows within
// the same transaction.
//
// Because DB2 LUW SQL Replication represents UPDATE as a D+I pair, callers
// will see two consecutive events for each logical update: a delete event
// (before-image) followed by an insert event (after-image) sharing the same CSN.
// BeforeData is reserved for a future pairing implementation and is always nil
// in the current version.
type ChangeEvent struct {
	Schema     string         `json:"schema"`                // DB2 source schema (TABSCHEMA)
	Table      string         `json:"table"`                 // DB2 source table name
	Operation  OpType         `json:"operation"`             // insert / delete / read (update never emitted)
	CSN        CSN            `json:"csn"`                   // log position; NullCSN for snapshot rows
	IntentSeq  int64          `json:"intent_seq"`            // IBMSNAP_INTENTSEQ, tie-breaks within a CSN
	Timestamp  time.Time      `json:"timestamp"`             // IBMSNAP_LOGMARKER from the change table
	Data       map[string]any `json:"data"`                  // after-image column values (always present)
	BeforeData map[string]any `json:"before_data,omitempty"` // reserved; always nil in current implementation
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
