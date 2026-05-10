// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSN(t *testing.T) {
	t.Run("create from uint64", func(t *testing.T) {
		csn := NewCSN(12345)
		assert.False(t, csn.IsNull())
		assert.Equal(t, uint64(12345), csn.Uint64())
		assert.Equal(t, "CSN:0000000000003039", csn.String())
	})

	t.Run("create from bytes", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39}
		csn := NewCSNFromBytes(data)
		assert.False(t, csn.IsNull())
		assert.Equal(t, uint64(12345), csn.Uint64())
	})

	t.Run("create from hex string", func(t *testing.T) {
		csn, err := NewCSNFromHex("0000000000003039")
		require.NoError(t, err)
		assert.Equal(t, uint64(12345), csn.Uint64())

		csn2, err := NewCSNFromHex("0x3039")
		require.NoError(t, err)
		assert.Equal(t, uint64(12345), csn2.Uint64())
	})

	t.Run("parse CSN", func(t *testing.T) {
		tests := []struct {
			input    string
			expected uint64
			wantErr  bool
		}{
			{"CSN:0000000000003039", 12345, false},
			{"0x3039", 12345, false},
			{"12345", 12345, false},
			{"", 0, false}, // Null CSN
			{"invalid", 0, true},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				csn, err := ParseCSN(tt.input)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					require.NoError(t, err)
					if tt.input == "" {
						assert.True(t, csn.IsNull())
					} else {
						assert.Equal(t, tt.expected, csn.Uint64())
					}
				}
			})
		}
	})

	t.Run("comparison", func(t *testing.T) {
		csn1 := NewCSN(100)
		csn2 := NewCSN(200)
		csn3 := NewCSN(100)

		assert.True(t, csn1.Less(csn2))
		assert.False(t, csn2.Less(csn1))
		assert.False(t, csn1.Less(csn3))

		assert.True(t, csn1.Equal(csn3))
		assert.False(t, csn1.Equal(csn2))

		assert.True(t, csn2.Greater(csn1))
		assert.False(t, csn1.Greater(csn2))

		assert.Equal(t, -1, csn1.Compare(csn2))
		assert.Equal(t, 1, csn2.Compare(csn1))
		assert.Equal(t, 0, csn1.Compare(csn3))
	})

	t.Run("null CSN", func(t *testing.T) {
		nullCSN := CSN{isNull: true}
		csn := NewCSN(100)

		assert.True(t, nullCSN.IsNull())
		assert.True(t, nullCSN.Less(csn))
		assert.False(t, csn.Less(nullCSN))
	})

	t.Run("next", func(t *testing.T) {
		csn := NewCSN(100)
		next := csn.Next()
		assert.Equal(t, uint64(101), next.Uint64())

		nullCSN := CSN{isNull: true}
		nextNull := nullCSN.Next()
		assert.Equal(t, uint64(1), nextNull.Uint64())
	})
}

func TestOpType(t *testing.T) {
	t.Run("from DB2 operation", func(t *testing.T) {
		tests := []struct {
			dbOp     string
			expected OpType
			wantErr  bool
		}{
			{"I", OpTypeInsert, false},
			{"i", OpTypeInsert, false},
			{"U", OpTypeUpdate, false},
			{"u", OpTypeUpdate, false},
			{"D", OpTypeDelete, false},
			{"d", OpTypeDelete, false},
			{"B", "", true}, // 'B' is Q-Replication only; LUW SQL Replication uses D+I pairs
			{"b", "", true},
			{"", "", true},
			{"X", "", true},
		}

		for _, tt := range tests {
			t.Run(tt.dbOp, func(t *testing.T) {
				op, err := FromDB2Op(tt.dbOp)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, op)
				}
			})
		}
	})
}

func TestVersion(t *testing.T) {
	t.Run("parse version", func(t *testing.T) {
		tests := []struct {
			input string
			major int
			minor int
			mod   int
			fix   int
		}{
			{"SQL11050", 11, 5, 0, 0},
			{"DB2 v11.5.0.0", 11, 5, 0, 0},
			{"v10.1.0.5", 10, 1, 0, 5},
			{"11.5", 11, 5, 0, 0},
			{"10.5.0", 10, 5, 0, 0},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				v, err := ParseVersion(tt.input)
				require.NoError(t, err)
				assert.Equal(t, tt.major, v.Major)
				assert.Equal(t, tt.minor, v.Minor)
				assert.Equal(t, tt.mod, v.Mod)
				assert.Equal(t, tt.fix, v.Fix)
			})
		}
	})

	t.Run("version comparison", func(t *testing.T) {
		v10_1 := Version{Major: 10, Minor: 1}
		v10_5 := Version{Major: 10, Minor: 5}
		v11_5 := Version{Major: 11, Minor: 5}
		v11_5_dup := Version{Major: 11, Minor: 5}

		assert.Equal(t, -1, v10_1.Compare(v10_5))
		assert.Equal(t, -1, v10_5.Compare(v11_5))
		assert.Equal(t, 0, v11_5.Compare(v11_5_dup))
		assert.Equal(t, 1, v11_5.Compare(v10_1))
	})

	t.Run("version feature support", func(t *testing.T) {
		v9_7 := Version{Major: 9, Minor: 7}
		v10_1 := Version{Major: 10, Minor: 1}
		v11_5 := Version{Major: 11, Minor: 5}

		assert.False(t, v9_7.SupportsCDC())
		assert.True(t, v10_1.SupportsCDC())
		assert.True(t, v11_5.SupportsCDC())
	})

	t.Run("AtLeast", func(t *testing.T) {
		v11_5 := Version{Major: 11, Minor: 5}

		assert.True(t, v11_5.AtLeast(11, 5))
		assert.True(t, v11_5.AtLeast(11, 0))
		assert.True(t, v11_5.AtLeast(10, 5))
		assert.False(t, v11_5.AtLeast(11, 6))
		assert.False(t, v11_5.AtLeast(12, 0))
	})

	t.Run("string representation", func(t *testing.T) {
		v := Version{Major: 11, Minor: 5, Mod: 0, Fix: 1}
		assert.Equal(t, "11.5.0.1", v.String())
	})
}

func TestChangeEvent(t *testing.T) {
	t.Run("JSON serialization", func(t *testing.T) {
		timestamp := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)
		event := ChangeEvent{
			Schema:    "DB2ADMIN",
			Table:     "EMPLOYEES",
			Operation: OpTypeInsert,
			CSN:       NewCSN(12345),
			IntentSeq: 1,
			Timestamp: timestamp,
			Data: map[string]any{
				"id":     123,
				"name":   "John Doe",
				"salary": 75000.00,
			},
		}

		// Test String() method returns valid JSON
		str := event.String()
		assert.NotEmpty(t, str)

		// Verify it's valid JSON
		var decoded map[string]any
		err := json.Unmarshal([]byte(str), &decoded)
		require.NoError(t, err)

		// Check required fields
		assert.Equal(t, "DB2ADMIN", decoded["schema"])
		assert.Equal(t, "EMPLOYEES", decoded["table"])
		assert.Equal(t, "insert", decoded["operation"])
		assert.Equal(t, "CSN:0000000000003039", decoded["csn"])
		assert.Equal(t, float64(1), decoded["intent_seq"])
		assert.NotEmpty(t, decoded["timestamp"])
		assert.NotNil(t, decoded["data"])
	})

	t.Run("JSON serialization with before_data", func(t *testing.T) {
		event := ChangeEvent{
			Schema:    "DB2ADMIN",
			Table:     "EMPLOYEES",
			Operation: OpTypeUpdate,
			CSN:       NewCSN(12345),
			IntentSeq: 2,
			Data: map[string]any{
				"id":     123,
				"salary": 80000.00,
			},
			BeforeData: map[string]any{
				"id":     123,
				"salary": 75000.00,
			},
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded map[string]any
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		// Verify before_data is present
		assert.NotNil(t, decoded["before_data"])
		beforeData := decoded["before_data"].(map[string]any)
		assert.Equal(t, float64(75000.00), beforeData["salary"])
	})

	t.Run("JSON serialization without timestamp", func(t *testing.T) {
		event := ChangeEvent{
			Schema:    "DB2ADMIN",
			Table:     "EMPLOYEES",
			Operation: OpTypeRead,
			CSN:       NewCSN(0),
			Data:      map[string]any{"id": 1},
		}

		str := event.String()
		var decoded map[string]any
		err := json.Unmarshal([]byte(str), &decoded)
		require.NoError(t, err)

		// timestamp should be omitted or empty when zero
		timestamp, ok := decoded["timestamp"]
		if ok {
			assert.Empty(t, timestamp)
		}
	})
}

// TestCSNBinaryRoundTrip verifies that CSN values are formatted as 20-character
// zero-padded hex literals for use in SQL WHERE clauses against CHAR(10) FOR BIT
// DATA columns (IBMSNAP_COMMITSEQ). DB2 requires the X'...' literal to be
// exactly 20 hexadecimal characters (10 bytes) — a shorter literal causes
// SQLSTATE 22001 (string data, right truncation).
func TestCSNBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   uint64
		wantStr string // canonical "CSN:<16hex>" representation
		wantSQL string // 20-char SQL hex literal
	}{
		{
			name:    "zero",
			value:   0,
			wantStr: "CSN:0000000000000000",
			wantSQL: "00000000000000000000",
		},
		{
			name:    "one",
			value:   1,
			wantStr: "CSN:0000000000000001",
			wantSQL: "00000000000000000001",
		},
		{
			name:    "0xC350 = 50000",
			value:   50000,
			wantStr: "CSN:000000000000C350",
			wantSQL: "0000000000000000C350",
		},
		{
			name:    "0xFFFF = 65535",
			value:   65535,
			wantStr: "CSN:000000000000FFFF",
			wantSQL: "0000000000000000FFFF",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			csn := NewCSN(tc.value)

			// Canonical string format uses 16 hex chars.
			assert.Equal(t, tc.wantStr, csn.String())

			// SQL hex literal format must be exactly 20 chars for CHAR(10) FOR BIT DATA.
			sqlLiteral := fmt.Sprintf("%020X", csn.Uint64())
			assert.Len(t, sqlLiteral, 20,
				"SQL hex literal for CHAR(10) FOR BIT DATA must be exactly 20 chars")
			assert.Equal(t, tc.wantSQL, sqlLiteral)
		})
	}
}

// TestCSNCompareRawBytes verifies that Compare uses full rawBytes comparison
// for DB2-read CSNs. DB2 12.1 uses 16-byte LSNs; nearby transactions on the
// same log page share the same first-8-byte value (uint64) but differ only in
// bytes 8–15. A uint64-only comparison would return 0 (equal) for such pairs,
// preventing the streamer from advancing SYNCHPOINT.
func TestCSNCompareRawBytes(t *testing.T) {
	t.Parallel()

	// Same first 8 bytes (0x1754) but different trailing 8 bytes.
	// This matches real DB2 12.1 output: 0000000000001754 | 000000000005A227
	base := NewCSNFromBytes([]byte{0, 0, 0, 0, 0, 0, 0x17, 0x54, 0, 0, 0, 0, 0, 0x05, 0xA2, 0x27})
	next := NewCSNFromBytes([]byte{0, 0, 0, 0, 0, 0, 0x17, 0x54, 0, 0, 0, 0, 0, 0x05, 0xB0, 0x00})

	assert.Equal(t, uint64(0x1754), base.Uint64(), "both have same uint64")
	assert.Equal(t, uint64(0x1754), next.Uint64(), "both have same uint64")

	assert.True(t, next.Greater(base), "next must be greater than base via rawBytes comparison")
	assert.True(t, base.Less(next), "base must be less than next via rawBytes comparison")
	assert.False(t, base.Equal(next), "base and next must not be equal")

	// Equal rawBytes → Equal CSNs.
	same := NewCSNFromBytes([]byte{0, 0, 0, 0, 0, 0, 0x17, 0x54, 0, 0, 0, 0, 0, 0x05, 0xA2, 0x27})
	assert.True(t, base.Equal(same), "identical rawBytes must be equal")

	// uint64-only CSNs (no rawBytes) still compare by value.
	u1 := NewCSN(0x1754)
	u2 := NewCSN(0x1755)
	assert.True(t, u2.Greater(u1), "uint64-only comparison still works")
}

// TestCSNFromBytesPreservesAllBytes verifies that CHAR(10) FOR BIT DATA values
// from DB2 are decoded correctly to uint64. NewCSNFromBytes reads the first 8
// bytes in big-endian order (bytes 8 and 9 of the 10-byte CHAR(10) column are
// ignored because uint64 holds only 8 bytes). Both 8-byte and 10-byte encodings
// of the same value must decode to identical CSN values.
func TestCSNFromBytesPreservesAllBytes(t *testing.T) {
	t.Parallel()

	// DB2 CHAR(10) FOR BIT DATA: the first 8 bytes hold the uint64 value in
	// big-endian order. Bytes 8 and 9 are extra padding. Value 50000 = 0xC350
	// encodes to bytes: {0,0,0,0,0,0,0xC3,0x50} in the first 8 positions.
	tenByteCSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC3, 0x50, 0x00, 0x00}
	csn10 := NewCSNFromBytes(tenByteCSN)
	assert.False(t, csn10.IsNull())
	assert.Equal(t, uint64(50000), csn10.Uint64())

	// The canonical 8-byte representation must decode identically.
	eightByteCSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC3, 0x50}
	csn8 := NewCSNFromBytes(eightByteCSN)
	assert.Equal(t, csn10.Uint64(), csn8.Uint64(),
		"8-byte and 10-byte encodings of the same CSN must produce equal values")
	assert.True(t, csn10.Equal(csn8),
		"CSN.Equal must hold between 8-byte and 10-byte encodings of the same value")
}
