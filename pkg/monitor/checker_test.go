package monitor

import (
	"testing"
	"time"
)

func TestParseOffsetMetadata(t *testing.T) {
	tests := []struct {
		name        string
		metadata    string
		wantTime    time.Time
		expectError bool
	}{
		{
			name:        "valid RFC3339 timestamp",
			metadata:    "2023-04-15T14:30:45Z",
			wantTime:    time.Date(2023, 4, 15, 14, 30, 45, 0, time.UTC),
			expectError: false,
		},
		{
			name:        "valid RFC3339 timestamp with nanoseconds",
			metadata:    "2023-04-15T14:30:45.123456789Z",
			wantTime:    time.Date(2023, 4, 15, 14, 30, 45, 123456789, time.UTC),
			expectError: false,
		},
		{
			name:        "valid RFC3339 timestamp with timezone",
			metadata:    "2023-04-15T14:30:45+02:00",
			wantTime:    time.Date(2023, 4, 15, 14, 30, 45, 0, time.FixedZone("", 2*60*60)),
			expectError: false,
		},
		{
			name:        "empty metadata",
			metadata:    "",
			wantTime:    time.Time{},
			expectError: true,
		},
		{
			name:        "invalid timestamp format",
			metadata:    "not a timestamp",
			wantTime:    time.Time{},
			expectError: true,
		},
		{
			name:        "numeric timestamp",
			metadata:    "1586934268123",
			wantTime:    time.Time{},
			expectError: true,
		},
		{
			name:        "partially valid timestamp",
			metadata:    "2023-04-15",
			wantTime:    time.Time{},
			expectError: true,
		},
		{
			name:        "timestamp with prefix",
			metadata:    "ts:2023-04-15T14:30:45Z",
			wantTime:    time.Time{},
			expectError: true,
		},
		{
			name:        "JSON metadata",
			metadata:    `{"timestamp":"2023-04-15T14:30:45Z","version":1}`,
			wantTime:    time.Time{},
			expectError: true,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTime, err := parseOffsetMetadata(tt.metadata)

			// Check error expectation
			if (err != nil) != tt.expectError {
				t.Errorf("parseOffsetMetadata() error = %v, expectError = %v", err, tt.expectError)
				return
			}

			// If we expect a valid result, check the time
			if !tt.expectError {
				if !gotTime.Equal(tt.wantTime) {
					t.Errorf("parseOffsetMetadata() = %v, want %v", gotTime, tt.wantTime)
				}
			}
		})
	}
}
