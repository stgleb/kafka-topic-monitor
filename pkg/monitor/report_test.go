package monitor

import (
	"bytes"
	"encoding/csv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReporter_Report(t *testing.T) {
	// Create a reporter instance
	r := &CsvReporter{}

	// Define test data
	topicActivityInfos := []*TopicActivityInfo{
		{
			LastWriteTime:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
			LastReadTime:    time.Date(2023, 10, 1, 12, 5, 0, 0, time.UTC),
			PartitionNumber: 0,
		},
		{
			LastWriteTime:   time.Date(2023, 10, 1, 13, 0, 0, 0, time.UTC),
			LastReadTime:    time.Date(2023, 10, 1, 13, 5, 0, 0, time.UTC),
			PartitionNumber: 1,
		},
	}

	// Call the Report method
	result, err := r.Report(topicActivityInfos)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parse the result as CSV
	reader := csv.NewReader(bytes.NewReader(result))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse CSV: %v", err)
	}

	// Verify the header
	expectedHeader := []string{"LastWriteTime", "LastReadTime", "PartitionNumber"}
	if len(records) < 1 || !assert.Equal(t, records[0], expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, records[0])
	}

	// Verify the data rows
	expectedRows := [][]string{
		{"2023-10-01T12:00:00Z", "2023-10-01T12:05:00Z", "0"},
		{"2023-10-01T13:00:00Z", "2023-10-01T13:05:00Z", "1"},
	}
	for i, expectedRow := range expectedRows {
		if !assert.Equal(t, records[i+1], expectedRow) {
			t.Errorf("expected row %v, got %v", expectedRow, records[i+1])
		}
	}
}
