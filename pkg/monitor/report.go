package monitor

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"
)

type CsvReporter struct{}

var (
	_ Reporter = &CsvReporter{}
)

func NewCsvReporter() *CsvReporter {
	return &CsvReporter{}
}

func (r *CsvReporter) Report(topicActivityInfos []*TopicActivityInfo) ([]byte, error) {
	// Create a buffer to write to
	var buf bytes.Buffer

	// Create a CSV writer
	csvWriter := csv.NewWriter(&buf)

	// Write the header row
	header := []string{"LastWriteTime", "LastReadTime", "PartitionNumber"}
	if err := csvWriter.Write(header); err != nil {
		return nil, fmt.Errorf("error writing CSV header: %w", err)
	}

	// Use RFC3339 format for timestamps (ISO 8601)
	timeFormat := time.RFC3339

	// Write the data rows
	for _, activity := range topicActivityInfos {
		row := []string{
			activity.LastWriteTime.Format(timeFormat),
			activity.LastReadTime.Format(timeFormat),
			strconv.Itoa(activity.PartitionNumber),
		}

		if err := csvWriter.Write(row); err != nil {
			return nil, fmt.Errorf("error writing CSV row: %w", err)
		}
	}

	// Flush the writer to ensure all data is written to the buffer
	csvWriter.Flush()

	// Check for any errors that occurred during flushing
	if err := csvWriter.Error(); err != nil {
		return nil, fmt.Errorf("error flushing CSV writer: %w", err)
	}

	// Return the buffer's bytes
	return buf.Bytes(), nil
}
