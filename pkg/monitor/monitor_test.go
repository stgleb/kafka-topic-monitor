package monitor

import (
	"testing"
	"time"
)

func TestIsActive(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name           string
		lastWriteTime  time.Time
		lastReadTime   time.Time
		inactivityDays int
		expected       bool
	}{
		{
			name:           "both timestamps zero",
			lastWriteTime:  time.Time{},
			lastReadTime:   time.Time{},
			inactivityDays: 7,
			expected:       false,
		},
		{
			name:           "both timestamps recent",
			lastWriteTime:  now.Add(-24 * time.Hour),
			lastReadTime:   now.Add(-48 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "write recent, read old",
			lastWriteTime:  now.Add(-24 * time.Hour),
			lastReadTime:   now.Add(-10 * 24 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "write old, read recent",
			lastWriteTime:  now.Add(-10 * 24 * time.Hour),
			lastReadTime:   now.Add(-24 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "both timestamps old",
			lastWriteTime:  now.Add(-10 * 24 * time.Hour),
			lastReadTime:   now.Add(-10 * 24 * time.Hour),
			inactivityDays: 7,
			expected:       false,
		},
		{
			name:           "at exact boundary",
			lastWriteTime:  now.Add(-7 * 24 * time.Hour),
			lastReadTime:   now.Add(-7 * 24 * time.Hour),
			inactivityDays: 7,
			expected:       false,
		},
		{
			name:           "just within boundary",
			lastWriteTime:  now.Add(-7*24*time.Hour + time.Minute),
			lastReadTime:   now.Add(-7*24*time.Hour + time.Minute),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "zero write, recent read",
			lastWriteTime:  time.Time{},
			lastReadTime:   now.Add(-24 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "recent write, zero read",
			lastWriteTime:  now.Add(-24 * time.Hour),
			lastReadTime:   time.Time{},
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "old write, zero read",
			lastWriteTime:  now.Add(-10 * 24 * time.Hour),
			lastReadTime:   time.Time{},
			inactivityDays: 7,
			expected:       false,
		},
		{
			name:           "zero write, old read",
			lastWriteTime:  time.Time{},
			lastReadTime:   now.Add(-10 * 24 * time.Hour),
			inactivityDays: 7,
			expected:       false,
		},
		{
			name:           "longer inactivity period",
			lastWriteTime:  now.Add(-14 * 24 * time.Hour),
			lastReadTime:   now.Add(-14 * 24 * time.Hour),
			inactivityDays: 30,
			expected:       true,
		},
		{
			name:           "zero inactivity period",
			lastWriteTime:  now.Add(-1 * time.Hour),
			lastReadTime:   now.Add(-1 * time.Hour),
			inactivityDays: 0,
			expected:       false,
		},
		{
			name:           "future timestamps",
			lastWriteTime:  now.Add(24 * time.Hour),
			lastReadTime:   now.Add(24 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
		{
			name:           "mixed activity timestamps",
			lastWriteTime:  now.Add(-10 * 24 * time.Hour),
			lastReadTime:   now.Add(-3 * 24 * time.Hour),
			inactivityDays: 7,
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isActive(tt.lastWriteTime, tt.lastReadTime, tt.inactivityDays)
			if got != tt.expected {
				t.Errorf("isActive() = %v, want %v for %s", got, tt.expected, tt.name)
				t.Errorf("  lastWriteTime: %v", tt.lastWriteTime)
				t.Errorf("  lastReadTime: %v", tt.lastReadTime)
				t.Errorf("  inactivityDays: %v", tt.inactivityDays)
			}
		})
	}
}
