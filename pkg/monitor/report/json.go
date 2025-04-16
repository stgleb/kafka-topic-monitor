package report

import (
	"encoding/json"
	"fmt"
)

type Json struct{}

func NewJson() *Json {
	return &Json{}
}

func (r *Json) Report(topicActivityInfos []*TopicActivityInfo) ([]byte, error) {
	data, err := json.MarshalIndent(topicActivityInfos, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}
	return data, nil
}
