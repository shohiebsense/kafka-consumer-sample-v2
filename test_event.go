package main

import "encoding/json"


type TestEvent struct {
	Message string `json:"message"`
	From   string `json:"from"`
}

func NewTestEvent(message, from string) *TestEvent {
	return &TestEvent{
		Message: message,
		From:   from,
	}
}

func EncodeTestEvent(event *TestEvent) ([]byte, error) {
	return json.Marshal(event)
}

func DecodeTestEvent(data []byte) (*TestEvent, error) {
	var event TestEvent
	err := json.Unmarshal(data, &event)
	return &event, err
}