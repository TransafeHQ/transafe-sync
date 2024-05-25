package events

import "time"

const SchemaVersion int = 1

type Event struct {
	timestamp  time.Time
	event_type string
	version    int
	payload    map[string]string
}

func createSyncStartEvent(payload map[string]string) *Event {
	event := Event{
		timestamp:  time.Now().UTC(),
		event_type: "sync_start",
		version:    SchemaVersion,
		payload:    payload,
	}
	return &event
}

func createSyncCompleteEvent(payload map[string]string) *Event {
	event := Event{
		timestamp:  time.Now().UTC(),
		event_type: "sync_complete",
		version:    SchemaVersion,
		payload:    payload,
	}
	return &event
}
