package bagman

import (
	"strings"
	"time"
)

/*
PremisEvent contains information about events that occur during
the processing of a file or intellectual object, such as the
verfication of checksums, generation of unique identifiers, etc.
We use this struct to exchange data in JSON format with the
fluctus API. Fluctus, in turn, is responsible for managing all of
this data in Fedora.

This structure has the following fields:

EventType is the type of Premis event we want to register: ingest,
validation, fixity_generation, fixity_check or identifier_assignment.

DateTime is when this event occurred in our system.

Detail is a brief description of the event.

Outcome is either success or failure

Outcome detail is the checksum for checksum generation, the id for
id generation.

Object is a description of the object that generated the checksum
or id.

Agent is a URL describing where to find more info about Object.

OutcomeInformation contains the text of an error message, if
Outcome was failure.
*/
type PremisEvent struct {
	Identifier         string    `json:"identifier"`
	EventType          string    `json:"type"`
	DateTime           time.Time `json:"date_time"`
	Detail             string    `json:"detail"`
	Outcome            string    `json:"outcome"`
	OutcomeDetail      string    `json:"outcome_detail"`
	Object             string    `json:"object"`
	Agent              string    `json:"agent"`
	OutcomeInformation string    `json:"outcome_information"`
}

// EventTypeValid returns true/false, indicating whether the
// structure's EventType property contains the name of a
// valid premis event.
func (premisEvent *PremisEvent) EventTypeValid() bool {
	lcEventType := strings.ToLower(premisEvent.EventType)
	for _, value := range EventTypes {
		if value == lcEventType {
			return true
		}
	}
	return false
}
