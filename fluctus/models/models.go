// Package fluctus provides data structures for exchanging data with
// fluctus, which is APTrust's API for accessing a Fedora repository.
package models

import (
	"strings"
)

// List of valid APTrust IntellectualObject rights.
var rights []string = []string {
	"consortial",
	"institutional",
	"restricted"}

// List of valid Premis Event types.
var eventTypes []string = []string {
	"ingest",
	"validation",
	"fixity_generation",
	"fixity_check",
	"identifier_assignment",
	"quarentine",
	"delete_action",
}

// EventTypeValid returns true/false, indicating whether the
// structure's EventType property contains the name of a
// valid premis event.
func (premisEvent *PremisEvent) EventTypeValid() bool {
	lcEventType := strings.ToLower(premisEvent.EventType)
	for _, value := range eventTypes {
		if value == lcEventType {
			return true
		}
	}
	return false
}

// RightsValid returns true or false to indicate whether the
// structure's Rights property contains a valid value.
func (intellectualObject *IntellectualObject) RightsValid() bool {
	lcRights := strings.ToLower(intellectualObject.Rights)
	for _, value := range rights {
		if value == lcRights {
			return true
		}
	}
	return false
}

/*
PremisEvent contains information about events that occur during
the processing of a file or intellectual objec, such as the
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
	EventType          string  `json:"type"`
	DateTime           string  `json:"date_time"`
	Detail             string  `json:"detail"`
	Outcome            string  `json:"outcome"`
	OutcomeDetail      string  `json:"outcome_detail"`
	Object             string  `json:"object"`
	Agent              string  `json:"agent"`
	OutcomeInformation string  `json:"outcome_information"`
}

/*
Institution represents an institution in fluctus. Name is the
institution's full name. BriefName is a shortened name.
*/
type Institution struct {
	Name               string  `json:"name"`
	BriefName          string  `json:"brief_name"`
}


/*
IntellectualObject belongs to an Institution and consists of
one or more GemericFiles.

Institution is the owner of the intellectual object.

Title is the title.

Description is a free-text description of the object.

Identifier is the object's unique identifier. (Whose assigned
this id? APTrust or the owner?)

Rights indicate who can acess the object. Valid values are
consortial, institution and restricted.
*/
type IntellectualObject struct {
	Institution        *Institution  `json:"institution"`
	Title              string  `json:"title"`
	Description        string  `json:"description"`
	Identifier         string  `json:"identifier"`
	Rights             string  `json:"rights"`
}

/*
ChecksumAttribute contains information about a checksum that
can be used to validate the integrity of a GenericFile.

DateTime should be in ISO8601 format for local time or UTC.
For example:

1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type ChecksumAttribute struct {
	Algorithm          string  `json:"algorithm"`
	DateTime           string  `json:"datetime"`
	Digest             string  `json:"digest"`
}

/*
GenericFile contains information about a file that makes up
part (or all) of an IntellectualObject.

IntellectualObject is the object to which the file belongs.

Format is typically a mime-type, such as "application/xml",
that describes the file format.

URI describes the location of the object (in APTrust?).

Size is the size of the object, in bytes.

Created is the date and time at which the object was created
(in APTrust, or at the institution that owns it?).

Modified is the data and time at which the object was last
modified (in APTrust, or at the institution that owns it?).

Created and Modified should be ISO8601 DateTime strings,
such as:

1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type GenericFile struct {
	IntellectualObject IntellectualObject `json:"intellectual_object"`
	Format             string  `json:"format"`
	URI                string  `json:"uri"`
	Size               int64   `json:"size"`
	Created            string  `json:"created"`
	Modified           string  `json:"modified"`
	ChecksumAttributes []ChecksumAttribute  `json:"checksum_attributes"`
}


// User struct is used for logging in to fluctus.
type User struct {
	Email              string  `json:"email"`
	Password           string  `json:"password,omitempty"`
	ApiKey             string  `json:"api_secret_key,omitempty"`
}
