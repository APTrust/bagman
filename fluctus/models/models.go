// Package fluctus provides data structures for exchanging data with
// fluctus, which is APTrust's API for accessing a Fedora repository.
package models

import (
    "strings"
    "time"
	"encoding/json"
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
	Identifier         string     `json:"identifier"`
    EventType          string     `json:"type"`
    DateTime           time.Time  `json:"date_time"`
    Detail             string     `json:"detail"`
    Outcome            string     `json:"outcome"`
    OutcomeDetail      string     `json:"outcome_detail"`
    Object             string     `json:"object"`
    Agent              string     `json:"agent"`
    OutcomeInformation string     `json:"outcome_information"`
}

// // Serialize the subset of PremisEvent data that fluctus
// // will accept. We're serializing everything but the id.
// func (event *PremisEvent) SerializeForFluctus() ([]byte, error) {
//     return json.Marshal(map[string]interface{}{
// 		"event_type": event.EventType,
// 		"date_time": event.DateTime,
// 		"detail": event.Detail,
// 		"outcome": event.Outcome,
// 		"outcome_detail": event.OutcomeDetail,
// 		"object": event.Object,
// 		"agent": event.Agent,
// 		"outcome_information": event.OutcomeInformation,
//     })
// }



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

Access indicate who can access the object. Valid values are
consortial, institution and restricted.
*/
type IntellectualObject struct {
    Id                 string         `json:"id"`
    InstitutionId      string         `json:"institution_id"`
    Title              string         `json:"title"`
    Description        string         `json:"description"`
    Access             string         `json:"access"`
	GenericFiles       []*GenericFile `json:"generic_files"`
	Events             []*PremisEvent `json:"events"`
}

// AccessValid returns true or false to indicate whether the
// structure's Access property contains a valid value.
func (obj *IntellectualObject) AccessValid() bool {
    lcAccess := strings.ToLower(obj.Access)
    for _, value := range rights {
        if value == lcAccess {
            return true
        }
    }
    return false
}

// Serialize the subset of IntellectualObject data that fluctus
// will accept.
func (obj *IntellectualObject) SerializeForFluctus() ([]byte, error) {
	// TODO: Why does fluctus require both pid and identifier?
    return json.Marshal(map[string]interface{}{
		"pid": obj.Id,
		"identifier": obj.Id,
		"title": obj.Title,
		"institution_id": obj.InstitutionId,
		"description": obj.Description,
		"access": obj.Access,
    })
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
    Algorithm          string     `json:"algorithm"`
    DateTime           time.Time  `json:"datetime"`
    Digest             string     `json:"digest"`
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
	Id                 string     `json:"id"`
	Identifier         string     `json:"identifier"`
    Format             string     `json:"format"`
    URI                string     `json:"uri"`
    Size               int64      `json:"size"`
    Created            time.Time  `json:"created"`
    Modified           time.Time  `json:"modified"`
    ChecksumAttributes []*ChecksumAttribute  `json:"checksum_attributes"`
    Events             []*PremisEvent        `json:"premisEvents"`
}

// Serializes a version of GenericFile that Fluctus will accept as post/put input.
func (gf *GenericFile) SerializeForFluctus()([]byte, error) {
    return json.Marshal(map[string]interface{}{
		"identifier": gf.Identifier,
		"format": gf.Format,
		"uri": gf.URI,
		"size": gf.Size,
		"created": gf.Created,
		"modified": gf.Modified,
		"checksum_attributes": gf.ChecksumAttributes,
    })
}


// User struct is used for logging in to fluctus.
type User struct {
    Email              string  `json:"email"`
    Password           string  `json:"password,omitempty"`
    ApiKey             string  `json:"api_secret_key,omitempty"`
    AuthToken          string  `json:"authenticity_token,omitempty"`
}
