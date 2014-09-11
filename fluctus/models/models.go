// Package fluctus provides data structures for exchanging data with
// fluctus, which is APTrust's API for accessing a Fedora repository.
package models

import (
	"encoding/json"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"strings"
	"time"
)

// List of valid APTrust IntellectualObject AccessRights.
var AccessRights []string = []string{
	"consortia",
	"institution",
	"restricted"}

// List of valid Premis Event types.
var eventTypes []string = []string{
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

/*
Institution represents an institution in fluctus. Name is the
institution's full name. BriefName is a shortened name.
Identifier is the institution's domain name.
*/
type Institution struct {
	Pid        string `json:"pid"`
	Name       string `json:"name"`
	BriefName  string `json:"brief_name"`
	Identifier string `json:"identifier"`
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
	Id            string         `json:"id"`
	Identifier    string         `json:"identifier"`
	InstitutionId string         `json:"institution_id"`
	Title         string         `json:"title"`
	Description   string         `json:"description"`
	Access        string         `json:"access"`
	GenericFiles  []*GenericFile `json:"generic_files"`
	Events        []*PremisEvent `json:"events"`
}

// Returns the total number of bytes of all of the generic
// files in this object. The object's bag size will be slightly
// larger than this, because it will include a manifest, tag
// files and tar header.
func (obj *IntellectualObject) TotalFileSize() (int64) {
	total := int64(0)
	for _, gf := range obj.GenericFiles {
		total += gf.Size
	}
	return total
}

// AccessValid returns true or false to indicate whether the
// structure's Access property contains a valid value.
func (obj *IntellectualObject) AccessValid() bool {
	lcAccess := strings.ToLower(obj.Access)
	for _, value := range AccessRights {
		if value == lcAccess {
			return true
		}
	}
	return false
}

// SerializeForCreate serializes an intellectual object along
// with all of its generic files and events in a single shot.
// The output is a byte array of JSON data.
func (obj *IntellectualObject) SerializeForCreate() ([]byte, error) {
	genericFileMaps := make([]map[string]interface{}, len(obj.GenericFiles))
	for i, gf := range obj.GenericFiles {
		genericFileMaps[i] = map[string]interface{}{
			"identifier":   gf.Identifier,
			"file_format":  gf.Format,
			"uri":          gf.URI,
			"size":         gf.Size,
			"created":      gf.Created,
			"modified":     gf.Modified,
			"checksum":     gf.ChecksumAttributes,
			"premisEvents": gf.Events,
		}
	}
	events := make([]*PremisEvent, 2)
	ingestEvent, err := obj.CreateIngestEvent()
	if err != nil {
		return nil, err
	}
	idEvent, err := obj.CreateIdEvent()
	if err != nil {
		return nil, err
	}
	events[0] = idEvent
	events[1] = ingestEvent

	// Even though we're sending only one object,
	// Fluctus expects an array.
	objects := make([]map[string]interface{}, 1)
	objects[0] = map[string]interface{}{
		"identifier":     obj.Identifier,
		"title":          obj.Title,
		"description":    obj.Description,
		"access":         obj.Access,
		"institution_id": obj.InstitutionId,
		"premisEvents":   events,
		"generic_files":  genericFileMaps,
	}
	jsonBytes, err := json.Marshal(objects)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}

func (obj *IntellectualObject) CreateIngestEvent() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "ingest",
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            "Success",
		OutcomeDetail:      fmt.Sprintf("%d files copied", len(obj.GenericFiles)),
		Object:             "goamz S3 client",
		Agent:              "https://launchpad.net/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}, nil
}

func (obj *IntellectualObject) CreateIdEvent() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "identifier_assignment",
		DateTime:           time.Now(),
		Detail:             "Assigned bag identifier",
		Outcome:            "Success",
		OutcomeDetail:      obj.Identifier,
		Object:             "APTrust bagman",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Institution domain + tar file name",
	}, nil
}

// Serialize the subset of IntellectualObject data that fluctus
// will accept. This is for post/put, where essential info, such
// as institution id and/or object id will be in the URL.
func (obj *IntellectualObject) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":  obj.Identifier,
		"title":       obj.Title,
		"description": obj.Description,
		"access":      obj.Access,
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
	Algorithm string    `json:"algorithm"`
	DateTime  time.Time `json:"datetime"`
	Digest    string    `json:"digest"`
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
	Id                 string               `json:"id"`
	Identifier         string               `json:"identifier"`
	Format             string               `json:"file_format"`
	URI                string               `json:"uri"`
	Size               int64                `json:"size"`
	Created            time.Time            `json:"created"`
	Modified           time.Time            `json:"modified"`
	ChecksumAttributes []*ChecksumAttribute `json:"checksum"`
	Events             []*PremisEvent       `json:"premisEvents"`
}

// Serializes a version of GenericFile that Fluctus will accept as post/put input.
func (gf *GenericFile) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":          gf.Identifier,
		"file_format":         gf.Format,
		"uri":                 gf.URI,
		"size":                gf.Size,
		"created":             gf.Created,
		"modified":            gf.Modified,
		"checksum_attributes": gf.ChecksumAttributes,
	})
}

// Returns the original path of the file within the original bag.
// This is just the identifier minus the institution id and bag name.
// For example, if the identifier is "uc.edu/cin.675812/data/object.properties",
// this returns "data/object.properties"
func (gf *GenericFile) OriginalPath() (string, error) {
	parts := strings.SplitN(gf.Identifier, "/data/", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return fmt.Sprintf("data/%s", parts[1]), nil
}

// Returns the name of the original bag.
func (gf *GenericFile) BagName() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[1], nil
}

// Returns the name of the institution that owns this file.
func (gf *GenericFile) InstitutionId() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[0], nil
}

// Returns the checksum digest for the given algorithm for this file.
func (gf *GenericFile) GetChecksum(algorithm string) (*ChecksumAttribute) {
	for _, cs := range gf.ChecksumAttributes {
		if cs.Algorithm == algorithm {
			return cs
		}
	}
	return nil
}

// Returns the name of this file in the preservation storage bucket
// (that should be a UUID), or an error if the GenericFile does not
// have a valid preservation storage URL.
func (gf *GenericFile) PreservationStorageFileName() (string, error) {
	if strings.Index(gf.URI, "/") < 0 {
		return "", fmt.Errorf("Cannot get preservation storage file name because GenericFile has an invalid URI")
	}
	parts := strings.Split(gf.URI, "/")
	return parts[len(parts) - 1], nil
}

// User struct is used for logging in to fluctus.
type User struct {
	Email     string `json:"email"`
	Password  string `json:"password,omitempty"`
	ApiKey    string `json:"api_secret_key,omitempty"`
	AuthToken string `json:"authenticity_token,omitempty"`
}
