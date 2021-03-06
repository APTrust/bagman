package bagman

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

/*
IntellectualObject is Fluctus' version of an IntellectualObject.
It belongs to an Institution and consists of one or more
GenericFiles and a number of events.

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
	AltIdentifier []string       `json:"alt_identifier"`
	GenericFiles  []*GenericFile `json:"generic_files"`
	Events        []*PremisEvent `json:"events"`
}

// Returns the original bag name of this object. That's
// the intellectual object id, minus the institution name
// and the slash. So "test.edu/test.edu.bag_999" returns
// "test.edu.bag_999"
func (obj *IntellectualObject) OriginalBagName() (string) {
	i := strings.Index(obj.Identifier, "/") + 1
	return obj.Identifier[i:]
}

// Returns the total number of bytes of all of the generic
// files in this object. The object's bag size will be slightly
// larger than this, because it will include a manifest, tag
// files and tar header.
func (obj *IntellectualObject) TotalFileSize() (int64) {
	total := int64(0)
	for _, genericFile := range obj.GenericFiles {
		total += genericFile.Size
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

// SerializeForCreate serializes a fluctus intellectual object
// along with all of its generic files and events in a single shot.
// The output is a byte array of JSON data.
//
// If maxGenericFiles is greater than zero, the JSON data will
// include only that number of generic files. Otherwise, it will
// include all of the generic files.
//
// Fluctus is somewhat efficient at creating new intellectual
// objects when all of the files and events are included in the
// JSON for the initial create request. But some Intellectual Objects
// contain more than 10,000 files, and if we send all of this data
// at once to Fluctus, it crashes.
func (obj *IntellectualObject) SerializeForCreate(maxGenericFiles int) ([]byte, error) {
	lastIndex := len(obj.GenericFiles)
	if maxGenericFiles > 0 {
		lastIndex = Min(maxGenericFiles, len(obj.GenericFiles))
	}
	genericFiles := obj.GenericFiles[0:lastIndex]

	genericFileMaps := GenericFilesToBulkSaveMaps(genericFiles)

	events := make([]*PremisEvent, 3)
	ingestEvent := obj.CreateIngestEvent()
	idEvent := obj.CreateIdEvent()
	rightsEvent := obj.CreateRightsEvent()
	events[0] = idEvent
	events[1] = ingestEvent
	events[2] = rightsEvent

	// Even though we're sending only one object,
	// Fluctus expects an array.
	objects := make([]map[string]interface{}, 1)
	objects[0] = map[string]interface{}{
		"identifier":     obj.Identifier,
		"title":          obj.Title,
		"description":    obj.Description,
		"alt_identifier": obj.AltIdentifier,
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

func (obj *IntellectualObject) CreateIngestEvent() (*PremisEvent) {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "ingest",
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            "Success",
		OutcomeDetail:      fmt.Sprintf("%d files copied", len(obj.GenericFiles)),
		Object:             "goamz S3 client",
		Agent:              "https://github.com/crowdmob/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}
}

func (obj *IntellectualObject) CreateIdEvent() (*PremisEvent) {
	eventId := uuid.NewV4()
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
	}
}

func (obj *IntellectualObject) CreateRightsEvent() (*PremisEvent) {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "access_assignment",
		DateTime:           time.Now(),
		Detail:             "Assigned bag access rights",
		Outcome:            "Success",
		OutcomeDetail:      obj.Access,
		Object:             "APTrust bagman",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Set access to " + obj.Access,
	}
}

// Serialize the subset of IntellectualObject data that fluctus
// will accept. This is for post/put, where essential info, such
// as institution id and/or object id will be in the URL.
func (obj *IntellectualObject) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":     obj.Identifier,
		"title":          obj.Title,
		"description":    obj.Description,
		"alt_identifier": obj.AltIdentifier,
		"access":         obj.Access,
	})
}
