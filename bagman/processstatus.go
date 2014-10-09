package bagman

import (
	"encoding/json"
	"time"
)


// ProcessStatus contains summary information describing
// the status of a bag in process. This data goes to Fluctus,
// so that APTrust partners can see which of their bags have
// been processed successfully, and why failed bags failed.
// See http://bit.ly/1pf7qxD for details.
//
// Type may have one of the following values: Ingest, Delete,
// Restore
//
// Stage may have one of the following values: Receive (bag was
// uploaded by partner into receiving bucket), Fetch (fetch
// tarred bag file from S3 receiving bucket), Unpack (unpack
// the tarred bag), Validate (make sure all data files are present,
// checksums are correct, required tags are present), Store (copy
// generic files to permanent S3 bucket for archiving), Record
// (save record of intellectual object, generic files and events
// to Fedora).
//
// Status may have one of the following values: Pending,
// Success, Failed.
type ProcessStatus struct {
	Id                     int        `json:"id"`
	ObjectIdentifier       string     `json:"object_identifier"`
	GenericFileIdentifier  string     `json:"generic_file_identifier"`
	Name                   string     `json:"name"`
	Bucket                 string     `json:"bucket"`
	ETag                   string     `json:"etag"`
	BagDate                time.Time  `json:"bag_date"`
	Institution            string     `json:"institution"`
	User                   string     `json:"user"`
	Date                   time.Time  `json:"date"`
	Note                   string     `json:"note"`
	Action                 ActionType `json:"action"`
	Stage                  StageType  `json:"stage"`
	Status                 StatusType `json:"status"`
	Outcome                string     `json:"outcome"`
	Retry                  bool       `json:"retry"`
	Reviewed               bool       `json:"reviewed"`
}

// Convert ProcessStatus to JSON, omitting id, which Rails won't permit.
// For internal use, json.Marshal() works fine.
func (status *ProcessStatus) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":                    status.Name,
		"bucket":                  status.Bucket,
		"etag":                    status.ETag,
		"bag_date":                status.BagDate,
		"institution":             status.Institution,
		"object_identifier":       status.ObjectIdentifier,
		"generic_file_identifier": status.GenericFileIdentifier,
		"date":                    status.Date,
		"note":                    status.Note,
		"action":                  status.Action,
		"stage":                   status.Stage,
		"status":                  status.Status,
		"outcome":                 status.Outcome,
		"retry":                   status.Retry,
		"reviewed":                status.Reviewed,
	})
}

// Returns true if an object's files have been stored in S3 preservation bucket.
func (status *ProcessStatus) HasBeenStored() (bool) {
	if status.Action == ActionIngest {
		return status.Stage == StageRecord ||
			status.Stage == StageCleanup ||
			status.Stage == StageResolve ||
			(status.Stage == StageStore && status.Status == StatusPending)
	} else {
		return true
	}
}

func (status *ProcessStatus) IsStoring() (bool) {
	return status.Action == ActionIngest &&
		status.Stage == StageStore &&
		status.Status == StatusStarted
}

// Returns true if we should try to ingest this item.
func (status *ProcessStatus) ShouldTryIngest() (bool) {
	return status.HasBeenStored() == false && status.IsStoring() == false && status.Retry == true
}
