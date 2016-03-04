package bagman

import (
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/op/go-logging"
	"os"
	"strings"
	"time"
)

/*
Retry will be set to true if the attempt to process the file
failed and should be tried again. This would be case, for example,
if the failure was due to a network error. Retry is
set to false if processing failed for some reason that
will not change: for example, if the file cannot be
untarred, checksums were bad, or data files were missing.
If processing succeeded, Retry is irrelevant.
*/
type ProcessResult struct {
	NsqMessage    *nsq.Message `json:"-"` // Don't serialize
	S3File        *S3File
	ErrorMessage  string
	FetchResult   *FetchResult
	TarResult     *TarResult
	BagReadResult *BagReadResult
	FedoraResult  *FedoraResult
	BagDeletedAt  time.Time
	Stage         StageType
	Retry         bool
}

// IntellectualObject returns an instance of IntellectualObject
// which describes what was unpacked from the bag. The IntellectualObject
// structure matches Fluctus' IntellectualObject model, and can be sent
// directly to Fluctus for recording.
func (result *ProcessResult) IntellectualObject() (obj *IntellectualObject, err error) {
	accessRights := result.BagReadResult.TagValue("Access")
	if accessRights == "" {
		accessRights = result.BagReadResult.TagValue("Rights")
	}
	// Fluctus wants access to be all lower-case
	accessRights = strings.ToLower(accessRights)
	// We probably should not do this correction, but we
	// need to get through test runs with the bad data
	// out partners submitted.
	if accessRights == "consortial" {
		accessRights = "consortia"
	} else if accessRights == "institutional" {
		accessRights = "institution"
	}
	institution := &Institution{
		BriefName: OwnerOf(result.S3File.BucketName),
	}
	identifier, err := result.S3File.ObjectName()
	if err != nil {
		return nil, err
	}
	files, err := result.GenericFiles()
	if err != nil {
		return nil, err
	}
	obj = &IntellectualObject{
		InstitutionId: institution.BriefName,
		Title:         result.BagReadResult.TagValue("Title"),
		Description:   result.BagReadResult.TagValue("Internal-Sender-Description"),
		Identifier:    identifier,
		Access:        accessRights,
		GenericFiles:  files,
	}
	altId := result.BagReadResult.TagValue("Internal-Sender-Identifier")
	if altId != "" {
		obj.AltIdentifier = []string { altId }
	}
	return obj, nil
}

// GenericFiles returns a list of GenericFile objects that were found
// in the bag.
func (result *ProcessResult) GenericFiles() (files []*GenericFile, err error) {
	files = make([]*GenericFile, len(result.TarResult.Files))
	for i, file := range result.TarResult.Files {
		gfModel, err := file.ToGenericFile()
		if err != nil {
			return nil, err
		}
		files[i] = gfModel
	}
	return files, nil
}


// IngestStatus returns a lightweight Status object suitable for reporting
// to the Fluctus results table, so that APTrust partners can view
// the status of their submitted bags.
//
// TODO: Refactor. We should have to pass in a logger. <Sigh>
func (result *ProcessResult) IngestStatus(logger *logging.Logger) (status *ProcessStatus) {
	status = &ProcessStatus{}
	status.Date = time.Now().UTC()
	status.Action = ActionIngest
	status.Name = result.S3File.Key.Key
	bagDate, _ := time.Parse(S3DateFormat, result.S3File.Key.LastModified)
	status.BagDate = bagDate
	status.Bucket = result.S3File.BucketName
	// Strip the quotes off the ETag
	status.ETag = strings.Replace(result.S3File.Key.ETag, "\"", "", 2)
	status.Stage = result.Stage
	status.Status = StatusPending
	if result.ErrorMessage != "" {
		status.Status = StatusStarted // Did not complete this stage
		status.Note = result.ErrorMessage
		// Indicate whether we want to try re-processing this bag.
		// For transient errors (e.g. network problems), we retry.
		// For permanent errors (e.g. invalid bag), we do not retry.
		status.Retry = result.Retry
		if status.Retry == false {
			// Only mark an item as failed if we know we're not
			// going to retry it. If we're going to retry it, leave
			// it as "Pending", so that institutional admins
			// cannot delete it from the ProcessedItems list in
			// Fluctus.
			status.Status = StatusFailed
		}
	} else {
		status.Note = "No problems"
		if result.Stage == "Cleanup" {
			status.Status = StatusSuccess
		}
		// If there were no errors, bag was processed sucessfully,
		// and there is no need to retry.
		status.Retry = false
	}
	status.Institution = OwnerOf(result.S3File.BucketName)
	status.Outcome = string(status.Status)

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		// This should never happen, but if it does,
		// log it and put a visible note in the Web UI,
		// or we'll never track down what went wrong.
		message := fmt.Sprintf("Error converting result to JSON: %v", err)
		logger.Error(message)
		status.Note = fmt.Sprintf("%s -- %s", status.Note, message)
	}
	status.State = string(jsonBytes)

	nodename, err := os.Hostname()
	if err != nil {
		logger.Error("Aarg, matey! I'm so fulla rum, I don't know me own name!")
		nodename = "Drunken Sailor"
	}
	status.Node = nodename
	status.Pid = os.Getpid()

	return status
}
