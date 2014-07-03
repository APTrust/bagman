// This file contains data structures that are shared by several sub-programs.

package bagman

import (
    "fmt"
    "time"
    "strings"
    "encoding/json"
    "launchpad.net/goamz/s3"
    "github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
    "github.com/APTrust/bagman/fluctus/models"
)

const (
	APTrustNamespace = "urn:mace:aptrust.org"
    ReceiveBucketPrefix = "aptrust.receiving."
    RestoreBucketPrefix = "aptrust.restore."
    S3DateFormat = "2006-01-02T15:04:05.000Z"
)


// S3File contains information about the S3 file we're
// trying to process from an intake bucket. BucketName
// and Key are the S3 bucket name and key. AttemptNumber
// describes whether this is the 1st, 2nd, 3rd,
// etc. attempt to process this file.
type S3File struct {
    BucketName     string
    Key            s3.Key
}

// ProcessStatus contains summary information describing
// the status of a bag in process. This data goes to Fluctus,
// so that APTrust partners can see which of their bags have
// been processed successfully, and why failed bags failed.
// See http://bit.ly/1pf7qxD for details.
//
// Type may have one of the following values: Ingest, Delete,
// Restore
//
// Stage may have one of the following values: Fetch (fetch
// tarred bag file from S3 receiving bucket), Unpack (unpack
// the tarred bag), Validate (make sure all data files are present,
// checksums are correct, required tags are present), Store (copy
// generic files to permanent S3 bucket for archiving), Record
// (save record of intellectual object, generic files and events
// to Fedora).
//
// Status may have one of the following values: Processing,
// Succeeded, Failed.
type ProcessStatus struct {
    Id           int         `json:"id"`
    Name         string      `json:"name"`
    Bucket       string      `json:"bucket"`
    ETag         string      `json:"etag"`
    BagDate      time.Time   `json:"bag_date"`
    Institution  string      `json:"institution"`
    Date         time.Time   `json:"date"`
    Note         string      `json:"note"`
    Action       string      `json:"action"`
    Stage        string      `json:"stage"`
    Status       string      `json:"status"`
    Outcome      string      `json:"outcome"`
    Retry        bool        `json:"retry"`
}

// Convert ProcessStatus to JSON, omitting id, which Rails won't permit.
// For internal use, json.Marshal() works fine.
func (status *ProcessStatus) SerializeForFluctus() ([]byte, error) {
    return json.Marshal(map[string]interface{}{
        "name": status.Name,
        "bucket": status.Bucket,
        "etag": status.ETag,
        "bag_date": status.BagDate,
        "institution": status.Institution,
        "date": status.Date,
        "note": status.Note,
        "action": status.Action,
        "stage": status.Stage,
        "status": status.Status,
        "outcome": status.Outcome,
        "retry": status.Retry,
    })
}

// Retry will be set to true if the attempt to process the file
// failed and should be tried again. This would be case, for example,
// if the failure was due to a network error. Retry is
// set to false if processing failed for some reason that
// will not change: for example, if the file cannot be
// untarred, checksums were bad, or data files were missing.
// If processing succeeded, Retry is irrelevant.
type ProcessResult struct {
    NsqMessage       *nsq.Message               `json:"-"`  // Don't serialize
    S3File           *S3File
    ErrorMessage     string
    FetchResult      *FetchResult
    TarResult        *TarResult
    BagReadResult    *BagReadResult
	FedoraResult     *FedoraResult
    Stage            string
    Retry            bool
}

// IntellectualObject returns an instance of models.IntellectualObject
// which describes what was unpacked from the bag. The IntellectualObject
// structure matches Fluctus' IntellectualObject model, and can be sent
// directly to Fluctus for recording.
func (result *ProcessResult) IntellectualObject() (obj *models.IntellectualObject, err error) {
    accessRights := result.BagReadResult.TagValue("Access")
    if accessRights == "" {
        accessRights = result.BagReadResult.TagValue("Rights")
    }
	// Fluctus wants access to be all lower-case
	accessRights = strings.ToLower(accessRights)
	// We probably should not do this correction, but we
	// need to get through test runs with the bad data
	// out partners submitted.
	// TODO: Remove this??
	if accessRights == "consortial" {
		accessRights = "consortia"
	} else if accessRights == "institutional" {
		accessRights = "institution"
	}
    institution := &models.Institution{
        BriefName: OwnerOf(result.S3File.BucketName),
    }
    // For now, object identifier is institution domain, plus the name
    // of the tar file, with ".tar" truncated.
    identifier := fmt.Sprintf("%s.%s",
        institution.BriefName,
        result.S3File.Key.Key[0:len(result.S3File.Key.Key)-4])
	files, err := result.GenericFiles()
	if err != nil {
		return nil, err
	}
    obj = &models.IntellectualObject{
		// TODO: Use proper institution id
        InstitutionId: institution.BriefName,
        Title: result.BagReadResult.TagValue("Title"),
        Description: result.BagReadResult.TagValue("Description"),
        Identifier: identifier,
        Access: accessRights,
		GenericFiles: files,
    }
	return obj, nil
}

// GenericFiles returns a list of GenericFile objects that were found
// in the bag.
func (result *ProcessResult) GenericFiles() (files []*models.GenericFile, err error) {
    files = make([]*models.GenericFile, len(result.TarResult.GenericFiles))
    for i, file := range(result.TarResult.GenericFiles) {
		gfModel, err := file.ToFluctusModel()
		if err != nil {
			return nil, err
		}
		files[i] = gfModel
    }
    return files, nil
}

// PremisEvents returns a list of Premis events generated during bag
// processing. Ingest, Fixity Generation (sha256), identifier
// assignment.
func (gf *GenericFile) PremisEvents() (events []*models.PremisEvent, err error) {
    events = make([]*models.PremisEvent, 5)
    // Fixity check
    fCheckEventUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for fixity check event: %v", err)
		return nil, detailedErr
	}
	// Fixity check event
    events[0] = &models.PremisEvent{
		Identifier: fCheckEventUuid.String(),
        EventType: "fixity_check",
        DateTime: gf.Md5Verified,
        Detail: "Fixity check against registered hash",
        Outcome: "Success",
        OutcomeDetail: fmt.Sprintf("md5:%s", gf.Md5),
        Object: "Go crypto/md5",
        Agent: "http://golang.org/pkg/crypto/md5/",
        OutcomeInformation: "Fixity matches",
    }

    // Ingest
    ingestEventUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for ingest event: %v", err)
		return nil, detailedErr
	}
	// Ingest event
    events[1] = &models.PremisEvent{
		Identifier: ingestEventUuid.String(),
        EventType: "ingest",
        DateTime: gf.StoredAt,
        Detail: "Completed copy to S3",
        Outcome: "Success",
        OutcomeDetail: gf.StorageMd5,
        Object: "bagman + goamz s3 client",
        Agent: "https://github.com/APTrust/bagman",
        OutcomeInformation: "Put using md5 checksum",
    }
    // Fixity Generation (sha256)
    fixityGenUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for fixity generation event: %v", err)
		return nil, detailedErr
	}
    events[2] = &models.PremisEvent{
		Identifier: fixityGenUuid.String(),
        EventType: "fixity_generation",
        DateTime: gf.Sha256Generated,
        Detail: "Calculated new fixity value",
        Outcome: "Success",
        OutcomeDetail: fmt.Sprintf("sha256:%s", gf.Sha256),
        Object: "Go language crypto/sha256",
        Agent: "http://golang.org/pkg/crypto/sha256/",
        OutcomeInformation: "",
    }
    // Identifier assignment (Friendly ID)
    idAssignmentUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for identifier assignment event for friendly ID: %v", err)
		return nil, detailedErr
	}
    events[3] = &models.PremisEvent{
		Identifier: idAssignmentUuid.String(),
        EventType: "identifier_assignment",
        DateTime: gf.UuidGenerated,
        Detail: "Assigned new institution.bag/path identifier",
        Outcome: "Success",
        OutcomeDetail: gf.Identifier,
        Object: "APTrust bag processor",
        Agent: "https://github.com/APTrust/bagman",
        OutcomeInformation: "",
    }
    // Identifier assignment (S3 URL)
    urlAssignmentUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for identifier assignment event for S3 URL: %v", err)
		return nil, detailedErr
	}
    events[4] = &models.PremisEvent{
		Identifier: urlAssignmentUuid.String(),
        EventType: "identifier_assignment",
        DateTime: gf.UuidGenerated,
        Detail: "Assigned new storage URL identifier",
        Outcome: "Success",
        OutcomeDetail: gf.StorageURL,
        Object: "Go uuid library + goamz S3 library",
        Agent: "http://github.com/nu7hatch/gouuid",
        OutcomeInformation: "",
    }
    return events, nil
}

// IngestStatus returns a lightweight Status object suitable for reporting
// to the Fluctus results table, so that APTrust partners can view
// the status of their submitted bags.
func (result *ProcessResult) IngestStatus() (status *ProcessStatus) {
    status = &ProcessStatus{}
    status.Date = time.Now().UTC()
    status.Action = "Ingest"
    status.Name = result.S3File.Key.Key
    bagDate, _ := time.Parse(S3DateFormat, result.S3File.Key.LastModified)
    status.BagDate = bagDate
    status.Bucket = result.S3File.BucketName
    // Strip the quotes off the ETag
    status.ETag = strings.Replace(result.S3File.Key.ETag, "\"", "", 2)
    status.Stage = result.Stage
    status.Status = "Processing"
    if result.ErrorMessage != "" {
        status.Note = result.ErrorMessage
        status.Status = "Failed"
		// Indicate whether we want to try re-processing this bag.
		// For transient errors (e.g. network problems), we retry.
		// For permanent errors (e.g. invalid bag), we do not retry.
		status.Retry = result.Retry
    } else {
        status.Note = "No problems"
        if result.Stage == "Validate" {
            // We made it through last stage with no errors
            // TODO: Change back to "Record" after demo.
            // *** NOTE: THE LAST STAGE SHOULD BE "Record", BUT FOR DEMO
            // WE'LL CONSIDER "Validate" TO BE SUCCESS ***
            status.Status = "Succeeded"
        }
		// If there were no errors, bag was processed sucessfully,
		// and there is no need to retry.
		status.Retry = false
    }
    status.Institution = OwnerOf(result.S3File.BucketName)
    status.Outcome = status.Status
    return status
}

// BucketSummary contains information about an S3 bucket and its contents.
type BucketSummary struct {
    BucketName     string
    Keys           []s3.Key // TODO: Change to slice of pointers!
}

// GenericFile contains information about a generic
// data file within the data directory of bag or tar archive.
type GenericFile struct {
	// Path is the path to the file within the bag. It should
	// always begin with "data/"
    Path               string
	// The size of the file, in bytes.
    Size               int64
	// The time the file was created. This is here because
	// it's part of the Fedora object model, but we do not
	// actually have access to this data. Created will usually
	// be set to empty time or mod time.
    Created            time.Time
	// The time the file was last modified.
    Modified           time.Time
	// The md5 checksum for the file & when we verified it.
    Md5                string
	Md5Verified        time.Time
	// The sha256 checksum for the file.
    Sha256             string
	// The time the sha256 checksum was generated. The bag processor
	// generates this checksum when it unpacks the file from the
	// tar archive.
    Sha256Generated    time.Time
	// The unique identifier for this file. This is generated by the
	// bag processor when it unpackes the file from the tar archive.
    Uuid               string
	// The time when the bag processor generated the UUID for this file.
    UuidGenerated      time.Time
	// The mime type of the file. This should be suitable for use in an
	// HTTP Content-Type header.
    MimeType           string
	// A message describing any errors that occurred during the processing
	// of this file. E.g. I/O error, bad checksum, etc. If this is empty,
	// there were no processing errors.
    ErrorMessage       string
	// The file's URL in the S3 preservation bucket. This is assigned by
	// the bag processor after it stores the file in the preservation
	// bucket. If this is blank, the file has not yet been sent to
	// preservation.
    StorageURL         string
	StoredAt           time.Time
	StorageMd5         string
	// The unique id of this GenericFile. Institution domain name +
	// "." + bag name.
	Identifier         string
	IdentifierAssigned time.Time
}


// Converts bagman.GenericFile to models.GenericFile, which is what
// Fluctus understands.
func (gf *GenericFile) ToFluctusModel() (*models.GenericFile, error) {
	checksumAttributes := make([]*models.ChecksumAttribute, 2)
	checksumAttributes[0] = &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: gf.Modified,
		Digest: gf.Md5,
	}
	checksumAttributes[1] = &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: gf.Sha256Generated,
		Digest: gf.Sha256,
	}
	events, err := gf.PremisEvents()
	if err != nil {
		return nil, err
	}
	gfModel := &models.GenericFile{
		Identifier: gf.Identifier,
		Format: gf.MimeType,
		URI: gf.StorageURL,
		Size: gf.Size,
		Created: gf.Modified,
		Modified: gf.Modified,
		ChecksumAttributes: checksumAttributes,
		Events: events,
	}
	return gfModel, nil
}

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
    InputFile       string
    OutputDir       string
    ErrorMessage    string
    Warnings        []string
    FilesUnpacked   []string
    GenericFiles    []*GenericFile
}

// GenericFilePaths returns a list of all the GenericFile paths
// that were untarred from the bag. The list will look something
// like "data/file1.gif", "data/file2.pdf", etc.
func (result *TarResult) GenericFilePaths() ([]string) {
	paths := make([]string, len(result.GenericFiles))
	for index, gf := range(result.GenericFiles) {
		paths[index] = gf.Path
	}
	return paths
}

// This Tag struct is essentially the same as the bagins
// TagField struct, but its properties are public and can
// be easily serialized to / deserialized from JSON.
type Tag struct {
    Label string
    Value string
}

// BagReadResult contains data describing the result of
// processing a single bag. If there were any processing
// errors, this structure should tell us exactly what
// happened and where.
type BagReadResult struct {
    Path             string
    Files            []string
    ErrorMessage     string
    Tags             []Tag
    ChecksumErrors   []error
}

// TagValue returns the value of the tag with the specified label.
func (result *BagReadResult) TagValue(tagLabel string) (tagValue string) {
    lcTagLabel := strings.ToLower(tagLabel)
    for _, tag := range result.Tags {
        if strings.ToLower(tag.Label) == lcTagLabel {
            tagValue = tag.Value
            break
        }
    }
    return tagValue
}

// FetchResult descibes the results of fetching a bag from S3
// and verification of that bag.
type FetchResult struct {
    BucketName       string
    Key              string
    LocalTarFile     string
    RemoteMd5        string
    LocalMd5         string
    Md5Verified      bool
    Md5Verifiable    bool
    ErrorMessage     string
    Warning          string
    Retry            bool
}

// Returns the domain name of the institution that owns the specified bucket.
// For example, if bucketName is 'aptrust.receiving.unc.edu' the return value
// will be 'unc.edu'.
func OwnerOf (bucketName string) (institution string) {
    if strings.HasPrefix(bucketName, ReceiveBucketPrefix) {
        institution = strings.Replace(bucketName, ReceiveBucketPrefix, "", 1)
    } else if strings.HasPrefix(bucketName, RestoreBucketPrefix) {
        institution = strings.Replace(bucketName, RestoreBucketPrefix, "", 1)
    }
    return institution
}

// Returns the name of the specified institution's receiving bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.receiving.unc.edu'
func ReceivingBucketFor (institution string) (bucketName string) {
    return ReceiveBucketPrefix + institution
}

// Returns the name of the specified institution's restoration bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.restore.unc.edu'
func RestorationBucketFor (institution string) (bucketName string) {
    return RestoreBucketPrefix + institution
}

// MetadataRecord describes the result of an attempt to record metadata
// in Fluctus/Fedora.
type MetadataRecord struct {
	// Type describes what we're trying to record in Fedora. It can
	// be "IntellectualObject", "GenericFile", or "PremisEvent"
	Type         string
	// Action contains information about what was in Fedora.
	// For Type IntellectualObject, this will be "object_registered".
	// For Type GenericFile, this will be "file_registered".
	// For Type PremisEvent, this will be the name of the event:
	// "ingest", "identifier_assignment", or "fixity_generation".
	Action       string
	// For actions or events pertaining to a GenericFile this will be the path
	// of the file the action pertains to. For example, for fixity_generation
	// on the file "data/images/aerial.jpg", the EventObject would be
	// "data/images/aerial.jpg". For actions or events pertaining to the
	// IntellectualObject, this will be the IntellectualObject identifier.
	EventObject  string
	// ErrorMessage contains a description of the error that occurred
	// when we tried to save this bit of metadata in Fluctus/Fedora.
	// It will be empty if there was no error, or if we have not yet
	// attempted to save the item.
	ErrorMessage string
}

// Returns true if this bit of metadata was successfully saved to Fluctus/Fedora.
func (record *MetadataRecord) Succeeded() (bool) {
	return record.ErrorMessage == ""
}

// FedoraResult is a collection of MetadataRecords, each indicating
// whether or not some bit of metadata has been recorded in Fluctus/Fedora.
// The bag processor needs to keep track of this information to ensure
// it successfully records all metadata in Fedora.
type FedoraResult struct {
	ObjectIdentifier  string
	GenericFilePaths []string
	MetadataRecords  []*MetadataRecord
}

// Creates a new FedoraResult object with the specified IntellectualObject
// identifier and list of GenericFile paths.
func NewFedoraResult(objectIdentifier string, genericFilePaths []string)(*FedoraResult) {
	return &FedoraResult{
		ObjectIdentifier: objectIdentifier,
		GenericFilePaths: genericFilePaths,
	}
}

// AddRecord adds a new MetadataRecord to the Fedora result.
func (result *FedoraResult) AddRecord (recordType, action, eventObject, errorMessage string) (error) {
	if recordType != "IntellectualObject" && recordType != "GenericFile" && recordType !=  "PremisEvent" {
		return fmt.Errorf("Param recordType must be one of 'IntellectualObject', 'GenericFile', or 'PremisEvent'")
	}
	if recordType == "PremisEvent" && action != "ingest" && action != "fixity_check" &&
		action != "identifier_assignment" && action != "fixity_generation" {
		return fmt.Errorf("'%s' is not a valid action for PremisEvent", action)
	} else if recordType == "IntellectualObject" && action != "object_registered" {
		return fmt.Errorf("'%s' is not a valid action for IntellectualObject", action)
	} else if recordType == "GenericFile" && action != "file_registered" {
		return fmt.Errorf("'%s' is not a valid action for GenericFile", action)
	}
	if eventObject == "" {
		return fmt.Errorf("Param eventObject cannot be empty")
	}
	record := &MetadataRecord{
		Type: recordType,
		Action: action,
		EventObject: eventObject,
		ErrorMessage: errorMessage,
	}
	result.MetadataRecords = append(result.MetadataRecords, record)
	return nil
}

// FindRecord returns the MetadataRecord with the specified type,
// action and event object.
func (result *FedoraResult) FindRecord(recordType, action, eventObject string) (*MetadataRecord) {
	for _, record := range result.MetadataRecords {
		if record.Type == recordType && record.Action == action && record.EventObject == eventObject {
			return record
		}
	}
	return nil
}

// Returns true/false to indicate whether the specified bit of
// metadata was recorded successfully in Fluctus/Fedora.
func (result *FedoraResult) RecordSucceeded(recordType, action, eventObject string) (bool) {
	record := result.FindRecord(recordType, action, eventObject)
	return record != nil && record.Succeeded()
}

// Returns true if all metadata was recorded successfully in Fluctus/Fedora.
// A true result means that all of the following were successfully recorded:
//
// 1) Registration of the IntellectualObject. This may mean creating a new
// IntellectualObject or updating an existing one.
//
// 2) Recording the ingest PremisEvent for the IntellectualObject.
//
// 3) Registration of EACH of the object's GenericFiles. This may mean
// creating a new GenericFile or updating an existing one.
//
// 4) Recording the intentifier_assignment for EACH GenericFile. The
// identifier is typically a UUID.
//
// 5) Recording the fixity_generation for EACH GenericFile. Although most
// files already come with md5 checksums from S3, we always generate a
// sha256 as well.
//
// A successful FedoraResult will have (2 + (3 * len(GenericFilePaths)))
// successful MetadataRecords.
func (result *FedoraResult) AllRecordsSucceeded() (bool) {
	// Make sure the IntellectualObject was created
	if false == result.RecordSucceeded("IntellectualObject", "object_registered", result.ObjectIdentifier) {
		return false
	}
	// Make sure the ingest event was recorded
	if false == result.RecordSucceeded("PremisEvent", "ingest", result.ObjectIdentifier) {
		return false
	}
	// Make sure we recorded fixity generation and identifier assignment
	// for each generic file.
	for _, filePath := range(result.GenericFilePaths) {
		if false == result.RecordSucceeded("GenericFile", "file_registered", filePath) {
			return false
		}
		if false == result.RecordSucceeded("PremisEvent", "identifier_assignment", filePath) {
			return false
		}
		if false == result.RecordSucceeded("PremisEvent", "fixity_generation", filePath) {
			return false
		}
	}
	return true
}
