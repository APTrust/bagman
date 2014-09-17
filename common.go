// This file contains data structures that are shared by several sub-programs.

package bagman

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/bitly/go-nsq"
	"github.com/diamondap/goamz/s3"
	"github.com/nu7hatch/gouuid"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var MultipartSuffix = regexp.MustCompile("\\.b\\d+\\.of\\d+$")

const (
	APTrustNamespace    = "urn:mace:aptrust.org"
	ReceiveBucketPrefix = "aptrust.receiving."
	RestoreBucketPrefix = "aptrust.restore."
	S3DateFormat        = "2006-01-02T15:04:05.000Z"
)

// S3File contains information about the S3 file we're
// trying to process from an intake bucket. BucketName
// and Key are the S3 bucket name and key. AttemptNumber
// describes whether this is the 1st, 2nd, 3rd,
// etc. attempt to process this file.
type S3File struct {
	BucketName string
	Key        s3.Key
}

// Returns the object identifier that will identify this bag
// in fedora. That's the institution identifier, followed by
// a slash and the tar file name, minus the .tar extension
// and the ".bag1of12" multipart extension. So for BucketName
// "aptrust.receiving.unc.edu" and Key.Key "nc_bag.001.of030.tar",
// this would return "unc.edu/nc_bag"
func (s3File *S3File) ObjectName() (string, error) {
	institution := OwnerOf(s3File.BucketName)
	cleanBagName, err := CleanBagName(s3File.Key.Key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", institution, cleanBagName), nil
}

// The name of the owning institution, followed by a slash, followed
// by the name of the tar file. This differs from the ObjectName,
// because it will have the .tar or bag.001.of030.tar suffix.
func (s3File *S3File) BagName() (string) {
	return fmt.Sprintf("%s/%s", OwnerOf(s3File.BucketName), s3File.Key.Key)
}



// Status enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
type StatusType string

const (
	StatusStarted   StatusType = "Started"
	StatusPending              = "Pending"
	StatusSuccess              = "Success"
	StatusFailed               = "Failed"
	StatusCancelled            = "Cancelled"
)

// Stage enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
type StageType string

const (
	StageRequested StageType = "Requested"
	StageReceive             = "Receive"
	StageFetch               = "Fetch"
	StageUnpack              = "Unpack"
	StageValidate            = "Validate"
	StageStore               = "Store"
	StageRecord              = "Record"
	StageCleanup             = "Cleanup"
	StageResolve             = "Resolve"
)

// Action enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
type ActionType string

const (
	ActionIngest      ActionType = "Ingest"
	ActionFixityCheck            = "Fixity Check"
	ActionRestore                = "Restore"
	ActionDelete                 = "Delete"
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

/*
Retry will be set to true if the attempt to process the file
failed and should be tried again. This would be case, for example,
if the failure was due to a network error. Retry is
set to false if processing failed for some reason that
will not change: for example, if the file cannot be
untarred, checksums were bad, or data files were missing.
If processing succeeded, Retry is irrelevant.

ReingestNoOop tells us if we've hit a special case in which
an already-ingested bag is reprocessed, but none of the GenericFiles
have changed. In this case, ingest was a no-op, since no files were
copied to S3, no metadata should be recorded in Fedora, and no events
should be generated.
*/
type ProcessResult struct {
	NsqMessage    *nsq.Message `json:"-"` // Don't serialize
	S3File        *S3File
	ErrorMessage  string
	FetchResult   *FetchResult
	TarResult     *TarResult
	BagReadResult *BagReadResult
	FedoraResult  *FedoraResult
	Stage         StageType
	Retry         bool
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
	if accessRights == "consortial" {
		accessRights = "consortia"
	} else if accessRights == "institutional" {
		accessRights = "institution"
	}
	institution := &models.Institution{
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
	obj = &models.IntellectualObject{
		// TODO: Use proper institution id
		InstitutionId: institution.BriefName,
		Title:         result.BagReadResult.TagValue("Title"),
		Description:   result.BagReadResult.TagValue("Description"),
		Identifier:    identifier,
		Access:        accessRights,
		GenericFiles:  files,
	}
	return obj, nil
}

// GenericFiles returns a list of GenericFile objects that were found
// in the bag.
func (result *ProcessResult) GenericFiles() (files []*models.GenericFile, err error) {
	files = make([]*models.GenericFile, len(result.TarResult.GenericFiles))
	for i, file := range result.TarResult.GenericFiles {
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
		Identifier:         fCheckEventUuid.String(),
		EventType:          "fixity_check",
		DateTime:           gf.Md5Verified,
		Detail:             "Fixity check against registered hash",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("md5:%s", gf.Md5),
		Object:             "Go crypto/md5",
		Agent:              "http://golang.org/pkg/crypto/md5/",
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
		Identifier:         ingestEventUuid.String(),
		EventType:          "ingest",
		DateTime:           gf.StoredAt,
		Detail:             "Completed copy to S3",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      gf.StorageMd5,
		Object:             "bagman + goamz s3 client",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Put using md5 checksum",
	}
	// Fixity Generation (sha256)
	fixityGenUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for fixity generation event: %v", err)
		return nil, detailedErr
	}
	events[2] = &models.PremisEvent{
		Identifier:         fixityGenUuid.String(),
		EventType:          "fixity_generation",
		DateTime:           gf.Sha256Generated,
		Detail:             "Calculated new fixity value",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("sha256:%s", gf.Sha256),
		Object:             "Go language crypto/sha256",
		Agent:              "http://golang.org/pkg/crypto/sha256/",
		OutcomeInformation: "",
	}
	// Identifier assignment (Friendly ID)
	idAssignmentUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for identifier assignment event for friendly ID: %v", err)
		return nil, detailedErr
	}
	events[3] = &models.PremisEvent{
		Identifier:         idAssignmentUuid.String(),
		EventType:          "identifier_assignment",
		DateTime:           gf.UuidGenerated,
		Detail:             "Assigned new institution.bag/path identifier",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      gf.Identifier,
		Object:             "APTrust bag processor",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "",
	}
	// Identifier assignment (S3 URL)
	urlAssignmentUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for identifier assignment event for S3 URL: %v", err)
		return nil, detailedErr
	}
	events[4] = &models.PremisEvent{
		Identifier:         urlAssignmentUuid.String(),
		EventType:          "identifier_assignment",
		DateTime:           gf.UuidGenerated,
		Detail:             "Assigned new storage URL identifier",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      gf.StorageURL,
		Object:             "Go uuid library + goamz S3 library",
		Agent:              "http://github.com/nu7hatch/gouuid",
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
		if result.Stage == "Record" {
			status.Status = StatusSuccess
		}
		// If there were no errors, bag was processed sucessfully,
		// and there is no need to retry.
		status.Retry = false
	}
	status.Institution = OwnerOf(result.S3File.BucketName)
	status.Outcome = string(status.Status)
	return status
}


// BucketSummary contains information about an S3 bucket and its contents.
type BucketSummary struct {
	BucketName string
	Keys       []s3.Key // TODO: Change to slice of pointers!
}

// GenericFile contains information about a generic
// data file within the data directory of bag or tar archive.
type GenericFile struct {
	// Path is the path to the file within the bag. It should
	// always begin with "data/"
	Path string
	// The size of the file, in bytes.
	Size int64
	// The time the file was created. This is here because
	// it's part of the Fedora object model, but we do not
	// actually have access to this data. Created will usually
	// be set to empty time or mod time.
	Created time.Time
	// The time the file was last modified.
	Modified time.Time
	// The md5 checksum for the file & when we verified it.
	Md5         string
	Md5Verified time.Time
	// The sha256 checksum for the file.
	Sha256 string
	// The time the sha256 checksum was generated. The bag processor
	// generates this checksum when it unpacks the file from the
	// tar archive.
	Sha256Generated time.Time
	// The unique identifier for this file. This is generated by the
	// bag processor when it unpackes the file from the tar archive.
	Uuid string
	// The time when the bag processor generated the UUID for this file.
	UuidGenerated time.Time
	// The mime type of the file. This should be suitable for use in an
	// HTTP Content-Type header.
	MimeType string
	// A message describing any errors that occurred during the processing
	// of this file. E.g. I/O error, bad checksum, etc. If this is empty,
	// there were no processing errors.
	ErrorMessage string
	// The file's URL in the S3 preservation bucket. This is assigned by
	// the bag processor after it stores the file in the preservation
	// bucket. If this is blank, the file has not yet been sent to
	// preservation.
	StorageURL string
	StoredAt   time.Time
	StorageMd5 string
	// The unique id of this GenericFile. Institution domain name +
	// "." + bag name.
	Identifier         string
	IdentifierAssigned time.Time

	// If true, some version of this file already exists in the S3
	// preservation bucket and its metadata is in Fedora.
	ExistingFile bool

	// If true, this file needs to be saved to the S3 preservation
	// bucket, and its metadata and events must be saved to Fedora.
	// This will be true if the file is new, or if its an existing
	// file whose contents have changed since it was last ingested.
	NeedsSave bool
}

func NewGenericFile() (*GenericFile) {
	return &GenericFile{
		ExistingFile: false,
		NeedsSave: true,
	}
}


// Converts bagman.GenericFile to models.GenericFile, which is what
// Fluctus understands.
func (gf *GenericFile) ToFluctusModel() (*models.GenericFile, error) {
	checksumAttributes := make([]*models.ChecksumAttribute, 2)
	checksumAttributes[0] = &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime:  gf.Modified,
		Digest:    gf.Md5,
	}
	checksumAttributes[1] = &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime:  gf.Sha256Generated,
		Digest:    gf.Sha256,
	}
	events, err := gf.PremisEvents()
	if err != nil {
		return nil, err
	}
	gfModel := &models.GenericFile{
		Identifier:         gf.Identifier,
		Format:             gf.MimeType,
		URI:                gf.StorageURL,
		Size:               gf.Size,
		Created:            gf.Modified,
		Modified:           gf.Modified,
		ChecksumAttributes: checksumAttributes,
		Events:             events,
	}
	return gfModel, nil
}

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
	InputFile     string
	OutputDir     string
	ErrorMessage  string
	Warnings      []string
	FilesUnpacked []string
	GenericFiles  []*GenericFile
}

// Returns true if any of the untarred files are new or updated.
func (result *TarResult) AnyFilesNeedSaving() (bool) {
	for _, gf := range result.GenericFiles {
		if gf.NeedsSave == true {
			return true
		}
	}
	return false
}

// GenericFilePaths returns a list of all the GenericFile paths
// that were untarred from the bag. The list will look something
// like "data/file1.gif", "data/file2.pdf", etc.
func (result *TarResult) GenericFilePaths() []string {
	paths := make([]string, len(result.GenericFiles))
	for index, gf := range result.GenericFiles {
		paths[index] = gf.Path
	}
	return paths
}

// Returns the GenericFile with the specified path, if it exists.
func (result *TarResult) GetFileByPath(filePath string) (*GenericFile) {
	for index, gf := range result.GenericFiles {
		if gf.Path == filePath {
			// Be sure to return to original, and not a copy!
			return result.GenericFiles[index]
		}
	}
	return nil
}

// MergeExistingFiles merges data from generic files that
// already exist in Fedora. This is necessary when an existing
// bag is reprocessed or re-uploaded.
func (result *TarResult) MergeExistingFiles(gfModels []*models.GenericFile) {
	for _, gfModel := range gfModels {
		origPath, _ := gfModel.OriginalPath()
		gf := result.GetFileByPath(origPath)
		if gf != nil {
			gf.ExistingFile = true
			// Files have the same path and name. If the checksum
			// has not changed, there is no reason to re-upload
			// this file to the preservation bucket, nor is there
			// any reason to create new ingest events in Fedora.
			existingMd5 := gfModel.GetChecksum("md5")
			if gf.Md5 == existingMd5.Digest {
				gf.NeedsSave = false
			}
		}
	}
}

// Returns true if any generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AnyFilesCopiedToPreservation() bool {
	for _, gf := range result.GenericFiles {
		if gf.StorageURL != "" {
			return true
		}
	}
	return false
}

// Returns true if all generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AllFilesCopiedToPreservation() bool {
	for _, gf := range result.GenericFiles {
		if gf.NeedsSave && gf.StorageURL == "" {
			return false
		}
	}
	return true
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
	Path           string
	Files          []string
	ErrorMessage   string
	Tags           []Tag
	ChecksumErrors []error
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
	BucketName    string
	Key           string
	LocalTarFile  string
	RemoteMd5     string
	LocalMd5      string
	Md5Verified   bool
	Md5Verifiable bool
	ErrorMessage  string
	Warning       string
	Retry         bool
}

// Returns the domain name of the institution that owns the specified bucket.
// For example, if bucketName is 'aptrust.receiving.unc.edu' the return value
// will be 'unc.edu'.
func OwnerOf(bucketName string) (institution string) {
	if strings.HasPrefix(bucketName, ReceiveBucketPrefix) {
		institution = strings.Replace(bucketName, ReceiveBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, RestoreBucketPrefix) {
		institution = strings.Replace(bucketName, RestoreBucketPrefix, "", 1)
	}
	return institution
}

// Returns the name of the specified institution's receiving bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.receiving.unc.edu'
func ReceivingBucketFor(institution string) (bucketName string) {
	return ReceiveBucketPrefix + institution
}

// Returns the name of the specified institution's restoration bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.restore.unc.edu'
func RestorationBucketFor(institution string) (bucketName string) {
	return RestoreBucketPrefix + institution
}

// Given the name of a tar file, returns the clean bag name. That's
// the tar file name minus the tar extension and any ".bagN.ofN" suffix.
func CleanBagName(bagName string) (string, error) {
	if len(bagName) < 5 {
		return "", fmt.Errorf("'%s' is not a valid tar file name", bagName)
	}
	// Strip the .tar suffix
	nameWithoutTar := bagName[0:len(bagName)-4]
	// Now get rid of the .b001.of200 suffix if this is a multi-part bag.
	cleanName := MultipartSuffix.ReplaceAll([]byte(nameWithoutTar), []byte(""))
	return string(cleanName), nil
}

// MetadataRecord describes the result of an attempt to record metadata
// in Fluctus/Fedora.
type MetadataRecord struct {
	// Type describes what we're trying to record in Fedora. It can
	// be "IntellectualObject", "GenericFile", or "PremisEvent"
	Type string
	// Action contains information about what was in Fedora.
	// For Type IntellectualObject, this will be "object_registered".
	// For Type GenericFile, this will be "file_registered".
	// For Type PremisEvent, this will be the name of the event:
	// "ingest", "identifier_assignment", or "fixity_generation".
	Action string
	// For actions or events pertaining to a GenericFile this will be the path
	// of the file the action pertains to. For example, for fixity_generation
	// on the file "data/images/aerial.jpg", the EventObject would be
	// "data/images/aerial.jpg". For actions or events pertaining to the
	// IntellectualObject, this will be the IntellectualObject identifier.
	EventObject string
	// ErrorMessage contains a description of the error that occurred
	// when we tried to save this bit of metadata in Fluctus/Fedora.
	// It will be empty if there was no error, or if we have not yet
	// attempted to save the item.
	ErrorMessage string
}

// Returns true if this bit of metadata was successfully saved to Fluctus/Fedora.
func (record *MetadataRecord) Succeeded() bool {
	return record.ErrorMessage == ""
}

// FedoraResult is a collection of MetadataRecords, each indicating
// whether or not some bit of metadata has been recorded in Fluctus/Fedora.
// The bag processor needs to keep track of this information to ensure
// it successfully records all metadata in Fedora.
type FedoraResult struct {
	ObjectIdentifier string
	GenericFilePaths []string
	MetadataRecords  []*MetadataRecord
	IsNewObject      bool
	ErrorMessage     string
}

// Creates a new FedoraResult object with the specified IntellectualObject
// identifier and list of GenericFile paths.
func NewFedoraResult(objectIdentifier string, genericFilePaths []string) *FedoraResult {
	return &FedoraResult{
		ObjectIdentifier: objectIdentifier,
		GenericFilePaths: genericFilePaths,
		IsNewObject:      true,
	}
}

// AddRecord adds a new MetadataRecord to the Fedora result.
func (result *FedoraResult) AddRecord(recordType, action, eventObject, errorMessage string) error {
	if recordType != "IntellectualObject" && recordType != "GenericFile" && recordType != "PremisEvent" {
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
		Type:         recordType,
		Action:       action,
		EventObject:  eventObject,
		ErrorMessage: errorMessage,
	}
	result.MetadataRecords = append(result.MetadataRecords, record)
	return nil
}

// FindRecord returns the MetadataRecord with the specified type,
// action and event object.
func (result *FedoraResult) FindRecord(recordType, action, eventObject string) *MetadataRecord {
	for _, record := range result.MetadataRecords {
		if record.Type == recordType && record.Action == action && record.EventObject == eventObject {
			return record
		}
	}
	return nil
}

// Returns true/false to indicate whether the specified bit of
// metadata was recorded successfully in Fluctus/Fedora.
func (result *FedoraResult) RecordSucceeded(recordType, action, eventObject string) bool {
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
func (result *FedoraResult) AllRecordsSucceeded() bool {
	for _, record := range result.MetadataRecords {
		if false == record.Succeeded() {
			return false
		}
	}
	return true
}

// Sends the JSON of a result object to the specified queue.
func Enqueue(nsqdHttpAddress, topic string, result *ProcessResult) error {
	key := result.S3File.Key.Key
	url := fmt.Sprintf("%s/put?topic=%s", nsqdHttpAddress, topic)
	json, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("Error marshalling result for '%s' to JSON for file: %v", key, err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(json))

	if err != nil {
		return fmt.Errorf("Nsqd returned an error when queuing '%s': %v", key, err)
	}
	if resp == nil {
		return fmt.Errorf("No response from nsqd at '%s'. Is it running?", url)
	}

	// nsqd sends a simple OK. We have to read the response body,
	// or the connection will hang open forever.
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyText := "[no response body]"
		if len(body) > 0 {
			bodyText = string(body)
		}
		return fmt.Errorf("nsqd returned status code %d when attempting to queue %s. " +
			"Response body: %s",
			resp.StatusCode, key, bodyText)
	}
	return nil
}

// CleanupFile represents a file (key) to be deleted
// from an S3 bucket.
type CleanupFile struct {
	// The name of the bucket that contains the key
	// we want to delete.
	BucketName string
	// The key to delete from S3.
	Key string
	// The error message, if the attempt
	// to delete the key resulted in an error.
	ErrorMessage string
	// The date and time at which the key/file
	// was successfully deleted from S3. If this
	// is zero time, file was not deleted. If it's
	// any other time, delete succeeded.
	DeletedAt time.Time
}

// Returns true if delete was attempted.
func (file *CleanupFile) DeleteAttempted() bool {
	return file.ErrorMessage != "" || file.DeletedAt.IsZero() == false
}

// CleanupResult describes one or more files to be
// deleted from S3, and the result of the attempts
// to delete those files. The combination of BagName +
// ETag + BagDate maps to a unique entry in Fluctus'
// ProcessedItems table.
type CleanupResult struct {
	// The NSQ message from nsqd
	NsqMessage *nsq.Message `json:"-"` // Don't serialize
	// The S3 key of the original bag file. This will
	// be in one of the receiving buckets. This is not
	// necessarily the file we'll be deleting, but all
	// files to be deleted are related to this bag.
	BagName string
	// The ETag of the original uploaded bag (minus the
	// quotes). This is the bag's md5 sum for bags under
	// about 2GB.
	ETag string
	// The modified date of the original bag.
	BagDate time.Time
	// The identifier of the intellectual object to which
	// the Files belong. This may be an empty string in
	// cases where we're cleaning up files from a bag that
	// failed ingest. If it's not null, the bag was successfully
	// ingested, and the identifier will look something like
	// virginia.edu/bag_name
	ObjectIdentifier string
	// Files contains a list of files/keys to be deleted
	// from S3.
	Files []*CleanupFile
}

// Returns true if all files were successfully deleted.
func (result *CleanupResult) Succeeded() bool {
	for _, file := range result.Files {
		if file.DeleteAttempted() == false || file.DeletedAt.IsZero() == true {
			return false
		}
	}
	return true
}
