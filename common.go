// This file contains data structures that are shared by several sub-programs.

package bagman

import (
    "fmt"
    "time"
    "strings"
    "encoding/json"
    "launchpad.net/goamz/s3"
    "github.com/bitly/go-nsq"
    "github.com/APTrust/bagman/fluctus/models"
)

const (
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
    Stage            string
    Retry            bool
}

// IntellectualObject returns an instance of models.IntellectualObject
// which describes what was unpacked from the bag. The IntellectualObject
// structure matches Fluctus' IntellectualObject model, and can be sent
// directly to Fluctus for recording.
func (result *ProcessResult) IntellectualObject() (obj *models.IntellectualObject) {
    accessRights := result.BagReadResult.TagValue("Access")
    if accessRights == "" {
        accessRights = result.BagReadResult.TagValue("Rights")
    }
    institution := &models.Institution{
        BriefName: OwnerOf(result.S3File.BucketName),
    }
    // For now, object identifier is institution domain, plus the name
    // of the tar file, with ".tar" truncated.
    identifier := fmt.Sprintf("%s.%s",
        institution.BriefName,
        result.S3File.Key.Key[0:len(result.S3File.Key.Key)-4])
    return &models.IntellectualObject{
		// TODO: Use proper institution id
        InstitutionId: institution.BriefName,
        Title: result.BagReadResult.TagValue("Title"),
        Description: result.BagReadResult.TagValue("Description"),
        Identifier: identifier,
        Access: accessRights,
		GenericFiles: result.GenericFiles(),
    }
}

// GenericFiles returns a list of GenericFile objects that were found
// in the bag.
func (result *ProcessResult) GenericFiles() (files []*models.GenericFile) {
    files = make([]*models.GenericFile, len(result.TarResult.GenericFiles))
    for i, file := range(result.TarResult.GenericFiles) {
        checksumAttributes := make([]*models.ChecksumAttribute, 2)
        checksumAttributes[0] = &models.ChecksumAttribute{
            Algorithm: "md5",
            DateTime: file.Modified,
            Digest: file.Md5,
        }
        checksumAttributes[1] = &models.ChecksumAttribute{
            Algorithm: "sha256",
            DateTime: file.Sha256Generated,
            Digest: file.Sha256,
        }
        files[i] = &models.GenericFile{
            URI: file.StorageURL,
            Size: file.Size,
            Created: file.Modified,
            Modified: file.Modified,
            ChecksumAttributes: checksumAttributes,
            Events: PremisEvents(file),
        }
    }
    return files
}

// PremisEvents returns a list of Premis events generated during bag
// processing. Ingest, Fixity Generation (sha256), identifier
// assignment.
func PremisEvents(gf *GenericFile) (events []*models.PremisEvent) {
    events = make([]*models.PremisEvent, 3)
    // Ingest
    // TODO: Actual timestamp and handle success/failure
    events[0] = &models.PremisEvent{
        EventType: "Ingest",
        DateTime: time.Time{},
        Detail: "Completed copy to S3",
        Outcome: "Success",
        OutcomeDetail: "s3 md5 digest here",
        Object: "bagman + goamz s3 client",
        Agent: "https://github.com/APTrust/bagman",
        OutcomeInformation: "Put using md5 checksum",
    }
    // Fixity Generation (sha256)
    events[1] = &models.PremisEvent{
        EventType: "Fixity Generation",
        DateTime: gf.Sha256Generated,
        Detail: "Calculated new fixity value",
        Outcome: "Success",
        OutcomeDetail: gf.Sha256,
        Object: "Go language crypto/sha256",
        Agent: "http://golang.org/pkg/crypto/sha256/",
        OutcomeInformation: "",
    }
    // Identifier assignment
    events[2] = &models.PremisEvent{
        EventType: "Identifier Assignment",
        DateTime: gf.UuidGenerated,
        Detail: "Assigned new identifier",
        Outcome: "Success",
        OutcomeDetail: gf.Uuid,
        Object: "Go language UUID generator",
        Agent: "http://github.com/nu7hatch/gouuid",
        OutcomeInformation: "",
    }
    return events
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
    Path             string
    Size             int64
    Created          time.Time  // we currently have no way of getting this
    Modified         time.Time
    Md5              string
    Sha256           string
    Sha256Generated  time.Time
    Uuid             string
    UuidGenerated    time.Time
    MimeType         string
    ErrorMessage     string
    StorageURL       string
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
