// This file contains data structures that are shared by several sub-programs.

package bagman

import (
	"time"
	"strings"
	"launchpad.net/goamz/s3"
	"github.com/bitly/go-nsq"
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
	Name         string      `json:"name"`
	Bucket       string      `json:"bucket"`
	ETag         string      `json:"etag"`
	BagDate      time.Time   `json:"bag_date"`
	UserId       int         `json:"user_id"`
	Institution  string      `json:"institution"`
	Date         time.Time   `json:"date"`
	Note         string      `json:"note"`
	Type         string      `json:"type"`
	Stage        string      `json:"stage"`
	Status       string      `json:"status"`
	Outcome      string      `json:"outcome"`
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
	NsqOutputChannel chan *nsq.FinishedMessage  `json:"-"`  // Don't serialize
	S3File           *S3File
	Error            error
	FetchResult      *FetchResult
	TarResult        *TarResult
	BagReadResult    *BagReadResult
	Stage            string
	Retry            bool
}

// IngestStatus returns a lightweight Status object suitable for reporting
// to the Fluctus results table, so that APTrust partners can view
// the status of their submitted bags.
func (result *ProcessResult) IngestStatus() (status *ProcessStatus) {
	status = &ProcessStatus{}
	status.Date = time.Now()
	status.Type = "Ingest"
	status.Name = result.S3File.Key.Key
	bagDate, _ := time.Parse(S3DateFormat, result.S3File.Key.LastModified)
	status.BagDate = bagDate
	status.Bucket = result.S3File.BucketName
	status.ETag = result.S3File.Key.ETag
	status.Stage = result.Stage
	status.Status = "Processing"
	if result.Error != nil {
		status.Note = result.Error.Error()
		status.Status = "Failed"
	} else {
		if result.Stage == "Record" {
			// We made it through last stage with no erros
			status.Status = "Succeeded"
		}
	}
	status.Institution = OwnerOf(result.S3File.BucketName)
	status.Outcome = ""
	return status
}

// BucketSummary contains information about an S3 bucket and its contents.
type BucketSummary struct {
	BucketName     string
	Keys           []s3.Key
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
	Error            error
}

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
	InputFile       string
	OutputDir       string
	Error           error
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
	Error            error
	Tags             []Tag
	ChecksumErrors   []error
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
	Error            error
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
