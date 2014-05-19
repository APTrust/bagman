// This file contains data structures that are shared by several sub-programs.

package bagman

import (
	"time"
	"launchpad.net/goamz/s3"
)


// S3File contains information about the S3 file we're
// trying to process from an intake bucket. BucketName
// and Key are the S3 bucket name and key. AttemptNumber
// describes whether this is the 1st, 2nd, 3rd,
// etc. attempt to process this file.
type S3File struct {
	BucketName     string
	Key            s3.Key
	AttemptNumber  int
}

// Retry will be set to true if the attempt to process the file
// failed and should be tried again. This would be case, for example,
// if the failure was due to a network error. Retry is
// set to false if processing failed for some reason that
// will not change: for example, if the file cannot be
// untarred, checksums were bad, or data files were missing.
// If processing succeeded, Retry is irrelevant.
type ProcessResult struct {
	S3File         *S3File
	Error          error
	FetchResult    *FetchResult
	TarResult      *TarResult
	BagReadResult  *BagReadResult
	Retry          bool
}

// BucketSummary contains information about an S3 bucket and its contents.
type BucketSummary struct {
	BucketName     string
	Keys           []s3.Key
	MaxFileSize    int64
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
