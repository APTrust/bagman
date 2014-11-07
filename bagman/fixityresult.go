package bagman

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/nu7hatch/gouuid"
	"time"
	"strings"
)


// FixityResult descibes the results of fetching a file from S3
// and verification of the file's sha256 checksum.
type FixityResult struct {

	// The generic file we're going to look at.
	// This file is sitting somewhere on S3.
	GenericFile   *GenericFile

	// The NSQ message being worked on. This is only relevant
	// if we using this struct in the context of an NSQ worker.
	// The struct is still valid if this member is nil.
	NsqMessage    *nsq.Message `json:"-"` // Don't serialize

	// Does the file exist in S3?
	S3FileExists  bool

	// The sha256 sum we calculated after downloading
	// the file.
	Sha256        string

	// The date and time at which we finished calculating
	// the md5 and sha256 checksums.
	CalculatedAt  time.Time

	// A string describing any error that might have
	// occurred during the fetch and/or checksum calculation.
	ErrorMessage  string

	// Should we retry the fixity check if the last attempt
	// failed? Typically, this will be true, because most
	// failures are transient network errors. It will be
	// false on fatal errors, such as if the remote file
	// does not exist.
	Retry         bool
}


func NewFixityResult(gf *GenericFile) (*FixityResult) {
	return &FixityResult {
		GenericFile: gf,
		S3FileExists: true,
		Retry: true,
	}
}

// Returns the name of the S3 bucket and key for the GenericFile.
func (result *FixityResult) BucketAndKey() (string, string, error) {
	parts := strings.Split(result.GenericFile.URI, "/")
	length := len(parts)
	if length < 4 {
		// This error is fatal, so don't retry.
		result.ErrorMessage = fmt.Sprintf("GenericFile URI '%s' is invalid", result.GenericFile.URI)
		result.Retry = false
		return "","", fmt.Errorf(result.ErrorMessage)
	}
	bucket := parts[length - 2]
	key := parts[length - 1]
	return bucket, key, nil
}

// Returns true if result.Sha256 was set.
func (result *FixityResult) GotDigestFromPreservationFile() (bool) {
	return result.Sha256 != ""
}

// Returns true if the underlying GenericFile includes a SHA256 checksum.
func (result *FixityResult) GenericFileHasDigest() (bool) {
	return result.FedoraSha256() != ""
}

// Returns the SHA256 checksum that Fedora has on record.
func (result *FixityResult) FedoraSha256() (string) {
	checksum := result.GenericFile.GetChecksum("sha256")
	if checksum == nil {
		return ""
	}
	return checksum.Digest
}

// Returns true if we have all the data we need to compare the
// existing checksum with the checksum of the S3 file.
func (result *FixityResult) FixityCheckPossible() (bool) {
	return result.GotDigestFromPreservationFile() && result.GenericFileHasDigest()
}

// Returns true if the sha256 sum we calculated for this file
// matches the sha256 sum recorded in Fedora.
func (result *FixityResult) Sha256Matches() (bool, error) {
	if result.FixityCheckPossible() == false {
		return false, fmt.Errorf("Fixity check is not possible because one or more checksums are not available.")
	}
	return result.FedoraSha256() == result.Sha256, nil
}

// Returns a PremisEvent describing the result of this fixity check.
func (result *FixityResult) BuildPremisEvent() (*PremisEvent, error) {
	detail := "Fixity check against registered hash"
	outcome := "success"
	outcomeInformation := "Fixity matches"
	ok, err := result.Sha256Matches()
	if err != nil {
		return nil, err
	}
	if ok == false {
		detail = "Fixity does not match expected value"
		outcome = "failure"
		outcomeInformation = fmt.Sprintf("Expected digest '%s', got '%s'",
			result.FedoraSha256(), result.Sha256)
	}

	youyoueyedee, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for fixity check event: %v", err)
		return nil, detailedErr
	}

	premisEvent := &PremisEvent {
		Identifier: youyoueyedee.String(),
		EventType: "fixity_check",
		DateTime: time.Now().UTC(),
		Detail: detail,
		Outcome: outcome,
		OutcomeDetail: result.Sha256,
		Object: "Go language cryptohash",
		Agent: "http://golang.org/pkg/crypto/sha256/",
		OutcomeInformation: outcomeInformation,
	}

	return premisEvent, nil
}
