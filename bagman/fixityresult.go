package bagman

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"time"
	"strings"
)


// FixityResult descibes the results of fetching a file from S3
// and verification of the file's md5 and sha256 checksums.
type FixityResult struct {

	// The generic file we're going to look at.
	// This file is sitting somewhere on S3.
	GenericFile   *GenericFile

	// Does the file exist in S3?
	S3FileExists  bool

	// The md5 sum we calculated after downloading
	// the file.
	Md5           string

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
		result.ErrorMessage = fmt.Sprintf("GenericFile URI '%s' is invalid", result.GenericFile.URI)
		return "","", fmt.Errorf(result.ErrorMessage)
	}
	bucket := parts[length - 2]
	key := parts[length - 1]
	return bucket, key, nil
}

// Returns true if the md5 sum we calculated for this file
// matches the md5 sum recorded in Fedora.
func (result *FixityResult) Md5Matches() (bool) {
	return result.checksumMatches("md5")
}

// Returns true if the sha256 sum we calculated for this file
// matches the sha256 sum recorded in Fedora.
func (result *FixityResult) Sha256Matches() (bool) {
	return result.checksumMatches("sha256")
}

func (result *FixityResult) checksumMatches(algorithm string) (bool) {
	currentDigest := result.Md5
	fedoraChecksum := result.GenericFile.GetChecksum("md5")
	if algorithm == "sha256" {
		currentDigest = result.Sha256
		fedoraChecksum = result.GenericFile.GetChecksum("sha256")
	}
	if currentDigest == "" {
		result.ErrorMessage = fmt.Sprintf("FixityResult object is missing %s digest!", algorithm)
		return false
	}
	if fedoraChecksum == nil {
		result.ErrorMessage = fmt.Sprintf("GenericFile record from Fedora is missing %s digest!", algorithm)
		return false
	}
	if fedoraChecksum.Digest != currentDigest {
		result.ErrorMessage = fmt.Sprintf(
			"Current %s digest '%s' does not match Fedora digest '%s'",
			algorithm, currentDigest, fedoraChecksum.Digest)
		return false
	}
	return true
}

// Returns a PremisEvent describing the result of this fixity check.
func (result *FixityResult) BuildPremisEvent() (*PremisEvent, error) {
	detail := "Fixity check against registered hash"
	outcome := "success"
	outcomeInformation := "Fixity matches"
	ok := result.Sha256Matches()
	if ok == false {
		detail = "Fixity does not match expected value"
		outcome = "failure"
		outcomeInformation = result.ErrorMessage
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
