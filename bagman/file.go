package bagman

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"time"
)

// File contains information about a generic
// data file within the data directory of bag or tar archive.
type File struct {
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

func NewFile() (*File) {
	return &File{
		ExistingFile: false,
		NeedsSave: true,
	}
}


// Converts bagman.File to GenericFile, which is what
// Fluctus understands.
func (file *File) ToGenericFile() (*GenericFile, error) {
	checksumAttributes := make([]*ChecksumAttribute, 2)
	checksumAttributes[0] = &ChecksumAttribute{
		Algorithm: "md5",
		DateTime:  file.Modified,
		Digest:    file.Md5,
	}
	checksumAttributes[1] = &ChecksumAttribute{
		Algorithm: "sha256",
		DateTime:  file.Sha256Generated,
		Digest:    file.Sha256,
	}
	events, err := file.PremisEvents()
	if err != nil {
		return nil, err
	}
	genericFile := &GenericFile{
		Identifier:         file.Identifier,
		Format:             file.MimeType,
		URI:                file.StorageURL,
		Size:               file.Size,
		Created:            file.Modified,
		Modified:           file.Modified,
		ChecksumAttributes: checksumAttributes,
		Events:             events,
	}
	return genericFile, nil
}

// PremisEvents returns a list of Premis events generated during bag
// processing. Ingest, Fixity Generation (sha256), identifier
// assignment.
func (file *File) PremisEvents() (events []*PremisEvent, err error) {
	events = make([]*PremisEvent, 5)
	// Fixity check
	fCheckEventUuid, err := uuid.NewV4()
	if err != nil {
		detailedErr := fmt.Errorf("Error generating UUID for fixity check event: %v", err)
		return nil, detailedErr
	}
	// Fixity check event
	events[0] = &PremisEvent{
		Identifier:         fCheckEventUuid.String(),
		EventType:          "fixity_check",
		DateTime:           file.Md5Verified,
		Detail:             "Fixity check against registered hash",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("md5:%s", file.Md5),
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
	events[1] = &PremisEvent{
		Identifier:         ingestEventUuid.String(),
		EventType:          "ingest",
		DateTime:           file.StoredAt,
		Detail:             "Completed copy to S3",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      file.StorageMd5,
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
	events[2] = &PremisEvent{
		Identifier:         fixityGenUuid.String(),
		EventType:          "fixity_generation",
		DateTime:           file.Sha256Generated,
		Detail:             "Calculated new fixity value",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("sha256:%s", file.Sha256),
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
	events[3] = &PremisEvent{
		Identifier:         idAssignmentUuid.String(),
		EventType:          "identifier_assignment",
		DateTime:           file.UuidGenerated,
		Detail:             "Assigned new institution.bag/path identifier",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      file.Identifier,
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
	events[4] = &PremisEvent{
		Identifier:         urlAssignmentUuid.String(),
		EventType:          "identifier_assignment",
		DateTime:           file.UuidGenerated,
		Detail:             "Assigned new storage URL identifier",
		Outcome:            string(StatusSuccess),
		OutcomeDetail:      file.StorageURL,
		Object:             "Go uuid library + goamz S3 library",
		Agent:              "http://github.com/nu7hatch/gouuid",
		OutcomeInformation: "",
	}
	return events, nil
}
