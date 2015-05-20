package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os"
	"strings"
	"time"
)

const (
	STAGE_PACKAGE  = "Packaging"
	STAGE_RECEIVE  = "Receiving"
	STAGE_VALIDATE = "Validation"
	STAGE_STORE    = "Storage"
	STAGE_RECORD   = "Recording"
)

type DPNResult struct {
	// BagIdentifier is the APTrust bag identifier, composed of
	// the institution domain name, a slash, and the institution's
	// internal bag identifier. E.g. "test.edu/ncsu.1840.16-1004"
	// For bags coming from other nodes, this will be blank.
	BagIdentifier    string

	// LocalPath is where this bag is stored on disk. The bag
	// may be a file ending in .tar or a directory if the bag
	// is not tarred.
	LocalPath        string

	// The NSQ message being processed. May be nil if we're
	// running tests.
	NsqMessage       *nsq.Message  `json:"-"`

	// The current stage of processing for this bag.
	Stage            string

	// A general error message describing what went wrong with
	// processing. More specific errors will appear in the
	// PackageResult, StorageResult or ValidationResult, depending
	// on the stage where processing failed. If this is empty,
	// there was no error.
	ErrorMessage     string

	// The DPN bag record for this object. This will be nil for
	// bags ingested through APTrust and in the packaging stage,
	// since the bag won't have a UUID until after it's packaged.
	DPNBag           *DPNBag

	// The DPN transfer request associated with this bag. This will
	// be nil if it's a bag created at our own node. It will be
	// non-nil for bags we're replicating from other nodes.
	TransferRequest  *DPNReplicationTransfer

	// The result of the attempt to package this object as a DPN
	// bag. We only package APTrust bags that we ingested and that
	// the depositor has indicated should go to DPN. Bags we
	// replicate from other nodes will already have been packaged
	// by the ingesting node, so  the PackageResult for those will
	// be empty.
	PackageResult    *PackageResult

	// The result of the attempt to store the bag in the long-term
	// storage bucket for DPN.
	StorageResult    *StorageResult

	// The result of the attempt to validate the bag. This includes
	// information about whether the bag's structure is valid, whether
	// all required tags are present, checksums checked out, etc.
	ValidationResult *ValidationResult

	// Indicates whether we should try to process this bag again.
	// For transient problems, such as network outages and lack of
	// disk space, this will be true. For fatal problems, such as
	// an invalid bag, this will be false.
	Retry            bool
}

func NewDPNResult(bagIdentifier string) (*DPNResult) {
	// Note that DPNBag and ValidationResult are not
	// initialized, so they are nil to begin with.
	return &DPNResult{
		BagIdentifier: bagIdentifier,
		Stage: STAGE_PACKAGE,
		PackageResult: &PackageResult{},
		StorageResult: &StorageResult{},
		Retry: true,
	}
}

func (result *DPNResult) OriginalBagName() (string, error) {
	parts := strings.SplitN(result.BagIdentifier, "/", 2)
	if len(parts) == 2 {
		return parts[1], nil
	}
	err := fmt.Errorf("BagIdentifier '%s' does not conform to " +
		"expected format of domain/bag_name.", result.BagIdentifier)
	return "", err
}

// PackageResult maintains information about the state of the
// packaging process. This struct is passed from channel to channel,
// accumulating information along the way. If packaging fails after
// max attempts, this struct will be dumped into the trouble queue
// as JSON.
type PackageResult struct {
	BagBuilder      *BagBuilder
	DPNFetchResults []*DPNFetchResult
	TarFilePath     string
	ErrorMessage    string
}

func (result *PackageResult) Errors() ([]string) {
	errors := make([]string, 0)
	if result.ErrorMessage != "" {
		errors = append(errors, result.ErrorMessage)
	}
	if result.BagBuilder.ErrorMessage != "" {
		errors = append(errors, result.BagBuilder.ErrorMessage)
	}
	for _, result := range result.DPNFetchResults {
		if result.FetchResult.ErrorMessage != "" {
			errors = append(errors, result.FetchResult.ErrorMessage)
		}
	}
	return errors
}

func (result *PackageResult) Succeeded() (bool) {
	return result.TarFilePath != "" && len(result.Errors()) == 0
}

// StorageResult maintains information about the state of
// an attempt to store a DPN bag in AWS Glacier.
type StorageResult struct {
	// The URL of this file in Glacier. This will be empty until
	// we actually manage to store the file.
	StorageURL      string

	// The file's md5 digest. We need this to copy to Amazon S3/Glacier.
	Md5Digest       string

	// Digest to be recorded in DPN.
	Sha256Digest    string
}

// DefaultMetadata includes mostly static information about bags
// that APTrust packages for DPN. You can specify this information
// in a json config file and load it with LoadConfig.
type DefaultMetadata struct {
	Comment                string
	BagItVersion           string
	BagItEncoding          string
	IngestNodeName         string
	IngestNodeAddress      string
	IngestNodeContactName  string
	IngestNodeContactEmail string
}

type RestClientConfig struct {
	Comment                string
	LocalServiceURL        string
	LocalAPIRoot           string
	LocalAuthToken         string
}

type DPNConfig struct {
	// LocalNode is the namespace of the node this code is running on.
	// E.g. "aptrust", "chron", "hathi", "tdr", "sdr"
	LocalNode             string
	// Default metadata that goes into bags produced at our node.
	DefaultMetadata       *DefaultMetadata
	// Settings for connecting to our own REST service
	RestClient            *RestClientConfig
	// API Tokens for connecting to remote nodes
	RemoteNodeTokens      map[string]string
}

func LoadConfig(pathToFile string) (*DPNConfig, error) {
	data, err := bagman.LoadRelativeFile(pathToFile)
	if err != nil {
		return nil, err
	}
	config := DPNConfig{}
	err = json.Unmarshal(data, &config)
    if err != nil {
        return nil, err
    }
	// Load local API token from environment to keep it out of config file.
	// Need a better solution for this.
	if config.RestClient.LocalAuthToken == "" {
		config.RestClient.LocalAuthToken = os.Getenv("DPN_REST_TOKEN")
	}
	// Add local node token into the map of remote node tokens if we can,
	// because in some of our integration tests, we do local-to-local
	// transfers, and the code in validator.go will look in this map
	// for the API token of the transfer from_node. In short, we really
	// only need to do this for testing.
	if config.RemoteNodeTokens[config.LocalNode] == "" && config.RestClient.LocalAuthToken != "" {
		config.RemoteNodeTokens[config.LocalNode] = config.RestClient.LocalAuthToken
	}
    return &config, nil
}


// BagBuilder builds a DPN bag from an APTrust intellectual object.
type BagBuilder struct {
	// LocalPath is the full, absolute path the the untarred bag
	// the builder will create. It will end with the bag's UUID,
	// so it should look something like this:
	// /mnt/dpn/bags/00000000-0000-0000-0000-000000000000.
	LocalPath              string

	// IntellectualObject is the APTrust IntellectualObject that
	// we'll be repackaging as a DPN bag.
	IntellectualObject     *bagman.IntellectualObject

	// DefaultMetadata is some metadata that goes into EVERY DPN
	// bag we create. This includes our name and address in the
	// DPN data section that describes who packaged the bag.
	// DefaultMetadata should be loaded from a JSON file using
	// the dpn.LoadConfig() function.
	DefaultMetadata        *DefaultMetadata

	// UUID is the DPN identifier for this bag. This has nothing to
	// do with any APTrust UUID. It's generated in the constructor.
	UUID                   string

	// ErrorMessage describes what when wrong while trying to
	// package this bag. If it's an empty string, packaging
	// succeeded.
	ErrorMessage           string

	bag                    *Bag
	bagtime                time.Time
}
