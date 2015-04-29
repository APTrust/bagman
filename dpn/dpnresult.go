package dpn

import (
	"encoding/json"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"os"
	"time"
)

const (
	STAGE_PACKAGE = "Packaging"
	STAGE_STORE   = "Storage"
	STAGE_RECORD  = "Recoding"
)

type DPNResult struct {
	BagIdentifier   string
	NsqMessage      *nsq.Message  `json:"-"`
	Stage           string
	ErrorMessage    string
	PackageResult   *PackageResult
	StorageResult   *StorageResult
	Retry           bool
}

func NewDPNResult(bagIdentifier string) (*DPNResult) {
	return &DPNResult{
		BagIdentifier: bagIdentifier,
		Stage: STAGE_PACKAGE,
		PackageResult: &PackageResult{},
		StorageResult: &StorageResult{},
		Retry: true,
	}
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
	DefaultMetadata       *DefaultMetadata
	RestClient            *RestClientConfig
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
	if config.RestClient.LocalAuthToken == "" {
		config.RestClient.LocalAuthToken = os.Getenv("DPN_REST_TOKEN")
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
