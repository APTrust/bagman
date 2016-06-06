package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"github.com/op/go-logging"
	"os"
	"strings"
	"time"
)

const (
	STAGE_PRE_COPY  = "Pre Copy"
	STAGE_COPY      = "Copying from ingest node"
	STAGE_PACKAGE   = "Packaging"
	STAGE_RECEIVE   = "Receiving"
	STAGE_VALIDATE  = "Validation"
	STAGE_STORE     = "Storage"
	STAGE_RECORD    = "Record"
	STAGE_COMPLETE  = "Complete"
	STAGE_CANCELLED = "Cancelled"

	DEFAULT_TOKEN_FORMAT_STRING = "token %s"

	BAG_TYPE_DATA = "data"
	BAG_TYPE_RIGHTS = "rights"
	BAG_TYPE_INTERPRETIVE = "interpretive"

	PATH_TYPE_LOCAL = "Local Filesystem"
	PATH_TYPE_S3    = "S3 Bucket"

	// These values are part of the published APTrust spec.
	APTRUST_BAGIT_VERSION = "0.97"
	APTRUST_BAGIT_ENCODING = "UTF-8"
)


type DPNResult struct {
	// BagIdentifier is the APTrust bag identifier, composed of
	// the institution domain name, a slash, and the institution's
	// internal bag identifier. E.g. "test.edu/ncsu.1840.16-1004"
	// For bags coming from other nodes, this will be blank.
	BagIdentifier    string

	// If this is an APTrust bag that we're ingesting into DPN,
	// this is the ID of the "send to DPN" request in the
	// ProcessedItem queue. For bags being replicated from
	// other nodes, this will be nil.
	ProcessedItemId  int

	// Private, transient copy of Fluctus ProcessStatus
	processStatus    *bagman.ProcessStatus

	// LocalPath is where this bag is stored on disk. The bag
	// may be a file ending in .tar or a directory if the bag
	// is not tarred.
	LocalPath        string

	// The bag's md5 digest. We need this to copy to Amazon S3/Glacier.
	BagMd5Digest     string

	// Sha256Digest of the tarred bag file. No longer used.
	BagSha256Digest  string

	// Sha256 digest of the tagmanifest-sha256.txt file. We use this
	// to verify that the bag was correctly copied by replicating
	// nodes.
	TagManifestDigest string

	// The size of the bag. Used when copying to S3/Glacier.
	BagSize          int64

	// The NSQ message being processed. May be nil if we're
	// running tests.
	NsqMessage       *nsq.Message                 `json:"-"`

	// The current stage of processing for this bag.
	Stage            string

	// A general error message describing what went wrong with
	// processing. More specific errors will appear in the
	// PackageResult or ValidationResult, depending
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

	// The results of attempts to fetch the S3 files that make up this
	// bag. This will be nil for bags that we're replicating from
	// other nodes, because in those cases, we just copy the whole
	// tar file from the remote node. For bags ingested at APTrust,
	// this should contain a number of records, once we've reached
	// the fetch stage of the process. Before that stage, it will
	// be nil.
	//
	// It would be nice to serialize this, but we wind up with
	// a lot of JSON.
	FetchResults     *FetchResultCollection         `json:"-"`

	// The result of the attempt to package this object as a DPN
	// bag. We only package APTrust bags that we ingested and that
	// the depositor has indicated should go to DPN. Bags we
	// replicate from other nodes will already have been packaged
	// by the ingesting node, so the PackageResult for those will
	// be nil. On successful copy, check DPNResult.LocalPath to
	// find where we stored the file.
	PackageResult    *PackageResult

	// The result of the attempt to copy the bag from its admin
	// or ingest node (typically ingest node). When a remote
	// node asks us to replicate a bag, we have to copy it from the
	// remote node to our staging area, usually via rsync. This
	// structure records the result of that copy. For bags that we
	// created at APTrust, this will be nil because we don't have
	// to copy from ourselves.
	CopyResult       *CopyResult

	// The URL of this item in long-term storage. This will be an
	// AWS S3 or Glacier URL. An empty string indicates the bag
	// has not yet been copied to storage.
	StorageURL       string

	// The result of the attempt to record information about the bag
	// in DPN and in APTrust. This object is defined in recorder.go.
	RecordResult     *RecordResult

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
		CopyResult: &CopyResult{},
		RecordResult: NewRecordResult(),
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

func (result *DPNResult) TarFilePath() (string) {
	// Locally ingested bags have a PackageResult...
	if result.PackageResult != nil && result.PackageResult.TarFilePath != "" {
		return result.PackageResult.TarFilePath
	}
	// Bags replicated from other nodes have a CopyResult...
	if result.CopyResult != nil && result.CopyResult.LocalPath != "" {
		return result.CopyResult.LocalPath
	}
	// This bag is messed up
	return ""
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
	// EnvironmentName is the name of this configuration: "dev", "test",
	// "production", etc.
	EnvironmentName        string
	// LocalNode is the namespace of the node this code is running on.
	// E.g. "aptrust", "chron", "hathi", "tdr", "sdr"
	LocalNode              string
	// Where should DPN service logs go?
	LogDirectory           string
	// Log level (4 = debug)
	LogLevel               logging.Level
	// Should we log to Stderr in addition to writing to
	// the log file?
	LogToStderr            bool
	// Number of nodes we should replicate bags to.
	ReplicateToNumNodes    int
	// Should we accept self-signed and otherwise invalid SSL
	// certificates? We need to do this in testing, but it
	// should not be allowed in production. Bools in Go default
	// to false, so if this is not set in config, we should be
	// safe.
	AcceptInvalidSSLCerts  bool
	// When copying bags from remote nodes, should we use rsync
	// over SSH (true) or just plain rsync (false)?
	UseSSHWithRsync        bool
	// Default metadata that goes into bags produced at our node.
	DefaultMetadata        *DefaultMetadata
	// Settings for connecting to our own REST service
	RestClient             *RestClientConfig
	// Standard Auth token header format for REST services
	// is "token %s", where "%s" will be the token. Rails
	// REST services require the format "Token token=%s".
	// This map of formats lets us override the standard
	// "token %s" with whatever the remote REST service
	// expects. Since the default is "token %s", there is no
	// need to create entries in this map for most nodes.
	AuthTokenHeaderFormats map[string]string
	// RemoteNodeAdminTokensForTesting are used in integration
	// tests only, when we want to perform admin-only operations,
	// such as creating bags and replication requests on a remote
	// node in the test cluster.
	RemoteNodeAdminTokensForTesting map[string]string
	// API Tokens for connecting to remote nodes
	RemoteNodeTokens       map[string]string
	// URLs for remote nodes. Set these only if you want to
	// override the node URLs we get back from our local
	// DPN REST server.
	RemoteNodeURLs         map[string]string
}

func (dpnConfig *DPNConfig) TokenFormatStringFor(nodeNamespace string) (string) {
	tokenFormat := DEFAULT_TOKEN_FORMAT_STRING
	if dpnConfig.AuthTokenHeaderFormats != nil {
		if specialFormat, ok := dpnConfig.AuthTokenHeaderFormats[nodeNamespace]; ok {
			tokenFormat = specialFormat
		}
	}
	return tokenFormat
}

func LoadConfig(pathToFile, requestedConfig string) (*DPNConfig, error) {
	data, err := bagman.LoadRelativeFile(pathToFile)
	if err != nil {
		return nil, err
	}
	configMap := make(map[string]*DPNConfig)
	err = json.Unmarshal(data, &configMap)
    if err != nil {
        return nil, err
    }
	config := configMap[requestedConfig]
	if config == nil {
		return nil, fmt.Errorf("DPN config '%s' does not exist", requestedConfig)
	}
	// Load local API token from environment to keep it out of config file.
	// Need a better solution for this.
	if config.RestClient.LocalAuthToken == "" {
		config.RestClient.LocalAuthToken = os.Getenv("DPN_REST_TOKEN")
	}

	// TODO: Don't hard code this!! Fix for this is part of the much larger
	// overall config management fix!
	tokensInConfig := config.RemoteNodeTokens != nil && len(config.RemoteNodeTokens) > 0
	if tokensInConfig && config.RemoteNodeTokens["chron"] == "" {
		config.RemoteNodeTokens["chron"] = os.Getenv("DPN_CHRON_TOKEN")
	}
	if tokensInConfig && config.RemoteNodeTokens["hathi"] == "" {
		config.RemoteNodeTokens["hathi"] = os.Getenv("DPN_HATHI_TOKEN")
	}
	if tokensInConfig && config.RemoteNodeTokens["sdr"] == "" {
		config.RemoteNodeTokens["sdr"] = os.Getenv("DPN_SDR_TOKEN")
	}
	if tokensInConfig && config.RemoteNodeTokens["tdr"] == "" {
		config.RemoteNodeTokens["tdr"] = os.Getenv("DPN_TDR_TOKEN")
	}

	expanded, err := bagman.ExpandTilde(config.LogDirectory)
	if err == nil {
		config.LogDirectory = expanded
	}
	config.EnvironmentName = strings.ToLower(requestedConfig)
    return config, nil
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

	// What type of bag is this? Data, rights or interpretive?
	BagType                string

	// The underlying bag object.
	Bag                    *bagins.Bag

	// Timestamp describing when the bag was assembled.
	bagtime                time.Time
}
