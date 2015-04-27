package dpn

const (
	STAGE_PACKAGE = "Packaging"
	STAGE_STORE   = "Storage"
	STAGE_RECORD  = "Recoding"
)

type DPNResult struct {
	BagIdentifier string
	Stage         string
	ErrorMessage  string
	PackageResult *PackageResult
	StorageResult *StorageResult
}

func NewDPNResult(bagIdentifier string) (*DPNResult) {
	return &DPNResult{
		BagIdentifier: bagIdentifier,
		Stage: STAGE_PACKAGE,
	}
}
