package bagman

// FetchResult descibes the results of fetching a bag from S3
// and verification of that bag.
type FetchResult struct {
	BucketName    string
	Key           string
	LocalTarFile  string
	RemoteMd5     string
	LocalMd5      string
	Md5Verified   bool
	Md5Verifiable bool
	ErrorMessage  string
	Warning       string
	Retry         bool
}
