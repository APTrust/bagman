package ingesthelper

import (
//	"encoding/base64"
//	"encoding/hex"
//	"encoding/json"
//	"flag"
//	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
//	"github.com/diamondap/goamz/s3"
//	"os"
//	"path/filepath"
//	"regexp"
//	"strings"
//	"sync/atomic"
//	"time"
)

type IngestHelper struct {
	ProcUtil    *processutil.ProcessUtil
	Result      *bagman.ProcessResult
}

// Returns a new IngestHelper
func NewIngestHelper(procUtil *processutil.ProcessUtil, message *nsq.Message, s3File *bagman.S3File) (*IngestHelper){
	return &IngestHelper{
		ProcUtil: procUtil,
		Result: newResult(message, s3File),
	}
}

// Returns a new ProcessResult for the specified NSQ message
// and S3 bag (tar file)
func newResult(message *nsq.Message, s3File *bagman.S3File) (*bagman.ProcessResult) {
	return &bagman.ProcessResult{
		NsqMessage:    message,
		S3File:        s3File,
		ErrorMessage:  "",
		FetchResult:   nil,
		TarResult:     nil,
		BagReadResult: nil,
		FedoraResult:  nil,
		Stage:         "",
		Retry:         true,
	}
}
