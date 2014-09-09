// Integration tests for the IngestHelper.
// These tests require access to S3 and Fluctus, but not NSQ.
package ingesthelper_test

import (
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/ingesthelper"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
	"github.com/diamondap/goamz/aws"
	"github.com/diamondap/goamz/s3"
	"os"
	"net/http"
	"strconv"
	"testing"
)

var fluctusUrl string = "http://localhost:3000"
var skipMessagePrinted bool = false

func fluctusAvailable() bool {
	_, err := http.Get(fluctusUrl)
	if err != nil {
		return false
	}
	return true
}

func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}

func environmentReady() (bool) {
	if fluctusAvailable() == false {
		if !skipMessagePrinted {
			msg := "Skipping IngestHelper tests because environment variables " +
				"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set."
			fmt.Fprintln(os.Stderr, msg)
			skipMessagePrinted = true
		}
		return false
	}
	if awsEnvAvailable() == false {
		if !skipMessagePrinted {
			msg := fmt.Sprintf("Skipping tests because Fluctus is not "+
				"running at %s", fluctusUrl)
			fmt.Fprintln(os.Stderr, msg)
			skipMessagePrinted = true
		}
		return false
	}
	return true
}

func getS3File() (*bagman.S3File) {
	return &bagman.S3File {
        BucketName: "aptrust.receiving.test.edu",
        Key: s3.Key{
            Key: "ncsu.1840.16-2928.tar",
            LastModified: "2014-04-25T19:01:20.000Z",
            Size: 696320,
            ETag: "\"b4f8f3072f73598fc5b65bf416b6019a\"",
            StorageClass: "STANDARD",
        },
	}
}

func getProcessUtil() (*processutil.ProcessUtil) {
	testConfig := "test"
	return processutil.NewProcessUtil(&testConfig)
}

func getIngestHelper() (*ingesthelper.IngestHelper) {
	msgId := nsq.MessageID{'1', '0','1', '0','1', '0','1', '0','1',
		'0','1', '0','1', '0','1', '0',}
	body := []byte{'h', 'e', 'l', 'l', 'o'}
	nsqMessage := nsq.NewMessage(msgId, body)
	return ingesthelper.NewIngestHelper(getProcessUtil(), nsqMessage, getS3File())
}

func TestBagNeedsProcessing(t *testing.T) {
	if environmentReady() == false {
		return
	}
	processUtil := getProcessUtil()
	s3File := getS3File()
	if ingesthelper.BagNeedsProcessing(s3File, processUtil) == false {
		t.Error("BagNeedsProcessing should have returned true")
	}
}

func TestIncompleteCopyToS3(t *testing.T) {
	if environmentReady() == false {
		return
	}
	helper := getIngestHelper()
	helper.Result.TarResult = &bagman.TarResult{}
	helper.Result.TarResult.GenericFiles = make([]*bagman.GenericFile, 2)
	gf0 := &bagman.GenericFile{
		StorageURL: "http://blah.blah.blah",
		NeedsSave: true,
	}
	gf1 := &bagman.GenericFile{
		StorageURL: "",
		NeedsSave: false,
	}
	helper.Result.TarResult.GenericFiles[0] = gf0
	helper.Result.TarResult.GenericFiles[1] = gf1

	// Only one file needed saving, and it was saved
	if helper.IncompleteCopyToS3() == true {
		t.Error("helper.IncompleteCopyToS3() should have returned false")
	}

	// Two files need saving, two were saved
	gf1.StorageURL = "http://yadda.yadda"
	gf1.NeedsSave = true
	if helper.IncompleteCopyToS3() == true {
		t.Error("helper.IncompleteCopyToS3() should have returned false")
	}

	// Two files need saving, one was saved
	gf1.StorageURL = ""
	if helper.IncompleteCopyToS3() == false {
		t.Error("helper.IncompleteCopyToS3() should have returned true")
	}

}

func TestFailedAndNoMoreRetries(t *testing.T) {
	if environmentReady() == false {
		return
	}
	helper := getIngestHelper()

	// No error message in result and we're on the first attempt.
	helper.Result.NsqMessage.Attempts = 1
	if helper.FailedAndNoMoreRetries() == true {
		t.Error("helper.FailedAndNoMoreRetries() should have returned false")
	}

	// Presence of ANY error message in result indicates failure.
	// But we're still at attempt #1, so we should be OK
	helper.Result.ErrorMessage = "Oopsie!"
	if helper.FailedAndNoMoreRetries() == true {
		t.Error("helper.FailedAndNoMoreRetries() should have returned false")
	}

	// We're above the retry threshold, but no error, so we should be OK.
	helper.Result.NsqMessage.Attempts = uint16(helper.ProcUtil.Config.MaxBagAttempts) * 2
	helper.Result.ErrorMessage = ""
	if helper.FailedAndNoMoreRetries() == true {
		t.Error("helper.FailedAndNoMoreRetries() should have returned false")
	}

	// Now we have an error and we're above the retry threshold.
	helper.Result.ErrorMessage = "Now you've done it!"
	if helper.FailedAndNoMoreRetries() == false {
		t.Error("helper.FailedAndNoMoreRetries() should have returned true")
	}

}


func TestGetS3Options(t *testing.T) {
	if environmentReady() == false {
		return
	}
	helper := getIngestHelper()
	gf := &bagman.GenericFile{
		Md5: "b4f8f3072f73598fc5b65bf416b6019a",
		Path: "/data/hansel/und/gretel.pdf",
	}
	opts, err := helper.GetS3Options(gf)
	if err != nil {
		t.Error(err)
	}
	if opts.ContentMD5 != "tPjzBy9zWY/Ftlv0FrYBmg==" {
		t.Error("Got incorrect base64-encoded md5 string")
	}
	expectedMd5 := "b4f8f3072f73598fc5b65bf416b6019a"
	if opts.Meta["md5"][0] != expectedMd5 {
		t.Errorf("Expected md5 metadata '%s', but found '%s'",
			expectedMd5, opts.Meta["md5"][0])
	}
	if opts.Meta["institution"][0] != "test.edu" {
		t.Errorf("Expected institution metadata 'test.edu', but found '%s'",
			opts.Meta["institution"][0])
	}
	if opts.Meta["bag"][0] != "ncsu.1840.16-2928" {
		t.Errorf("Expected bag metadata 'ncsu.1840.16-2928', but found '%s'",
			opts.Meta["bag"][0])
	}
	if opts.Meta["bagpath"][0] != gf.Path {
		t.Errorf("Expected bag metadata '%s', but found '%s'",
			gf.Path, opts.Meta["bagpath"][0])
	}
}

func TestFullProcess(t *testing.T) {
	if environmentReady() == false {
		return
	}
	helper := getIngestHelper()

	helper.FetchTarFile()
	if helper.Result.ErrorMessage != "" {
		t.Errorf(helper.Result.ErrorMessage)
	}
	verifyFetchResult(t, helper.Result.FetchResult)

	helper.ProcessBagFile()
	// Tests

	helper.SaveGenericFiles()
	// Tests

	helper.LogResult()
	// Tests

	helper.DeleteLocalFiles()
	// Tests

}

func verifyResult(t *testing.T, itemName, expected, actual string) {
	if expected != actual {
		t.Errorf("%s expected '%s' but got '%s'", itemName, expected, actual)
	}
}

func verifyFetchResult(t *testing.T, fetchResult *bagman.FetchResult) {
	verifyResult(t, "BucketName", "aptrust.receiving.test.edu", fetchResult.BucketName)
	verifyResult(t, "Key", "ncsu.1840.16-2928.tar", fetchResult.Key)
	verifyResult(t, "LocalTarFile", "tmp/ncsu.1840.16-2928.tar", fetchResult.LocalTarFile)
	verifyResult(t, "RemoteMd5", "b4f8f3072f73598fc5b65bf416b6019a", fetchResult.RemoteMd5)
	verifyResult(t, "LocalMd5", "b4f8f3072f73598fc5b65bf416b6019a", fetchResult.LocalMd5)
	verifyResult(t, "Md5Verified", "true", strconv.FormatBool(fetchResult.Md5Verified))
	verifyResult(t, "Md5Verifiable", "true", strconv.FormatBool(fetchResult.Md5Verifiable))
	verifyResult(t, "ErrorMessage", "", fetchResult.ErrorMessage)
	verifyResult(t, "Warning", "", fetchResult.Warning)
	verifyResult(t, "Retry", "true", strconv.FormatBool(fetchResult.Retry))
}
