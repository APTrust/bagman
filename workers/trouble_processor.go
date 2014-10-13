package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// TroubleProcessor dumps the ProcessResult structure of
// items that failed the ingest process into JSON files.
// The JSON is formatted and human-readable, and may be
// deserialized and loaded into other processes in the future.
// The ProcessResult structure contains fairly detailed
// information about every stage of the ingest process.
type TroubleProcessor struct {
	ProcUtil *bagman.ProcessUtil
}

func NewTroubleProcessor(procUtil *bagman.ProcessUtil) (*TroubleProcessor) {
	return &TroubleProcessor{
		ProcUtil: procUtil,
	}
}

func (troubleProcessor *TroubleProcessor) HandleMessage(message *nsq.Message) error {
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		troubleProcessor.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	troubleProcessor.dumpToFile(&result)
	troubleProcessor.ProcUtil.MessageLog.Info("Processed %s", result.S3File.Key.Key)
	return nil
}

func (troubleProcessor *TroubleProcessor) dumpToFile(result *bagman.ProcessResult) error {
	outdir := path.Join(troubleProcessor.ProcUtil.Config.LogDirectory, "trouble")
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		err := os.Mkdir(outdir, 0766)
		if err != nil {
			panic(err)
		}
	}
	filename := fmt.Sprintf("%s_%s",
		bagman.OwnerOf(result.S3File.BucketName),
		strings.Replace(result.S3File.Key.Key, ".tar", ".json", -1))
	json, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(path.Join(outdir, filename), json, 0644)
	if err != nil {
		panic(err)
	}
	return nil
}
