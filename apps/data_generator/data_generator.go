package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/nu7hatch/gouuid"
	"math/rand"
	"os"
	"time"
)

var FILE_COUNT = 15000
var EXT = []string{".mp3", ".mp4", ".pdf", ".xml", ".txt"}
var MIME = []string{"audio/mpeg3", "video/mpeg4", "application/pdf", "application/xml", "text/plain"}
var HEX_CHARS = []rune("0123456789abcdef")

// Creates one IntellectualObject in Fluctus with lots of generic files.
func main() {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("data_generator started")
	bagRecorder := workers.NewBagRecorder(procUtil)

	processResult, err := bagman.LoadResult("testdata/result_good.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	prepareObject(processResult, FILE_COUNT)
	bagRecorder.RunWithoutNsq(processResult)
}

func prepareObject(result *bagman.ProcessResult, count int) {
	bagSuffix := time.Now().Unix()
	bagName := fmt.Sprintf("bag-with-%d-files-%d", FILE_COUNT, bagSuffix)
	result.S3File.Key.Key = bagName + ".tar"
	result.FetchResult.Key = bagName + ".tar"
	result.TarResult.OutputDir = fmt.Sprintf("/mnt/apt_data/%d-file-bag-%d", FILE_COUNT, bagSuffix)
	result.TarResult.InputFile = result.TarResult.OutputDir + ".tar"
	result.FetchResult.LocalTarFile = result.TarResult.OutputDir + ".tar"
	result.TarResult.FilesUnpacked = nil
	result.TarResult.Files = nil
	result.BagReadResult.Tags[5].Value = "Sample Bag " + bagName
	for i := 0; i < count; i++ {
		file := makeFile(bagName, i)
		result.TarResult.FilesUnpacked = append(result.TarResult.FilesUnpacked, file.Path)
		result.TarResult.Files = append(result.TarResult.Files, file)
	}
}

func makeFile(bagName string, number int) (*bagman.File) {
	mimeType, ext := getMimeAndExt()
	path := fmt.Sprintf("data/%d%s", number, ext)
	baseDate := getBaseDate()
	uuid, _ := uuid.NewV4()
	md5 := randSeq(32)
	return &bagman.File{
		Path: path,
		Size: int64(rand.Intn(1000000)),
		Created: baseDate,
		Modified: baseDate.Add(240 * time.Hour),
		Md5: md5,
		Md5Verified: baseDate.Add(1200 * time.Hour),
		Sha256: randSeq(64),
		Sha256Generated: baseDate.Add(1201 * time.Hour),
		Uuid: uuid.String(),
		UuidGenerated: baseDate.Add(1202 * time.Hour),
		MimeType: mimeType,
		ErrorMessage: "",
		StorageURL: fmt.Sprintf("https://s3.amazonaws.com/not_really_stored/%s", uuid.String()),
		StoredAt: baseDate.Add(1203 * time.Hour),
		StorageMd5: md5,
		Identifier: fmt.Sprintf("ncsu.edu/%s/%s", bagName, path),
		IdentifierAssigned: baseDate.Add(1204 * time.Hour),
		ExistingFile: false,
		NeedsSave: true,
	}
}

func randSeq(n int) (string) {
    b := make([]rune, n)
    for i := range b {
        b[i] = HEX_CHARS[rand.Intn(len(HEX_CHARS))]
    }
    return string(b)
}

func getMimeAndExt() (string, string) {
	i := rand.Intn(len(EXT))
	return MIME[i], EXT[i]
}

func getBaseDate() (time.Time) {
	randHours := time.Duration(rand.Intn(18000) * -1)
	return time.Now().Add(randHours * time.Hour)
}
