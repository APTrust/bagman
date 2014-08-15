package bagman

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/bagman/fluctus/models"
	"os"
	"path/filepath"
	"strings"
)

// (nsq worker) Make sure we have enough disk space.
// Create the bag (baggins).
// Create tag file with title and description.
// Create subdirectories.
// Download files from S3 to tmp area.
// Copy files into subdirectories.
// Save bag.
// (nsq worker) Tar the bag.
// (nsq worker) Copy the tar file to S3 restoration bucket.
// (nsq worker) Log results.
// (nsq worker) Clean up all temp files.

type BagRestorer struct {
	// The intellectual object we'll be restoring.
	IntellectualObject *models.IntellectualObject
	// The bag.
	Bag                *bagins.Bag

	workingDir         string
	bagName            string
	errorMessage       string
}

// Creates a new bag restorer from the intellectual object.
// Param working dir is the path to the directory into which
// files should be downloaded and the bag should be built.
func NewBagRestorer(intelObj *models.IntellectualObject, workingDir string) (*BagRestorer, error) {
	if intelObj == nil {
		return nil, fmt.Errorf("IntellectualObject cannot be nil")
	}
	if _, err := os.Stat(workingDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("Directory %s workingDir does not exist")
	}
	absWorkingDir, err := filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}
	// Make sure we have a directory for this institution
	instDir := filepath.Join(absWorkingDir, intelObj.InstitutionId)
	err = os.Mkdir(instDir, 0755)
	if err != nil {
		return nil, err
	}
	// Specify the location & create a new empty bag.
	location := filepath.Join(absWorkingDir, intelObj.Identifier)
	bagName := strings.Replace(intelObj.Identifier, intelObj.InstitutionId + "/", "", 1)
	bag, err := bagins.NewBag(location, bagName, "md5")
	restorer := BagRestorer {
		IntellectualObject: intelObj,
		Bag: bag,
		workingDir: location,
		bagName: bagName,
	}
	return &restorer, nil
}

func (bagRestorer *BagRestorer) CreateBag() (error) {
	// Create tag file with title and description.
	// Create subdirectories.
	// Download files from S3 to tmp area.
	// Copy files into subdirectories.
	// Save bag.
	return nil
}

// Returns the path to the tar file. This is the file that should
// be copied to the S3 restoration bucket.
func (bagRestorer *BagRestorer) TarFile() (string) {
	return ""
}

func (bagRestorer *BagRestorer) WorkingDir() (string) {
	return bagRestorer.workingDir
}

func (bagRestorer *BagRestorer) BagName() (string) {
	return bagRestorer.bagName
}
