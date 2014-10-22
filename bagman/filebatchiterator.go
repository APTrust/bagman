package bagman

import (
	"errors"
)

var ErrStopIteration = errors.New("iteration completed")

// FileBatchIterator returns batches of files whose metadata
// needs to be saved in Fluctus.
type FileBatchIterator struct {
	files        []*File
	fileCount    int
	currentIndex int
	batchSize    int
}

// Returns a new BatchFileIterator that will iterate over files.
// Each call to Next() will return up to batchSize files that
// need to be saved.
func NewFileBatchIterator (files []*File, batchSize int) (*FileBatchIterator) {
	return &FileBatchIterator {
		files: files,
		fileCount: len(files),
		batchSize: batchSize,
		currentIndex: 0,
	}
}

// Next returns the next N files that need to be saved, where N is
// the batchSize that was passed into NewFileBatchIterator. This may
// return fewer than batchSize files if only a few need saving.
// When no remaining files need to be saved, this returns an error.
func (iter *FileBatchIterator) NextBatch() ([]*GenericFile, error) {
	matches := make([]*GenericFile, 0)
	for i := iter.currentIndex; i < iter.fileCount; i++ {
		file := iter.files[i]
		if file.NeedsSave == true {
			// Error here, failure to generate UUID, should be extremely rare.
			genericFile, err := file.ToGenericFile()
			if err != nil {
				return nil, err
			}
			matches = append(matches, genericFile)
		}
		iter.currentIndex++
		if len(matches) == iter.batchSize {
			break
		}
	}
	if len(matches) == 0 {
		return nil, ErrStopIteration
	}
	return matches, nil
}
