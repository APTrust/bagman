package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
)

func TestNext(t *testing.T) {
	// Create 101 test file objects.
	// 4 out of every 5 need saving.
	files := make([]*bagman.File, 101)
	for i := 0; i < 101; i++ {
		if (i + 1) % 5 == 0 {
			files[i] = &bagman.File{
				NeedsSave: false,
			}
		} else {
			files[i] = &bagman.File{
				NeedsSave: true,
			}
		}
	}

	// Create the iterator to return files that need
	// saving in batches of 10.
	iter := bagman.NewFileBatchIterator(files, 10)

	// 80 of our 100 files need saving, and we set a
	// batch size of 10 in the constructor. We should
	// be able to pull back 8 batches of 10 without
	// getting an error.
	for i := 0; i < 8; i++ {
		batch, err := iter.NextBatch()
		if err != nil {
			t.Errorf("NextBatch() returned unexpected error: %v", err)
		}
		if len(batch) != 10 {
			t.Errorf("NextBatch() returned %d records, expected 10", len(batch))
		}
	}

	// Should get the 101st element
	batch, err := iter.NextBatch()
	if err != nil {
		t.Errorf("NextBatch() returned unexpected error: %v", err)
	}
	if len(batch) != 1 {
		t.Errorf("NextBatch() returned %d records, expected only one", len(batch))
	}

	// Should get error, since we've iterated through all files.
	batch, err = iter.NextBatch()
	if err == nil {
		t.Errorf("NextBatch() should have returned error at end of iteration")
	}

}
