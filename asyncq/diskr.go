package asyncq

import (
	"fmt"
	"os"
	"strings"
)

func (w *worker[Input]) readWAL(
	flusherWAL func(tasks []Task[Input]) error,
) {
	// scan whole directory of active wal path
	entries, err := os.ReadDir(w.walPath)
	if err != nil {
		w.readerErrHandler(WALPath{
			path: w.walPath,
		}, fmt.Errorf("error reading wal directory"))
	}

	// wal indices to be rescaned further
	walIdx := make([]int, 0, len(entries))

	// poison wal tombstone marker map to avoid being rescan more than once
	poisonMarkMap := make(map[string]struct{})

	// error wal tombstone marker map to avoid being rescan more than once
	errMarkMap := make(map[string]struct{})

	// scan only to filer valid gobs file , poison & error tombstone file
	for i := range entries {
		switch {
		case entries[i].IsDir():
			continue
		case strings.HasSuffix(entries[i].Name(), string(gobF)):
			walIdx = append(walIdx, i)
		case strings.HasSuffix(entries[i].Name(), string(gobFPoison)):
			fileName := strings.TrimSuffix(entries[i].Name(), string(poisonF))
			poisonMarkMap[fileName] = struct{}{}
		case strings.HasSuffix(entries[i].Name(), string(gobFError)):
			fileName := strings.TrimSuffix(entries[i].Name(), string(errorF))
			errMarkMap[fileName] = struct{}{}
		}
	}

	// isPoison & isErrMarked for identifying tombstones for poison & error to be skipped
	isPoison := func(fileName string) bool {
		_, found := poisonMarkMap[fileName]
		return found
	}
	isErrMarked := func(fileName string) bool {
		_, found := errMarkMap[fileName]
		return found
	}

	// scanning valid wal indices
	for _, i := range walIdx {
		if isPoison(entries[i].Name()) || isErrMarked(entries[i].Name()) {
			continue
		}
		
	}

}

func (w *worker[Input]) readWALOnSIG(
	flusherWAL func(tasks []Task[Input]) error,
) {
	for range w.readerSig {
		w.readWAL(flusherWAL)
	}
}
