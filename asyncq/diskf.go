package asyncq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func (w *worker[Input]) flushFailed(tasks []flushdTask[Input]) {
	for _, t := range tasks {
		t.replyErr <- FailedDiskWriteErr
	}
}

func (w *worker[Input]) flushSuccess(tasks []flushdTask[Input]) {
	for _, t := range tasks {
		t.replyErr <- nil
	}
}

func (w *worker[Input]) flushToDisk(ctx context.Context, tasks []flushdTask[Input]) error {
	// wal file path targeting uniqueness in wal path
	unixMs := time.Now().UnixMilli()
	tempDirPath := filepath.Join(w.tempPath, fmt.Sprintf("%d%s", unixMs, gobF))

	// open first in temp directory
	file, err := os.OpenFile(tempDirPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to write wal on temporary dir:%w", err)
	}
	isFileOpen := true
	isFileOnTemp := true
	defer func() {
		if isFileOpen {
			file.Close()
		}
		if isFileOnTemp {
			os.Remove(tempDirPath)
		}
	}()

	// payload from all inputs in single slice preparing for encoding
	payload := make([]Input, 0, len(tasks))
	for _, t := range tasks {
		payload = append(payload, t.input)
	}

	// writer using bufio writer later to be flush
	writer := bufio.NewWriter(file)

	// encode with go binary
	var payloadBuf bytes.Buffer
	encoder := gob.NewEncoder(&payloadBuf)
	if err := encoder.Encode(payload); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to encode payload in flush to disk:%w", err)
	}
	payloadBytes := payloadBuf.Bytes()

	// compute CRC32 dari hasil encode Gob
	checksum := crc32.Checksum(payloadBytes, crcTable)

	// write Header& payload [[len(payloadBytes)][CRC][payloadBytes]] to bufio.Writer
	// write byte header [[len(payloadBytes)][CRC]] 8 byte
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payloadBytes))) // len(payloadBytes) 4 bytes
	binary.LittleEndian.PutUint32(header[4:8], checksum)                  // CRC32 4 bytes

	// append byte header to file writer
	if _, err := writer.Write(header[:]); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to write header: %w", err)
	}

	// gob bytes payload write
	if _, err := writer.Write(payloadBytes); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to write payload bytes before flushing to disk:%w", err)
	}

	// Flush from buffer Go to OS Page Cache
	if err := writer.Flush(); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to flush bytes to os page cache:%w", err)
	}

	// Sync from OS to Disk physically (100% Durable in temp)
	if err := file.Sync(); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to sync file to make sure file is durable in disk:%w", err)
	}

	// close file before rename (mandatory, especially on windows)
	if err := file.Close(); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to close temp file before removing to wal directory:%w", err)
	}
	isFileOpen = false

	// checking for conflict on wal path
	counter := -1 // counter for adding integer to avoid conflict
	newWalDirPath := func() string {
		counter++
		return filepath.Join(w.walPath, fmt.Sprintf("%d-%d%s", unixMs, counter,gobF))
	}
	var walDirPath string
	for {
		walDirPath = newWalDirPath()
		if _, err := os.Stat(walDirPath); err == nil {
			// File exists
			continue

		} else if !errors.Is(err, os.ErrNotExist) {
			w.flushFailed(tasks)
			return fmt.Errorf("failed to check wal file existence before moving to wal directory: %w", err)
		}
		break
	}

	// atomicly remove from temp to wal path (1x syscall rename)
	if err := os.Rename(tempDirPath, walDirPath); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to rename file path to wal directory:%w", err)
	}
	isFileOnTemp = false
	// 6. Sync the walPath for durability
	dir, err := os.Open(w.walPath)
	if err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to open new wal path:%w", err)
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		w.flushFailed(tasks)
		return fmt.Errorf("failed to move data from temp to wal for durability:%w", err)
	}
	w.flushSuccess(tasks)

	// send reader signal to read WAL
	select {
	case w.readerSig <- struct{}{}:
	default:
	}

	return nil
}
