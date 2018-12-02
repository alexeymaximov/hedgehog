package hadgehog

import (
	"encoding/binary"
	"math"
	"os"
	"runtime"
	"sync"

	"github.com/alexeymaximov/stateholder"
	"github.com/alexeymaximov/syspack"
	"github.com/alexeymaximov/syspack/mmap"
)

// Transmitter metadata sign.
var txSign = []byte{'T', 'X', 1, 0, 0}

// Transmitter metadata keys.
const (
	txFrame  = "frame"
	txOffset = "offset"
	txCount  = "count"
)

type Transmitter struct {
	// Transmitter.

	// Storage.
	storage *Storage

	// Size of single data frame.
	// Data frame file will be extended to this size if it is shorter.
	frameSize syspack.Size

	// Metadata.
	meta *stateholder.Stateholder

	// Current data frame.
	frame *mmap.Mapping

	// Capacity of current data frame.
	capacity syspack.Size

	// Mutex.
	mutex *sync.RWMutex
}

// Make new transmitter.
func NewTransmitter(storage *Storage, frameSize syspack.Size) (*Transmitter, error) {
	if storage == nil {
		return nil, &ErrorStorageInvalid{}
	}
	if frameSize > math.MaxInt64 {
		return nil, &ErrorFrameTooLarge{Size: frameSize}
	}
	transmitter := &Transmitter{
		storage:   storage,
		frameSize: frameSize,
		mutex:     &sync.RWMutex{},
	}
	metaPath, err := transmitter.storage.Transmitter()
	if err != nil {
		return nil, err
	}
	transmitter.meta = stateholder.NewStateholder()
	if err := transmitter.meta.DefineUint64(txFrame); err != nil {
		transmitter.meta.Close()
		return nil, err
	}
	if err := transmitter.meta.DefineUint64(txOffset); err != nil {
		transmitter.meta.Close()
		return nil, err
	}
	if err := transmitter.meta.DefineUint64(txCount); err != nil {
		transmitter.meta.Close()
		return nil, err
	}
	if _, err := transmitter.meta.Attach(metaPath, txSign); err != nil {
		transmitter.meta.Close()
		return nil, err
	}
	if err := transmitter.openFrame(); err != nil {
		transmitter.meta.Close()
		return nil, err
	}
	runtime.SetFinalizer(transmitter, (*Transmitter).Close)
	return transmitter, nil
}

// Get current state.
func (transmitter *Transmitter) State() (Frame, syspack.Offset, uint64, error) {
	transmitter.mutex.RLock()
	defer transmitter.mutex.RUnlock()
	if transmitter.meta == nil {
		return 0, 0, 0, &ErrorTransmitterClosed{}
	}
	frame, err := transmitter.meta.GetUint64(txFrame)
	if err != nil {
		return 0, 0, 0, err
	}
	offset, err := transmitter.meta.GetUint64(txOffset)
	if err != nil {
		return 0, 0, 0, err
	}
	count, err := transmitter.meta.GetUint64(txCount)
	if err != nil {
		return 0, 0, 0, err
	}
	return frame, syspack.Offset(offset), count, nil
}

// Sync data.
func (transmitter *Transmitter) sync() error {
	if err := transmitter.frame.Sync(); err != nil {
		return err
	}
	return transmitter.meta.Sync()
}

// Create data frame file.
func (transmitter *Transmitter) createFrameFile(path string) error {
	lockedPath := path + ".lock"
	file, err := os.OpenFile(lockedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err := file.Truncate(int64(transmitter.frameSize)); err != nil {
		file.Close()
		os.Remove(lockedPath)
		return err
	}
	if n, err := file.WriteAt([]byte{0}, 0); err != nil {
		file.Close()
		os.Remove(lockedPath)
		return err
	} else if n < 1 {
		file.Close()
		os.Remove(lockedPath)
		return &ErrorCorruptedWrite{Real: n, Expected: 1}
	}
	if err := file.Close(); err != nil {
		os.Remove(lockedPath)
		return err
	}
	if err := os.Rename(lockedPath, path); err != nil {
		return err
	}
	return nil
}

// Open current data frame.
func (transmitter *Transmitter) openFrame() error {
	frame, err := transmitter.meta.GetUint64(txFrame)
	if err != nil {
		return err
	}
	framePath, err := transmitter.storage.Frame(frame)
	if err != nil {
		return err
	}
	if _, err := os.Stat(framePath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := transmitter.createFrameFile(framePath); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(framePath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := syspack.Size(info.Size())
	mapping, err := mmap.NewMapping(file.Fd(), 0, size, &mmap.Options{
		Mode: mmap.ModeReadWrite,
	})
	if err != nil {
		return err
	}
	transmitter.frame = mapping
	transmitter.capacity = size
	return nil
}

// Close current data frame.
func (transmitter *Transmitter) closeFrame() error {
	if err := transmitter.sync(); err != nil {
		return err
	}
	if err := transmitter.frame.Close(); err != nil {
		return err
	}
	transmitter.frame = nil
	transmitter.capacity = 0
	return nil
}

// Roll next data frame.
func (transmitter *Transmitter) rollFrame() error {
	defer func() {
		if transmitter.frame == nil {
			transmitter.openFrame()
		}
	}()
	if err := transmitter.closeFrame(); err != nil {
		return err
	}
	if err := transmitter.meta.Begin(); err != nil {
		return err
	}
	defer transmitter.meta.Rollback()
	if _, _, err := transmitter.meta.IncUint64(txFrame, 1); err != nil {
		return err
	}
	if err := transmitter.meta.SetUint64(txOffset, 0); err != nil {
		return err
	}
	if err := transmitter.openFrame(); err != nil {
		return err
	}
	if err := transmitter.meta.Persist(); err != nil {
		transmitter.frame.Close()
		transmitter.frame = nil
		transmitter.capacity = 0
		return err
	}
	return nil
}

// Write record to data frame.
func (transmitter *Transmitter) writeToFrame(record []byte) error {
	offset, err := transmitter.meta.GetUint64(txOffset)
	if err != nil {
		return err
	}
	recordSize := len(record)
	blockSize := 9 + recordSize
	bufferSize := blockSize + 1
	if syspack.Size(offset)+syspack.Size(bufferSize) > transmitter.capacity {
		if err := transmitter.rollFrame(); err != nil {
			return err
		}
		return transmitter.writeToFrame(record)
	}
	buffer := make([]byte, bufferSize)
	buffer[0] = 0
	binary.LittleEndian.PutUint64(buffer[1:9], uint64(recordSize))
	copy(buffer[9:blockSize], record)
	buffer[bufferSize-1] = 0
	if n, err := transmitter.frame.WriteAt(buffer, syspack.Offset(offset)); err != nil {
		return err
	} else if n < bufferSize {
		return &ErrorCorruptedWrite{Real: n, Expected: bufferSize}
	}
	if err := transmitter.frame.WriteByteAt(1, syspack.Offset(offset)); err != nil {
		return err
	}
	if err := transmitter.meta.Begin(); err != nil {
		return err
	}
	defer transmitter.meta.Rollback()
	if _, _, err := transmitter.meta.IncUint64(txOffset, uint64(blockSize)); err != nil {
		return err
	}
	if _, _, err := transmitter.meta.IncUint64(txCount, 1); err != nil {
		return err
	}
	if err := transmitter.meta.Commit(); err != nil {
		return err
	}
	return nil
}

// Transmit record.
func (transmitter *Transmitter) Transmit(record []byte) error {
	transmitter.mutex.Lock()
	defer transmitter.mutex.Unlock()
	if transmitter.meta == nil {
		return &ErrorTransmitterClosed{}
	}
	if transmitter.frame == nil {
		return &ErrorFrameClosed{}
	}
	if recordSize := syspack.Len(record); 9+recordSize+1 > transmitter.frameSize {
		return &ErrorRecordTooLarge{Size: recordSize}
	}
	return transmitter.writeToFrame(record)
}

// Sync data.
func (transmitter *Transmitter) Sync() error {
	transmitter.mutex.Lock()
	defer transmitter.mutex.Unlock()
	if transmitter.meta == nil {
		return &ErrorTransmitterClosed{}
	}
	if transmitter.frame == nil {
		return &ErrorFrameClosed{}
	}
	return transmitter.sync()
}

// Close transmitter.
func (transmitter *Transmitter) Close() error {
	transmitter.mutex.Lock()
	defer transmitter.mutex.Unlock()
	if transmitter.meta == nil {
		return &ErrorTransmitterClosed{}
	}
	if transmitter.frame != nil {
		if err := transmitter.closeFrame(); err != nil {
			return err
		}
	}
	if err := transmitter.meta.Close(); err != nil {
		return err
	}
	transmitter.meta = nil
	runtime.SetFinalizer(transmitter, nil)
	return nil
}
