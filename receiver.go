package hadgehog

// TODO: Use direct mapping slice.

import (
	"encoding/binary"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/alexeymaximov/stateholder"
	"github.com/alexeymaximov/syspack"
	"github.com/alexeymaximov/syspack/mmap"
)

// Receiver metadata sign.
var rxSign = []byte{'R', 'X', 1, 0, 0}

// Receiver metadata keys.
const (
	rxFrame  = "frame"
	rxOffset = "offset"
	rxCount  = "count"
)

type Receiver struct {
	// Receiver.

	// Storage.
	storage *Storage

	// Port.
	port Port

	// Metadata.
	meta *stateholder.Stateholder

	// Current data frame.
	frame *mmap.Mapping

	// Capacity of current data frame.
	capacity syspack.Size

	// Transaction.
	transaction *receiverTransaction

	// Mutex.
	mutex *sync.RWMutex
}

// Make new receiver.
func NewReceiver(storage *Storage, port Port, frame Frame) (*Receiver, error) {
	if storage == nil {
		return nil, &ErrorStorageInvalid{}
	}
	receiver := &Receiver{
		storage: storage,
		port:    port,
		mutex:   &sync.RWMutex{},
	}
	metaPath, err := receiver.storage.Receiver(receiver.port)
	if err != nil {
		return nil, err
	}
	receiver.meta = stateholder.NewStateholder()
	if err := receiver.meta.DefineUint64(rxFrame); err != nil {
		receiver.meta.Close()
		return nil, err
	}
	if err := receiver.meta.DefineUint64(rxOffset); err != nil {
		receiver.meta.Close()
		return nil, err
	}
	if err := receiver.meta.DefineUint64(rxCount); err != nil {
		receiver.meta.Close()
		return nil, err
	}
	if init, err := receiver.meta.Attach(metaPath, rxSign); err != nil {
		return nil, err
	} else if init {
		if err := receiver.meta.SetUint64(rxFrame, frame); err != nil {
			receiver.meta.Close()
			return nil, err
		}
	}
	if err := receiver.openFrame(); err != nil {
		receiver.meta.Close()
		return nil, err
	}
	runtime.SetFinalizer(receiver, (*Receiver).Close)
	return receiver, nil
}

// Get current state.
func (receiver *Receiver) State(transactional bool) (Frame, syspack.Offset, uint64, error) {
	receiver.mutex.RLock()
	defer receiver.mutex.RUnlock()
	if receiver.meta == nil {
		return 0, 0, 0, &ErrorReceiverClosed{}
	}
	if transactional && receiver.transaction != nil {
		tx := receiver.transaction
		return tx.frame, syspack.Offset(tx.offset), tx.count, nil
	}
	frame, err := receiver.meta.GetUint64(rxFrame)
	if err != nil {
		return 0, 0, 0, err
	}
	offset, err := receiver.meta.GetUint64(rxOffset)
	if err != nil {
		return 0, 0, 0, err
	}
	count, err := receiver.meta.GetUint64(rxCount)
	if err != nil {
		return 0, 0, 0, err
	}
	return frame, syspack.Offset(offset), count, nil
}

// Open current data frame.
func (receiver *Receiver) openFrame() error {
	var frame uint64
	if receiver.transaction != nil {
		frame = receiver.transaction.frame
	} else {
		var err error
		frame, err = receiver.meta.GetUint64(rxFrame)
		if err != nil {
			return err
		}
	}
	framePath, err := receiver.storage.Frame(frame)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(framePath, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return io.EOF
		}
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := syspack.Size(info.Size())
	mapping, err := mmap.NewMapping(file.Fd(), 0, size, &mmap.Options{
		Mode: mmap.ModeReadOnly,
	})
	if err != nil {
		return err
	}
	receiver.frame = mapping
	receiver.capacity = size
	return nil
}

// Close current data frame.
func (receiver *Receiver) closeFrame() error {
	if receiver.transaction == nil {
		if err := receiver.meta.Sync(); err != nil {
			return err
		}
	}
	if err := receiver.frame.Close(); err != nil {
		return err
	}
	receiver.frame = nil
	receiver.capacity = 0
	return nil
}

// Check next data frame.
func (receiver *Receiver) checkFrame() error {
	var frame uint64
	if receiver.transaction != nil {
		frame = receiver.transaction.frame
	} else {
		var err error
		frame, err = receiver.meta.GetUint64(rxFrame)
		if err != nil {
			return err
		}
	}
	frame++
	framePath, err := receiver.storage.Frame(frame)
	if err != nil {
		return err
	}
	if _, err := os.Stat(framePath); err != nil {
		if os.IsNotExist(err) {
			return io.EOF
		}
		return err
	}
	return nil
}

// Roll next data frame.
func (receiver *Receiver) rollFrame() error {
	if err := receiver.checkFrame(); err != nil {
		return err
	}
	backupTransaction := receiver.transaction
	defer func() {
		receiver.transaction = backupTransaction
		if receiver.frame == nil {
			receiver.openFrame()
		}
	}()
	if err := receiver.closeFrame(); err != nil {
		return err
	}
	if receiver.transaction != nil {
		backupTransaction = &receiverTransaction{
			frame:  receiver.transaction.frame,
			offset: receiver.transaction.offset,
			count:  receiver.transaction.count,
		}
		receiver.transaction.frame++
		receiver.transaction.offset = 0
	} else {
		if err := receiver.meta.Begin(); err != nil {
			return err
		}
		defer receiver.meta.Rollback()
		if _, _, err := receiver.meta.IncUint64(rxFrame, 1); err != nil {
			return err
		}
		if err := receiver.meta.SetUint64(rxOffset, 0); err != nil {
			return err
		}
	}
	if err := receiver.openFrame(); err != nil {
		return err
	}
	if receiver.transaction != nil {
		backupTransaction = receiver.transaction
	} else {
		if err := receiver.meta.Persist(); err != nil {
			receiver.frame.Close()
			receiver.frame = nil
			receiver.capacity = 0
			return err
		}
	}
	return nil
}

// Read record from data frame.
func (receiver *Receiver) readFromFrame() ([]byte, error) {
	var offset uint64
	if receiver.transaction != nil {
		offset = receiver.transaction.offset
	} else {
		var err error
		offset, err = receiver.meta.GetUint64(rxOffset)
		if err != nil {
			return nil, err
		}
	}
	if syspack.Size(offset) >= receiver.capacity {
		if err := receiver.rollFrame(); err != nil {
			return nil, err
		}
		return receiver.readFromFrame()
	}
	if magic, err := receiver.frame.ReadByteAt(syspack.Offset(offset)); err != nil {
		return nil, err
	} else if magic != 1 {
		if err := receiver.rollFrame(); err != nil {
			return nil, err
		}
		return receiver.readFromFrame()
	}
	var recordSizeBuffer = make([]byte, 8)
	if n, err := receiver.frame.ReadAt(recordSizeBuffer, syspack.Offset(offset+1)); err != nil {
		return nil, err
	} else if n < 8 {
		return nil, &ErrorCorruptedRead{Real: n, Expected: 8}
	}
	recordSize := binary.LittleEndian.Uint64(recordSizeBuffer)
	var record = make([]byte, recordSize)
	if n, err := receiver.frame.ReadAt(record, syspack.Offset(offset)+9); err != nil {
		return nil, err
	} else if n < int(recordSize) {
		return nil, &ErrorCorruptedRead{Real: n, Expected: int(recordSize)}
	}
	blockSize := uint64(9 + recordSize)
	if receiver.transaction != nil {
		receiver.transaction.offset += blockSize
		receiver.transaction.count++
	} else {
		if err := receiver.meta.Begin(); err != nil {
			return nil, err
		}
		defer receiver.meta.Rollback()
		if _, _, err := receiver.meta.IncUint64(rxOffset, blockSize); err != nil {
			return nil, err
		}
		if _, _, err := receiver.meta.IncUint64(rxCount, 1); err != nil {
			return nil, err
		}
		if err := receiver.meta.Commit(); err != nil {
			return nil, err
		}
	}
	return record, nil
}

// Receive record.
func (receiver *Receiver) Receive() ([]byte, error) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return nil, &ErrorReceiverClosed{}
	}
	if receiver.frame == nil {
		return nil, &ErrorFrameClosed{}
	}
	return receiver.readFromFrame()
}

// Sync data.
func (receiver *Receiver) Sync() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	return receiver.meta.Sync()
}

// Close receiver.
func (receiver *Receiver) Close() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	if receiver.frame != nil {
		if err := receiver.closeFrame(); err != nil {
			return err
		}
	}
	if err := receiver.meta.Close(); err != nil {
		return err
	}
	receiver.meta = nil
	runtime.SetFinalizer(receiver, nil)
	return nil
}
