package hedgehog

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
)

type GarbageCollector struct {
	// Garbage collector.

	// Storage.
	storage *Storage

	// Transmitter.
	transmitter *Transmitter

	// Receivers.
	receivers map[Port]*Receiver

	// Mutex.
	mutex *sync.Mutex
}

// Make new garbage collector.
func NewGarbageCollector(storage *Storage, transmitter *Transmitter) (*GarbageCollector, error) {
	if storage == nil {
		return nil, &ErrorStorageInvalid{}
	}
	if transmitter == nil || transmitter.storage != storage {
		return nil, &ErrorTransmitterInvalid{}
	}
	gc := &GarbageCollector{
		storage:     storage,
		transmitter: transmitter,
		receivers:   make(map[Port]*Receiver),
		mutex:       &sync.Mutex{},
	}
	runtime.SetFinalizer(gc, (*GarbageCollector).Close)
	return gc, nil
}

// Consider receiver.
func (gc *GarbageCollector) Consider(receiver *Receiver) error {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	if gc.storage == nil {
		return &ErrorGarbageCollectorClosed{}
	}
	if receiver == nil || receiver.storage != gc.storage {
		return &ErrorReceiverInvalid{}
	}
	if _, ok := gc.receivers[receiver.port]; ok {
		return &ErrorPortLinked{Port: receiver.port}
	}
	gc.receivers[receiver.port] = receiver
	return nil
}

// Run garbage collection.
func (gc *GarbageCollector) Run() error {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	if gc.storage == nil {
		return &ErrorGarbageCollectorClosed{}
	}
	directory, err := gc.storage.Directory()
	if err != nil {
		return err
	}
	minFrame := Frame(0)
	if err := gc.transmitter.Sync(); err != nil {
		return err
	}
	if frame, _, _, err := gc.transmitter.State(); err != nil {
		return err
	} else {
		minFrame = frame
	}
	for _, receiver := range gc.receivers {
		if err := receiver.Sync(); err != nil {
			return err
		}
		if frame, _, _, err := receiver.State(false); err != nil {
			return err
		} else if frame < minFrame {
			minFrame = frame
		}
	}
	return filepath.Walk(directory, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			filename := info.Name()
			if filepath.Ext(filename) == ".dat" {
				if frame, err := strconv.ParseUint(filename[:len(filename)-4], 10, 64); err == nil {
					if frame < minFrame {
						if err := os.Remove(filepath.Join(directory, filename)); err != nil {
							return err
						}
					}
				}
			}
		}
		return err
	})
}

// Close garbage collector.
func (gc *GarbageCollector) Close() error {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	if gc.storage == nil {
		return &ErrorGarbageCollectorClosed{}
	}
	gc.receivers = nil
	gc.transmitter = nil
	gc.storage = nil
	runtime.SetFinalizer(gc, nil)
	return nil
}
