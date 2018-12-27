package hedgehog

import (
	"runtime"
	"sync"

	"github.com/alexeymaximov/syspack"
)

// Data frame index.
type Frame = uint64

// Receiver port.
type Port = uint16

type Hedgehog struct {
	// Hedgehog.

	// Storage.
	storage *Storage

	// Transmitter.
	transmitter *Transmitter

	// Receivers.
	receivers map[Port]*Receiver

	// Garbage collector.
	gc *GarbageCollector

	// Mutex.
	mutex *sync.RWMutex
}

// Make new caravan.
func NewHedgehog(directory string, frameSize syspack.Size) (*Hedgehog, error) {
	caravan := &Hedgehog{receivers: make(map[Port]*Receiver), mutex: &sync.RWMutex{}}
	var err error
	caravan.storage, err = NewStorage(directory)
	if err != nil {
		return nil, err
	}
	caravan.transmitter, err = NewTransmitter(caravan.storage, frameSize)
	if err != nil {
		caravan.storage.Close()
		return nil, err
	}
	caravan.gc, err = NewGarbageCollector(caravan.storage, caravan.transmitter)
	if err != nil {
		caravan.transmitter.Close()
		caravan.storage.Close()
		return nil, err
	}
	runtime.SetFinalizer(caravan, (*Hedgehog).Close)
	return caravan, nil
}

// Get storage.
func (caravan *Hedgehog) Storage() (*Storage, error) {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.storage == nil {
		return nil, &ErrorClosed{}
	}
	return caravan.storage, nil
}

// Get transmitter.
func (caravan *Hedgehog) Transmitter() (*Transmitter, error) {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.transmitter == nil {
		return nil, &ErrorClosed{}
	}
	return caravan.transmitter, nil
}

// Link receiver.
func (caravan *Hedgehog) Link(port Port) (*Receiver, error) {
	caravan.mutex.Lock()
	defer caravan.mutex.Unlock()
	if caravan.storage == nil || caravan.transmitter == nil || caravan.receivers == nil || caravan.gc == nil {
		return nil, &ErrorClosed{}
	}
	if _, ok := caravan.receivers[port]; ok {
		return nil, &ErrorPortLinked{Port: port}
	}
	frame, _, _, err := caravan.transmitter.State()
	if err != nil {
		return nil, err
	}
	receiver, err := NewReceiver(caravan.storage, port, frame)
	if err != nil {
		return nil, err
	}
	if err := caravan.gc.Consider(receiver); err != nil {
		receiver.Close()
		return nil, err
	}
	caravan.receivers[port] = receiver
	return receiver, nil
}

// Get receiver.
func (caravan *Hedgehog) Receiver(port Port) (*Receiver, error) {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.receivers == nil {
		return nil, &ErrorClosed{}
	}
	receiver, ok := caravan.receivers[port]
	if !ok {
		return nil, &ErrorPortUnknown{Port: port}
	}
	return receiver, nil
}

// Get garbage collector.
func (caravan *Hedgehog) GC() (*GarbageCollector, error) {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.gc == nil {
		return nil, &ErrorClosed{}
	}
	return caravan.gc, nil
}

// Transmit record.
func (caravan *Hedgehog) Transmit(record []byte) error {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.transmitter == nil {
		return &ErrorClosed{}
	}
	return caravan.transmitter.Transmit(record)
}

// Receive record.
func (caravan *Hedgehog) Receive(port Port) ([]byte, error) {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.receivers == nil {
		return nil, &ErrorClosed{}
	}
	receiver, ok := caravan.receivers[port]
	if !ok {
		return nil, &ErrorPortUnknown{Port: port}
	}
	return receiver.Receive()
}

// Cleanup data.
func (caravan *Hedgehog) Cleanup() error {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.gc == nil {
		return &ErrorClosed{}
	}
	return caravan.gc.Run()
}

// Sync data.
func (caravan *Hedgehog) Sync() error {
	caravan.mutex.RLock()
	defer caravan.mutex.RUnlock()
	if caravan.transmitter == nil || caravan.receivers == nil {
		return &ErrorClosed{}
	}
	if err := caravan.transmitter.Sync(); err != nil {
		return err
	}
	for _, receiver := range caravan.receivers {
		if err := receiver.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close caravan.
func (caravan *Hedgehog) Close() error {
	caravan.mutex.Lock()
	defer caravan.mutex.Unlock()
	if caravan.storage == nil {
		return &ErrorClosed{}
	}
	if caravan.gc != nil {
		if err := caravan.gc.Close(); err != nil {
			return err
		}
		caravan.gc = nil
	}
	if caravan.receivers != nil {
		for id, receiver := range caravan.receivers {
			if err := receiver.Close(); err != nil {
				return err
			}
			delete(caravan.receivers, id)
		}
		caravan.receivers = nil
	}
	if caravan.transmitter != nil {
		if err := caravan.transmitter.Close(); err != nil {
			return err
		}
		caravan.transmitter = nil
	}
	if err := caravan.storage.Close(); err != nil {
		return err
	}
	caravan.storage = nil
	runtime.SetFinalizer(caravan, nil)
	return nil
}
