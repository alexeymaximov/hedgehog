package hedgehog

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/alexeymaximov/syspack"
)

type Storage struct {
	// Storage.

	// Path to base directory which contains:
	// META.tx - transmitter metadata file;
	// META-00000.rx - receiver metadata file where 00000 is receiver port;
	// 00000000000000000000.dat - data frame file where 000...0 is frame index.
	directory string

	// Lock file.
	// This file will be created exclusively on storage initialization and removed on it's closing.
	lock *os.File

	// Mutex.
	mutex *sync.RWMutex
}

// Make new storage.
func NewStorage(directory string) (*Storage, error) {
	if info, err := os.Stat(directory); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.MkdirAll(directory, 0700); err != nil {
			return nil, err
		}
	} else if !info.IsDir() {
		return nil, &ErrorBadDirectory{Path: directory}
	}
	storage := &Storage{directory: directory, mutex: &sync.RWMutex{}}
	var err error
	lockPath := filepath.Join(storage.directory, "LOCK")
	storage.lock, err = os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil, &ErrorLocked{Path: storage.directory}
		}
		return nil, err
	}
	runtime.SetFinalizer(storage, (*Storage).Close)
	return storage, nil
}

// Get path to base directory.
func (storage *Storage) Directory() (string, error) {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()
	if storage.lock == nil {
		return "", &ErrorStorageClosed{}
	}
	return storage.directory, nil
}

// Get path to transmitter metadata file.
func (storage *Storage) Transmitter() (string, error) {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()
	if storage.lock == nil {
		return "", &ErrorStorageClosed{}
	}
	return filepath.Join(storage.directory, "META.tx"), nil
}

// Get path to receiver metadata file.
func (storage *Storage) Receiver(port Port) (string, error) {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()
	if storage.lock == nil {
		return "", &ErrorStorageClosed{}
	}
	return filepath.Join(storage.directory, "META-"+fmt.Sprintf("%05d", port)+".rx"), nil
}

// Get path to data frame file.
func (storage *Storage) Frame(frame Frame) (string, error) {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()
	if storage.lock == nil {
		return "", &ErrorStorageClosed{}
	}
	return filepath.Join(storage.directory, fmt.Sprintf("%020d", frame)+".dat"), nil
}

// Get total size.
func (storage *Storage) Size() (syspack.Size, error) {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()
	if storage.lock == nil {
		return 0, &ErrorStorageClosed{}
	}
	size := syspack.Size(0)
	err := filepath.Walk(storage.directory, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += syspack.Size(info.Size())
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Close storage.
func (storage *Storage) Close() error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	if storage.lock == nil {
		return &ErrorStorageClosed{}
	}
	lockPath := storage.lock.Name()
	if err := storage.lock.Close(); err != nil {
		return err
	}
	storage.lock = nil
	runtime.SetFinalizer(storage, nil)
	if err := os.Remove(lockPath); err != nil {
		return err
	}
	return nil
}
