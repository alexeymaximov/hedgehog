package hadgehog

import (
	"fmt"

	"github.com/alexeymaximov/syspack"
)

// Error occurred when hedgehog closed.
type ErrorClosed struct{}

// Get error message.
func (err *ErrorClosed) Error() string {
	return "hedgehog: closed"
}

// Error occurred on read corruption.
type ErrorCorruptedRead struct{ Real, Expected int }

// Get error message.
func (err *ErrorCorruptedRead) Error() string {
	return fmt.Sprintf("hedgehog: read %d bytes instead of %d", err.Real, err.Expected)
}

// Error occurred on write corruption.
type ErrorCorruptedWrite struct{ Real, Expected int }

// Get error message.
func (err *ErrorCorruptedWrite) Error() string {
	return fmt.Sprintf("hedgehog: write %d bytes instead of %d", err.Real, err.Expected)
}

// Error occurred when directory is bad.
type ErrorBadDirectory struct{ Path string }

// Get error message.
func (err *ErrorBadDirectory) Error() string {
	return fmt.Sprintf("hedgehog: bad directory %s", err.Path)
}

// Error occurred when data frame closed.
type ErrorFrameClosed struct{}

// Get error message.
func (err *ErrorFrameClosed) Error() string {
	return "hedgehog: data frame closed"
}

// Error occurred when data frame size too large.
type ErrorFrameTooLarge struct{ Size syspack.Size }

// Get error message.
func (err *ErrorFrameTooLarge) Error() string {
	return fmt.Sprintf("hedgehog: data frame of %d bytes size too large", err.Size)
}

// Error occurred when garbage collector closed.
type ErrorGarbageCollectorClosed struct{}

// Get error message.
func (err *ErrorGarbageCollectorClosed) Error() string {
	return "hedgehog: garbage collector closed"
}

// Error occurred when lock file already exists.
type ErrorLocked struct{ Path string }

// Get error message.
func (err *ErrorLocked) Error() string {
	return fmt.Sprintf("hedgehog: %s locked", err.Path)
}

// Error occurred when receiver port already linked.
type ErrorPortLinked struct{ Port Port }

// Get error message.
func (err *ErrorPortLinked) Error() string {
	return fmt.Sprintf("hedgehog: receiver port %d already linked", err.Port)
}

// Error occurred when receiver port is unknown.
type ErrorPortUnknown struct{ Port Port }

// Get error message.
func (err *ErrorPortUnknown) Error() string {
	return fmt.Sprintf("hedgehog: unknown receiver port %d", err.Port)
}

// Error occurred when receiver closed.
type ErrorReceiverClosed struct{}

// Get error message.
func (err *ErrorReceiverClosed) Error() string {
	return "hedgehog: receiver closed"
}

// Error occurred when receiver is invalid.
type ErrorReceiverInvalid struct{}

// Get error message.
func (err *ErrorReceiverInvalid) Error() string {
	return "hedgehog: invalid receiver"
}

// Error occurred when record size is greater than data frame size.
type ErrorRecordTooLarge struct{ Size syspack.Size }

// Get error message.
func (err *ErrorRecordTooLarge) Error() string {
	return fmt.Sprintf("hedgehog: record of %d bytes size too large", err.Size)
}

// Error occurred when data storage closed.
type ErrorStorageClosed struct{}

// Get error message.
func (err *ErrorStorageClosed) Error() string {
	return "hedgehog: storage closed"
}

// Error occurred when storage is invalid.
type ErrorStorageInvalid struct{}

// Get error message.
func (err *ErrorStorageInvalid) Error() string {
	return "hedgehog: invalid storage"
}

// Error occurred when transaction not started.
type ErrorTransactionNotStarted struct{}

// Get error message.
func (err *ErrorTransactionNotStarted) Error() string {
	return "hedgehog: transaction not started"
}

// Error occurred when transaction already started.
type ErrorTransactionStarted struct{}

// Get error message.
func (err *ErrorTransactionStarted) Error() string {
	return "hedgehog: transaction already started"
}

// Error occurred when transmitter closed.
type ErrorTransmitterClosed struct{}

// Get error message.
func (err *ErrorTransmitterClosed) Error() string {
	return "hedgehog: transmitter closed"
}

// Error occurred when transmitter is invalid.
type ErrorTransmitterInvalid struct{}

// Get error message.
func (err *ErrorTransmitterInvalid) Error() string {
	return "hedgehog: invalid transmitter"
}
