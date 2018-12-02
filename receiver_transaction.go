package hadgehog

import "io"

// Receiver transaction.
type receiverTransaction struct{ frame, offset, count uint64 }

// Start transaction.
func (receiver *Receiver) Begin() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	if receiver.transaction != nil {
		return &ErrorTransactionStarted{}
	}
	frame, err := receiver.meta.GetUint64(rxFrame)
	if err != nil {
		return err
	}
	offset, err := receiver.meta.GetUint64(rxOffset)
	if err != nil {
		return err
	}
	count, err := receiver.meta.GetUint64(rxCount)
	if err != nil {
		return err
	}
	receiver.transaction = &receiverTransaction{frame: frame, offset: offset, count: count}
	return nil
}

// Rollback transaction.
func (receiver *Receiver) Rollback() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	if receiver.transaction == nil {
		return &ErrorTransactionNotStarted{}
	}
	receiver.transaction = nil
	if err := receiver.openFrame(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// Commit transaction.
func (receiver *Receiver) commit() error {
	if receiver.transaction == nil {
		return &ErrorTransactionNotStarted{}
	}
	if err := receiver.meta.Begin(); err != nil {
		return err
	}
	defer receiver.meta.Rollback()
	if err := receiver.meta.SetUint64(rxFrame, receiver.transaction.frame); err != nil {
		return err
	}
	if err := receiver.meta.SetUint64(rxOffset, receiver.transaction.offset); err != nil {
		return err
	}
	if err := receiver.meta.SetUint64(rxCount, receiver.transaction.count); err != nil {
		return err
	}
	if err := receiver.meta.Commit(); err != nil {
		return err
	}
	receiver.transaction = nil
	return nil
}

// Commit transaction.
func (receiver *Receiver) Commit() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	return receiver.commit()
}

// Commit transaction and sync data.
func (receiver *Receiver) Persist() error {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.meta == nil {
		return &ErrorReceiverClosed{}
	}
	if err := receiver.commit(); err != nil {
		return err
	}
	return receiver.meta.Sync()
}
