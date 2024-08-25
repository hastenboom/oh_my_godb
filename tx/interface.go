package tx

import fm "oh_my_godb/file_manager"

type TransactionInterface interface {
	Commit()
	Rollback() error
	Recover()
	Pin(blk *fm.BlockId)
	Unpin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) (uint64, error)
	GetString(blk *fm.BlockId, offset uint64) (string, error)
	SetInt(blk *fm.BlockId, offset uint64, val uint64, okToLog bool) error
	SetString(blk *fm.BlockId, offset uint64, value string, okToLog bool) error
	AvailableBuffers() uint64
	Size(fileName string) uint64
	Append(fileName string) *fm.BlockId
	BlockSize() uint64
}
type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const (
	UINT64_LEN = uint64(8)
	EOF        = -1
)

type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxNumber() uint64
	Undo(tx TransactionInterface)
	ToString() string
}
