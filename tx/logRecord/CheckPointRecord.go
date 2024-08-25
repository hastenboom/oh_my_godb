package logRecord

import (
	"math"
	fm "oh_my_godb/file_manager"
	lg "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

type CheckPointRecord struct {
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (c *CheckPointRecord) Op() tx.RECORD_TYPE {
	return tx.CHECKPOINT
}

func (c *CheckPointRecord) TxNumber() uint64 {
	return math.MaxUint64 //它没有对应的交易号
}

func (c *CheckPointRecord) Undo(_ tx.TransactionInterface) {
	return
}

func (c *CheckPointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckPointToLog(lgmr *lg.LogFileManager) (uint64, error) {
	rec := make([]byte, tx.UINT64_LEN)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(tx.CHECKPOINT))
	return lgmr.AppendLogRecordIntoPage(rec)
}
