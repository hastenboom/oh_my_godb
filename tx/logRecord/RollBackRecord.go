package logRecord

import (
	"fmt"
	fm "oh_my_godb/file_manager"
	lg "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

type RollBackRecord struct {
	tx_num uint64
}

func NewRollBackRecord(p *fm.Page) *RollBackRecord {
	return &RollBackRecord{
		tx_num: p.GetInt(tx.UINT64_LEN),
	}
}

func (r *RollBackRecord) Op() tx.RECORD_TYPE {
	return tx.ROLLBACK
}

func (r *RollBackRecord) TxNumber() uint64 {
	return r.tx_num
}

func (r *RollBackRecord) Undo(_ tx.TransactionInterface) {
	//它没有回滚操作
}

func (r *RollBackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.tx_num)
}

func WriteRollBackLog(lgmr *lg.LogFileManager, tx_num uint64) (uint64, error) {
	rec := make([]byte, 2*tx.UINT64_LEN)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(tx.ROLLBACK))
	p.SetInt(tx.UINT64_LEN, tx_num)

	return lgmr.AppendLogRecordIntoPage(rec)
}
