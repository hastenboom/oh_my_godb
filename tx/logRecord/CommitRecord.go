package logRecord

import (
	"fmt"
	fm "oh_my_godb/file_manager"
	lg "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

type CommitRecord struct {
	tx_num uint64
}

func NewCommitRecord(p *fm.Page) *CommitRecord {
	return &CommitRecord{
		tx_num: p.GetInt(tx.UINT64_LEN),
	}
}

func (r *CommitRecord) Op() tx.RECORD_TYPE {
	return tx.COMMIT
}

func (r *CommitRecord) TxNumber() uint64 {
	return r.tx_num
}

func (r *CommitRecord) Undo(_ tx.TransactionInterface) {
	//它没有回滚操作
}

func (r *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", r.tx_num)
}

func WriteCommitRecordLog(lgmr *lg.LogFileManager, tx_num uint64) (uint64, error) {
	rec := make([]byte, 2*tx.UINT64_LEN)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(tx.COMMIT))
	p.SetInt(tx.UINT64_LEN, tx_num)

	return lgmr.AppendLogRecordIntoPage(rec)
}
