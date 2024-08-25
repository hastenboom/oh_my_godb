package tx

import (
	bm "oh_my_godb/buffer_manager"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"oh_my_godb/tx/logRecord"
)

type RecoveryManager struct {
	logMgr    *lm.LogFileManager
	bufferMgr *bm.BufferManager
	tx        *Transaction
	txNum     int32
}

func NewRecoveryManager(
	tx *Transaction, logMgr *lm.LogFileManager,
	bufferMgr *bm.BufferManager, txNum int32) *RecoveryManager {
	rm := &RecoveryManager{
		logMgr:    logMgr,
		bufferMgr: bufferMgr,
		tx:        tx,
		txNum:     txNum,
	}

	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(0, uint64(txNum))
	startRecord := logRecord.NewStartRecord(p, logMgr)
	_, err := startRecord.WriteToLog()
	if err != nil {
		return nil
	}

	return rm
}

func (r *RecoveryManager) Commit() error {
	r.bufferMgr.FlushAll(r.txNum)

	lsn, err := logRecord.WriteCommitRecordLog(r.logMgr, uint64(r.txNum))
	if err != nil {
		return err
	}

	err = r.logMgr.FlushByLSN(lsn)
	if err != nil {
		return err
	}

	return nil
}

func (r *RecoveryManager) Rollback() error {

	r.doRollback()

	r.bufferMgr.FlushAll(r.txNum)

	lsn, err := logRecord.WriteRollBackLog(r.logMgr, uint64(r.txNum))
	if err != nil {
		return err
	}

	err = r.logMgr.FlushByLSN(lsn)
	if err != nil {
		return err
	}

	return nil
}

// Recover if found the START but not no COMMIT found, the DBMS should automatically call Recover()
func (r *RecoveryManager) Recover() error {
	r.doRecover()

	r.bufferMgr.FlushAll(r.txNum)
	//CheckPoint indicates the DBMS that Recovery() is used
	lsn, err := logRecord.WriteCheckPointToLog(r.logMgr)
	if err != nil {
		return err
	}

	err = r.logMgr.FlushByLSN(lsn)
	if err != nil {
		return err
	}

	return nil
}

func (r *RecoveryManager) SetInt(buffer *bm.Buffer, offset uint64, value uint64) (uint64, error) {

	oldVal := buffer.Contents().GetInt(offset)
	blk := buffer.Block()

	buffer.Contents().SetInt(offset, value) //redundant

	return logRecord.WriteSetIntLog(r.logMgr, uint64(r.txNum), blk, offset, oldVal)

}

func (r *RecoveryManager) SetString(buffer *bm.Buffer, offset uint64, value string) (uint64, error) {

	oldVal := buffer.Contents().GetString(offset)
	blk := buffer.Block()

	buffer.Contents().SetString(offset, value) //redundant, consider to remove this statement

	return logRecord.WriteSetStringLog(r.logMgr, uint64(r.txNum), blk, offset, oldVal)

}

func (r *RecoveryManager) CreateRecord(bytes []byte) LogRecordInterface {
	page := fm.NewPageByBytes(bytes)
	switch RECORD_TYPE(page.GetInt(0)) {
	case CHECKPOINT:
		return logRecord.NewCheckPointRecord()
	case START:
		return logRecord.NewStartRecord(page, r.logMgr)
	case COMMIT:
		return logRecord.NewCommitRecord(page)
	case ROLLBACK:
		return logRecord.NewRollBackRecord(page)
	case SETINT:
		return logRecord.NewSetIntRecord(page)
	case SETSTRING:
		return logRecord.NewSetStringRecord(page)
	default:
		panic("unknown record type")
	}
}

/*
Aim for a specific txn
*/
func (r *RecoveryManager) doRollback() {
	iter := r.logMgr.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		logRecord := r.CreateRecord(rec)
		if logRecord.TxNumber() != uint64(r.txNum) {
			if logRecord.Op() == START {
				return
			}
			logRecord.Undo(r.tx)
		}
	}

}

/*
Aim for all txns
*/
func (r *RecoveryManager) doRecover() {

	finishedTxSet := make(map[uint64]bool)

	iter := r.logMgr.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		logRecord := r.CreateRecord(rec)

		/*TODO: ????*/
		if logRecord.Op() == COMMIT || logRecord.Op() == ROLLBACK {
			finishedTxSet[logRecord.TxNumber()] = true
		}

		/*这个tx只有start而没有commit或rollback，说明是未完成的tx，需要回滚*/
		existed, ok := finishedTxSet[logRecord.TxNumber()]
		if !ok || !existed {
			logRecord.Undo(r.tx)
		}
	}

}
