package logRecord

import (
	"fmt"
	fm "oh_my_godb/file_manager"
	log "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

// <START, 1>  // start transaction 1
const START_STRING_FROMAT = "<START %d>"

type StartRecord struct {
	txNum      uint64
	logManager *log.LogFileManager
}

/*
NewStartRecord  the page contains something like "<START 1>", the start is an uint64
*/
func NewStartRecord(p *fm.Page, logManager *log.LogFileManager) *StartRecord {

	txNum := p.GetInt(tx.UINT64_LEN) //just over the START, the first 64bits
	return &StartRecord{
		txNum:      txNum,
		logManager: logManager,
	}

}

func (s *StartRecord) Op() tx.RECORD_TYPE {
	return tx.START
}

func (s *StartRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *StartRecord) Undo(_ tx.TransactionInterface) {
	return
}

func (s *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", s.txNum)
}

func (s *StartRecord) WriteToLog() (uint64, error) {
	record := make([]byte, 2*tx.UINT64_LEN)
	p := fm.NewPageByBytes(record)
	p.SetInt(uint64(0), uint64(tx.START)) // set the START
	p.SetInt(tx.UINT64_LEN, s.txNum)      // set txNum

	return s.logManager.AppendLogRecordIntoPage(record)
}
