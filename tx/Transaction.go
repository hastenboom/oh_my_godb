package tx

import (
	"errors"
	"fmt"
	"log"
	bm "oh_my_godb/buffer_manager"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"sync"
)

var tx_num_mu sync.Mutex

var nextTxNum = int32(0)

func getNextTxNum() int32 {
	tx_num_mu.Lock()
	defer tx_num_mu.Unlock()
	nextTxNum += 1
	return nextTxNum
}

type Transaction struct {
	//concurMgr *ConcurrencyManager
	recoveryMgr *RecoveryManager
	fileMgr     *fm.FileManager
	logMgr      *lm.LogFileManager
	bufferMgr   *bm.BufferManager
	myBuffers   *BufferList
	txNum       int32
}

func (t *Transaction) RollBack() error {
	//TODO implement me
	panic("implement me")
}

func NewTransaction(
	fileMgr *fm.FileManager,
	logMgr *lm.LogFileManager,
	bufferMgr *bm.BufferManager) *Transaction {

	tx := &Transaction{
		fileMgr:   fileMgr,
		logMgr:    logMgr,
		bufferMgr: bufferMgr,
		myBuffers: NewBufferList(bufferMgr),
		txNum:     getNextTxNum(),
	}

	//TODO: create concurMgr
	//TODO: create recoveryMgr

	return tx
}

func (t *Transaction) Commit() {
	t.recoveryMgr.Commit()
	r := fmt.Sprintf("transaction %d committed\n", t.txNum)
	log.Printf(r)

	t.myBuffers.UnpinAll()
}

func (t *Transaction) Rollback() error {
	err := t.recoveryMgr.Rollback()
	if err != nil {
		return err
	}

	r := fmt.Sprintf("transaction %d rolled back\n", t.txNum)
	log.Printf(r)

	//TODO: release the concurMgr

	t.myBuffers.UnpinAll()

	return nil
}

/*
Recover once the system shut down unexpectedly, the DBMS uses this to recover the DB state.
*/
func (t *Transaction) Recover() {
	t.bufferMgr.FlushAll(t.txNum)
	t.recoveryMgr.Recover()
}

func (t *Transaction) Pin(blk *fm.BlockId) {
	t.myBuffers.Pin(blk)
}

func (t *Transaction) Unpin(blk *fm.BlockId) {
	t.myBuffers.Unpin(blk)
}

func (t *Transaction) bufferNotExist(blk *fm.BlockId) error {
	errStr := fmt.Sprintf("No buffer found for given blk %d in the file: %s\n",
		blk.BlkNum(), blk.GetFilePath())

	log.Fatal(errStr)
	return errors.New(errStr)
}

func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) (uint64, error) {
	// TODO: use the concurMgr to add `shared lock`, read lock
	// TODO: the lock shouldn't be explicitly released
	buff := t.myBuffers.getBuffer(blk)
	if buff == nil {
		return -1, t.bufferNotExist(blk)
	}

	return buff.Contents().GetInt(offset), nil
}

func (t *Transaction) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	// TODO: use the concurMgr to add `shared lock`, read lock

	buff := t.myBuffers.getBuffer(blk)
	if buff == nil {
		return "", t.bufferNotExist(blk)
	}

	return buff.Contents().GetString(offset), nil
}

func (t *Transaction) SetInt(blk *fm.BlockId, offset uint64, val uint64, okToLog bool) error {
	// TODO: use the concurMgr to add `exclusive lock`, write lock

	buff := t.myBuffers.getBuffer(blk)
	if buff == nil {
		return t.bufferNotExist(blk)
	}

	var lsn uint64
	var err error

	if okToLog {
		lsn, err = t.recoveryMgr.SetInt(buff, offset, uint64(val))
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetInt(offset, uint64(val))
	buff.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	// TODO: use the concurMgr to add `exclusive lock`, write lock

	buff := t.myBuffers.getBuffer(blk)
	if buff == nil {
		return t.bufferNotExist(blk)
	}

	var lsn uint64
	var err error

	if okToLog {
		lsn, err = t.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) Size(fileName string) uint64 {
	// TODO: shared Lock
	//dummyBlk := fm.NewBlockId(fileName, uint64(EOF))
	// t.concurMgr.Slock(dummyBlk

	s, _ := t.fileMgr.BlockNum(fileName)

	return s
}

func (t *Transaction) Append(fileName string) *fm.BlockId {
	// TODO: use the concurMgr to add `exclusive lock`, write lock
	// dummyBlk:=fm.NewBlockId(fileName, uint64(EOF))
	// t.concurMgr.Xlock(dummyBlk)

	blk, err := t.fileMgr.Append(fileName)

	if err != nil {
		return nil
	}

	return &blk
}

func (t *Transaction) BlockSize() uint64 {
	return t.fileMgr.BlockSize()
}

func (t *Transaction) AvailableBuffers() uint64 {
	return uint64(t.bufferMgr.Available())
}
