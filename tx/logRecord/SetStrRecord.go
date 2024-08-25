package logRecord

import (
	"fmt"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

/*
 <SETSTRING, 2, testfile, 1, 40, one, two>

This can be transformed into
 <SETSTRING 2 testfile 1 40 one>
 <SETSTRING 2 testfile 1 40 two>

 <OP TxNum FileName BlkNum Offset Value>
*/

const SET_STRING_RECORD_FORMAT = "<SETSTRING %d %d %d %s>"

type SetStringRecord struct {
	txNum  uint64
	offset uint64 // the offset of the record in the block
	value  string
	blk    *fm.BlockId
}

/*
NewSetStringRecord the page's layout is:

| txNum | fileName | blkNum | offset | value |
*/
func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	txNumPos := tx.UINT64_LEN
	txNum := p.GetInt(txNumPos)

	fileNamePos := txNumPos + tx.UINT64_LEN
	fileName := p.GetString(fileNamePos)

	blkNumPos := fileNamePos + fm.MaxLengthForStr(fileName)
	blkNum := p.GetInt(blkNumPos)

	offsetPos := blkNumPos + tx.UINT64_LEN
	offset := p.GetInt(offsetPos)

	valuePos := offsetPos + tx.UINT64_LEN
	value := p.GetString(valuePos)

	return &SetStringRecord{
		txNum:  txNum,
		offset: offset,
		value:  value,
		blk:    fm.NewBlockId(fileName, blkNum),
	}
}

func (s *SetStringRecord) Op() tx.RECORD_TYPE {
	return tx.SETSTRING
}

func (s *SetStringRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %d %d %s>", s.txNum, s.blk.BlkNum(), s.offset, s.value)
	return str
}

func (s *SetStringRecord) Undo(tx tx.TransactionInterface) {
	tx.Pin(s.blk)
	//the tx will use this info to roll back
	tx.SetString(s.blk, s.offset, s.value, false)
	tx.Unpin(s.blk)
}

func WriteSetStringLog(
	lm *lm.LogFileManager, txNum uint64,
	blk *fm.BlockId, offset uint64, value string) (uint64, error) {

	txNumPos := tx.UINT64_LEN

	fileNamePos := txNumPos + tx.UINT64_LEN

	page := fm.NewPageBySize(1)

	blockNumPos := fileNamePos + fm.MaxLengthForStr(blk.GetFilePath())

	offsetPos := blockNumPos + tx.UINT64_LEN

	valuePos := offsetPos + tx.UINT64_LEN

	rec_len := valuePos + fm.MaxLengthForStr(value)
	rec := make([]byte, rec_len)

	page = fm.NewPageByBytes(rec)
	page.SetInt(0, uint64(tx.SETSTRING))
	page.SetInt(txNumPos, txNum)
	page.SetString(fileNamePos, blk.GetFilePath())
	page.SetInt(blockNumPos, blk.BlkNum())
	page.SetInt(offsetPos, offset)
	page.SetString(valuePos, value)

	return lm.AppendLogRecordIntoPage(rec)
}
