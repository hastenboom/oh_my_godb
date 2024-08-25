package logRecord

import (
	"fmt"
	fm "oh_my_godb/file_manager"
	lg "oh_my_godb/log_manager"
	"oh_my_godb/tx"
)

type SetIntRecord struct {
	txNum  uint64
	offset uint64
	value  uint64
	blk    *fm.BlockId
}

func NewSetIntRecord(p *fm.Page) *SetIntRecord {

	txNumPos := tx.UINT64_LEN
	txNum := p.GetInt(txNumPos)

	fileNamePos := txNumPos + tx.UINT64_LEN
	filename := p.GetString(fileNamePos)

	blkNumPos := fileNamePos + fm.MaxLengthForStr(filename)
	blkNum := p.GetInt(blkNumPos)
	blk := fm.NewBlockId(filename, blkNum)

	offsetPos := blkNumPos + tx.UINT64_LEN
	offset := p.GetInt(offsetPos)

	valuePos := offsetPos + tx.UINT64_LEN
	value := p.GetInt(valuePos)

	return &SetIntRecord{
		txNum:  txNum,
		offset: offset,
		value:  value,
		blk:    blk,
	}
}

func (s *SetIntRecord) Op() tx.RECORD_TYPE {
	return tx.SETINT
}

func (s *SetIntRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetIntRecord) ToString() string {
	str := fmt.Sprintf("<SETINT %d %d %d %d>", s.txNum, s.blk.BlkNum(),
		s.offset, s.value)

	return str
}

func (s *SetIntRecord) Undo(tx tx.TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, s.value, false) //将原来的字符串写回去
	tx.Unpin(s.blk)
}

func WriteSetIntLog(log_manager *lg.LogFileManager, tx_num uint64,
	blk *fm.BlockId, offset uint64, val uint64) (uint64, error) {

	tpos := tx.UINT64_LEN
	fpos := tpos + tx.UINT64_LEN
	p := fm.NewPageBySize(1)
	bpos := fpos + fm.MaxLengthForStr(blk.GetFilePath())
	opos := bpos + tx.UINT64_LEN
	vpos := opos + tx.UINT64_LEN
	rec_len := vpos + tx.UINT64_LEN
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(tx.SETSTRING))
	p.SetInt(tpos, tx_num)
	p.SetString(fpos, blk.GetFilePath())
	p.SetInt(bpos, blk.BlkNum())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return log_manager.AppendLogRecordIntoPage(rec)
}
