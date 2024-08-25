package buffer_manager

import (
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
)

/*
Buffer describes the following things:

1. which page is corresponding to this blk

2. transaction identifier, txNum

3. log serviced provided by LogFileManager, and Write-Read service provided by FileManager

4. pins acts as a reference count, which indicates how many components are using this buffer.
*/
type Buffer struct {
	fm       *fm.FileManager // init
	contents *fm.Page        //init
	blk      *fm.BlockId
	lm       *lm.LogFileManager // init, for recovery
	pins     uint32             // refCount
	txNum    int32              // init, transaction number
	lsn      uint64             // init, log sequence number
}

func NewBuffer(fileManager *fm.FileManager, logManager *lm.LogFileManager) *Buffer {
	return &Buffer{
		fm:    fileManager,
		lm:    logManager,
		txNum: -1,
		lsn:   0,
		// assign a new page to the buffer
		contents: fm.NewPageBySize(fileManager.BlockSize()),
	}
}

func (b *Buffer) Contents() *fm.Page {
	return b.contents
}

func (b *Buffer) Block() *fm.BlockId {
	return b.blk
}

/*
SetModified indicates dirty buffer

TODO: ?

- this method is and must be called when the upper level components MODIFY the data within the buffer.

- if the upper level components just read the data, they should NOT call this method.

@param txNum transaction number

@param lsn log sequence number, used for recovery
*/
func (b *Buffer) SetModified(txNum int32, lsn uint64) {
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

/*
IsPinned check if the buffer is used by other components
*/
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// ModifyingTx return the transaction number of the modifying transaction
func (b *Buffer) ModifyingTx() int32 {
	return b.txNum
}

// AssignToBlock assign the buffer to a block
func (b *Buffer) AssignToBlock(blk *fm.BlockId) {
	//before assignment, flush the buffer into disk

	b.Flush()

	_, err := b.fm.Read(blk, b.contents)
	if err != nil {
		return
	}

	b.blk = blk
	b.pins = 0
}

/*
Flush flush the log and the buffer into disk
*/
func (b *Buffer) Flush() {

	var err error

	if b.txNum > 0 {

		err = b.lm.FlushByLSN(b.lsn) //write back the log
		if err != nil {
			return
		}

		_, err = b.fm.Write(b.blk, b.contents) //write back the buffer
		if err != nil {
			return
		}
		//-1, indicates this transaction is committed
		b.txNum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins += 1
}

func (b *Buffer) Unpin() {
	b.pins -= 1
}
