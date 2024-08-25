package log_manager

import fm "oh_my_godb/file_manager"

type LogIterator struct {
	fileManager  *fm.FileManager
	blockId      *fm.BlockId // const
	logPage      *fm.Page
	curPos       uint64
	whereToWrite uint64
}

func NewLogIterator(fileManager *fm.FileManager, blockId *fm.BlockId) *LogIterator {
	it := LogIterator{
		fileManager: fileManager,
		blockId:     blockId,
	}

	it.logPage = fm.NewPageBySize(fileManager.BlockSize())
	err := it.moveToBlock(blockId)

	if err != nil {
		return nil
	}
	return &it

}

// map the logPage to the blockId
func (it *LogIterator) moveToBlock(blockId *fm.BlockId) error {
	var err error

	_, err = it.fileManager.Read(blockId, it.logPage) //mmap

	if err != nil {
		return err
	}

	/*
			|whereToWrite|empty|.....|empty|LR1Len|LR1      |LR0Len|LR0     |
			|8B          |empty|.....|empty|8B    |LR1Len B |8B    |LR0Len B|
		                                   ⬆ whereToWrite, curPos
	*/
	it.whereToWrite = it.logPage.GetInt(0)
	it.curPos = it.whereToWrite

	return nil
}

/*
Next get the next log record from the page.

LogPage and LogBlock layout:
|whereToWrite|LRNLen|LRN     |.....|LR1Len|LR1      |LR0Len|LR0     |
|8B          |8B    |LRNLen B|.....|8B    |LR1Len B |8B    |LR0Len B|

- LogRecord is written in reversed order, but we read it in forward order.
- The greater the N, the newer the log record.
*/
func (it *LogIterator) Next() []byte {

	// the end of the log page, move to the next block
	if it.curPos == it.fileManager.BlockSize() {
		err := it.moveToBlock(fm.NewBlockId(it.blockId.GetFilePath(), it.blockId.BlkNum()-1))
		if err != nil {
			return nil
		}
	}
	/*
			|whereToWrite|empty|.....|empty|LR1Len|LR1      |LR0Len|LR0     |
			|8B          |empty|.....|empty|8B    |LR1Len B |8B    |LR0Len B|
		                                          |         |
		                                          - Record  -
		                                    ⬆ oldCurPos      ⬆ newCurPos

	*/
	record := it.logPage.GetBytes(it.curPos)
	it.curPos += UINT64_LEN + it.logPage.GetInt(it.curPos) //update the curPos

	return record
}

func (it *LogIterator) HasNext() bool {
	return it.curPos < it.fileManager.BlockSize() || it.blockId.BlkNum() > 0
}
