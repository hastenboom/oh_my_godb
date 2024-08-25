package log_manager

import (
	fm "oh_my_godb/file_manager"
	"sync"
)

const (
	UINT64_LEN = 8
)

/*
LogFile Structure:
|Block0|Block1|Block2|...|BlockN|
    ↓ expand
The LogFile Block Structure:
|whereToWrite|LRNLen|LRN     |.....|LR1Len|LR1      |LR0Len|LR0     |
|8B          |8B    |LRNLen B|.....|8B    |LR1Len B |8B    |LR0Len B|

The mapped logPage has the same structure:

|whereToWrite|LRNLen|LRN     |.....|LR1Len|LR1      |LR0Len|LR0     |
|8B          |8B    |LRNLen B|.....|8B    |LR1Len B |8B    |LR0Len B|

- notice the LogRecord is written in reverse order
- suppose the whereToWrite is 400( appendNewBlockAndMmap() ), and the logRecordX is 100 bytes
- then this logRecordX should be written in offset[300,399]
- the whereToWrite should be updated as 300
- TODO: the logFile is not concurrent. And no matter how many blocks the file contains, the logManager always use only one memory Page to handle it;

------------------------------------------------------------------------------------

Snippet of the completed LogFile layout:
|Block0| 									  |Block1|
|whereToWrite|LRN-2Len|LRN-2|LRN-3Len|LRN-3|  |whereToWrite|LRNLen|LRN|LRN-1Len|LRN-1|

newest to oldest:
LRN -> LRN-1 -> LRN-2 -> LRN-3 -> ...

*/

/*
LogFileManager treated it as the child of the fileManager.
FIXME: the design of the latestLSN and lastSavedLSN are buggy and confusing.
*/
type LogFileManager struct {
	fileManager  *fm.FileManager // !the ref is const
	logFileName  string          // !the ref is const, fileName of the log file
	logPage      *fm.Page        // !the ref is const, cache
	currentBlk   *fm.BlockId     // current blockId being written to, will only be updated in AppendLogRecordIntoPage()
	latestLSN    uint64          // LSN, the last sequence number of the log file, also the number of log records
	lastSavedLSN uint64          // LSN, the last sequence number of the log file that has been saved to disk
	mutex        *sync.Mutex     // !the ref is const
}

func NewLogManagerWithConfig(dbPath string, blockSize uint64, logFileName string) (*LogFileManager, error) {
	fileManager, err := fm.NewFileManager(dbPath, blockSize)
	if err != nil {
		return nil, err
	}
	return NewLogManager(fileManager, logFileName)
}

func NewLogManager(fileManager *fm.FileManager, logFileName string) (*LogFileManager, error) {
	logManager := LogFileManager{
		fileManager:  fileManager,
		logFileName:  logFileName,
		logPage:      fm.NewPageBySize(fileManager.BlockSize()),
		latestLSN:    0,
		lastSavedLSN: 0,
		mutex:        new(sync.Mutex),
	}

	logBlockNum, err := fileManager.BlockNum(logFileName)

	if err != nil {
		return nil, err
	}

	//handle the curBlk, map it into the logPage
	if logBlockNum == 0 {
		/*
			if the logFile is empty, create a new blockId for it
			|empty| -> |Block0|
						 ⬆ currentBlk
		*/
		blockId, err := logManager.appendNewBlockAndMmap()
		if err != nil {
			return nil, err
		}
		logManager.currentBlk = blockId
	} else {
		/*
			if the logFile is not empty and the logBlockNum = N + 1
			|Block0|Block1|Block2|...|BlockN|
										⬆ currentBlk
		*/
		logManager.currentBlk = fm.NewBlockId(logManager.logFileName, logBlockNum-1)
		//mmap, map the currentBlock to the mem Page
		_, err = logManager.fileManager.Read(logManager.currentBlk, logManager.logPage)
		if err != nil {
			return nil, err
		}
	}

	return &logManager, nil
}

/*
AppendLogRecordIntoPage

- before storing the log, first check if the logPage is sufficient to contain it

- if not, this logPage should be written back to the file blockId. Then, create a new blockId and map it into the Page(not a new Page)

- if ok, append the logRecord to the current logPage
*/
func (l *LogFileManager) AppendLogRecordIntoPage(logRecord []byte) (uint64, error) {
	//low efficiency lock, check the java FileChannel.fileLock for more intuitions
	l.mutex.Lock()
	defer l.mutex.Unlock()

	/*
		LogFile_Page structure, LR = logRecord:
				|whereToWrite|LRNLen|LRN     |.....|LR1Len|LR1      |LR0Len|LR0     |
				|8bytes      |8B    |LRNLen B|.....|8B    |LR1Len B |8B    |LR0Len B|
		Suppose the whereToWrite is 400, and the recordSize is 100, then this new Record should be written in file offset[300,399]
	*/
	whereToWrite := l.logPage.GetInt(0) // check the appendNewBlockAndMmap()
	recordSize := uint64(len(logRecord))
	//byte slice should be appended with the len of the slice, check the Page struct
	bytesNeed := recordSize + UINT64_LEN

	//the logPage can't contain the logRecord
	if (whereToWrite - bytesNeed) < uint64(UINT64_LEN) {
		/*
					|Block0|Block1|             |Block0|Block1|Block2|
			                   ⬆ curBlk                           ⬆ curBlk
				- make sure the curBlk should be written back with updated data
				- create a new Block2 for storing the locRecord
		*/
		err := l.Flush() //write logPage data into Block1
		if err != nil {
			return l.latestLSN, err
		}
		//mmap, the logPage now mapping to the empty Block2 is also empty
		l.currentBlk, err = l.appendNewBlockAndMmap()
		if err != nil {
			return l.latestLSN, err
		}
		//get the whereToWrite
		whereToWrite = l.logPage.GetInt(0)
	}

	recordOffset := whereToWrite - bytesNeed

	/*
		if the logPage is sufficient, it still maps to the Block1;
		if not, the logPage now maps to the Block2(empty)
	*/

	l.logPage.SetBytes(recordOffset, logRecord) //update the logRecord
	l.logPage.SetInt(0, recordOffset)           //set whereToWrite
	l.latestLSN += 1                            // the logRecord sequence number

	return l.latestLSN, nil
}

/*
appendNewBlockAndMmap allocates a new Block and map it to a Page(not a new one). Use the logPage to assign the whereToWrite to the beginning of the blockId.

WARN: don't use it solely
*/
func (l *LogFileManager) appendNewBlockAndMmap() (*fm.BlockId, error) {
	//allocate new blockId for the log file

	// |Block0| -> |Block0|Block1|
	blockId, err := l.fileManager.Append(l.logFileName)
	if err != nil {
		return nil, err
	}

	/*
				|Block0|       -> |Block0|Block1|
			      ⬆ whereToWrite                ⬆ whereToWrite
		Zoom in:
				|whereToWrite|empty| ...|empty|empty|
			                                        ⬆ whereToWrite
	*/
	l.logPage.SetInt(0, l.fileManager.BlockSize()) //set whereToWrite

	//empty block means empty page
	_, err = l.fileManager.Write(&blockId, l.logPage)
	if err != nil {
		return nil, err
	}

	return &blockId, nil
}

/*
FlushByLSN used for write the logRecord back to disk with number LSN.
But if other LRs share the same block with the logRecord, they will also be flushed.
*/
func (l *LogFileManager) FlushByLSN(lsn uint64) error {
	// if the lastSavedLSN is 6, and the lsn is 5, that means the log file has been flushed to disk
	if lsn > l.lastSavedLSN {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.lastSavedLSN = lsn
	}

	return nil
}

/*
Flush just write the current logPage back to the file blockId. It won't alter currentBlk and latestLSN, latestSavedLSN.
*/
func (l *LogFileManager) Flush() error {
	_, err := l.fileManager.Write(l.currentBlk, l.logPage)
	if err != nil {
		return err
	}
	return nil
}

/*
Iterator For a consistent view, the logPage should be flushed into the disk before creating the iterator. So that's a time consuming operation.
*/
func (l *LogFileManager) Iterator() *LogIterator {
	err := l.Flush()
	if err != nil {
		return nil
	}
	return NewLogIterator(l.fileManager, l.currentBlk)
}
