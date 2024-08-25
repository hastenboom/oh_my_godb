package log_manager

import (
	"fmt"
	"github.com/stretchr/testify/require"
	fm "oh_my_godb/file_manager"
	"testing"
)

/*
return a []byte looks like
LR = |strLen|string|int|
*/
func makeLogRecord(strRecord string, intRecord uint64) []byte {

	length := fm.MaxLengthForStr(strRecord)

	buf := make([]byte, length+UINT64_LEN) //LRLEN + LR

	/*this page doesn't map to any block*/
	page := fm.NewPageByBytes(buf)

	page.SetString(0, strRecord)
	page.SetInt(length, intRecord)

	fmt.Println(string(buf))

	return buf
}

func createRecord(lm *LogFileManager, startLSN uint64, endLSN uint64) {
	for i := startLSN; i < endLSN+1; i++ {
		recordBytes := makeLogRecord(fmt.Sprintf("record%d", i), i)
		//|LRNLen|LRN| ... |LR0Len|LRN|
		_, err := lm.AppendLogRecordIntoPage(recordBytes)
		if err != nil {
			return
		}
	}
}

func TestLogManager(t *testing.T) {
	//prepare
	var err error

	logManager, err := NewLogManagerWithConfig("log_test", 200, "logfile")

	if err != nil {
		return
	}
	startLSN, endLSN := uint64(0), uint64(30)

	//create a series of log records into the Page
	createRecord(logManager, startLSN, endLSN)

	// read the log record
	recCount := endLSN
	// for view consistency, the iterator will trigger flush
	it := logManager.Iterator()
	for it.HasNext() {
		logFileBlockBuf := it.Next()
		// use the page to analyse the []byte
		page := fm.NewPageByBytes(logFileBlockBuf)

		strRecord_act := page.GetString(0)
		strRecord_exp := fmt.Sprintf("record%d", recCount)
		require.Equal(t, strRecord_exp, strRecord_act)

		strLen := fm.MaxLengthForStr(strRecord_act)
		intRecord_act := page.GetInt(strLen)
		require.Equal(t, recCount, intRecord_act)
		recCount -= 1
	}
}
