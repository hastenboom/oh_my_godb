package tx

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"oh_my_godb/tx/logRecord"
	"testing"
)

func TestStartRecord(t *testing.T) {
	fileManager, err := fm.NewFileManager("recordtest", 400)
	if err != nil {
		t.Error(err)
	}
	logManager, err := lm.NewLogManager(fileManager, "record_file")
	if err != nil {
		return
	}

	txNum := uint64(13)
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(UINT64_LEN, txNum)

	startRecord := logRecord.NewStartRecord(p, logManager)

	expected := fmt.Sprintf("<START %d>", txNum)
	require.Equal(t, expected, startRecord.ToString())

	_, err = startRecord.WriteToLog()
	require.Nil(t, err)

	it := logManager.Iterator()
	rec := it.Next()
	recOp := binary.LittleEndian.Uint64(rec[0:8])
	recTxNum := binary.LittleEndian.Uint64(rec[8:16])

	require.Equal(t, recOp, uint64(START))
	require.Equal(t, recTxNum, txNum)
}

func TestSetStringRecord(t *testing.T) {
	fileManager, err := fm.NewFileManager("recordtest", 400)
	if err != nil {
		t.Error(err)
	}
	logManager, err := lm.NewLogManager(fileManager, "record_file")
	if err != nil {
		return
	}

	// Test Writing And Reading
	// simulate client write something into the blk
	str := "original string"
	blkNum := uint64(1)

	dummyBlk := fm.NewBlockId("dummy_id", blkNum)

	txNum := uint64(1)
	offset := uint64(13)

	// as the client write "original string" into the blk, generate a setStringRecord
	// the logFile now should have something like: <SETSTRING 1 dummy_id 13 original string>
	_, err = logRecord.WriteSetStringLog(logManager, txNum, dummyBlk, offset, str)
	if err != nil {
		return
	}

	iter := logManager.Iterator()
	rec := iter.Next()

	// read the <SETSTRING 1 dummy_id 13 original string>  back
	logPage := fm.NewPageByBytes(rec)
	setStr := logRecord.NewSetStringRecord(logPage)
	setStr_exp := fmt.Sprintf(logRecord.SET_STRING_RECORD_FORMAT, txNum, blkNum, offset, str)
	require.Equal(t, setStr.ToString(), setStr_exp)

	/*--------------------------------------------------*/
	//Test Undo

	page := fm.NewPageBySize(400)
	page.SetString(offset, "modify string 1")
	page.SetString(offset, "modify string 2")

	txStub := NewTxStub(page)
	//rewrite the page in txStub with the "original string"
	setStr.Undo(txStub)

	setString_recovery := page.GetString(offset)

	// str := "original string"
	require.Equal(t, setString_recovery, str)

}

func TestCommitRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "commit")
	tx_num := uint64(13)
	logRecord.WriteCommitRecordLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := logRecord.NewCommitRecord(pp)
	expected_str := fmt.Sprintf("<COMMIT %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "checkpoint")
	logRecord.WriteCheckPointToLog(log_manager)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	check_point_rec := logRecord.NewCheckPointRecord()
	expected_str := "<CHECKPOINT>"
	require.Equal(t, expected_str, check_point_rec.ToString())
}
