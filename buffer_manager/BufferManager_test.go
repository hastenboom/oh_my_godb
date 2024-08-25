package buffer_manager

import (
	"github.com/stretchr/testify/require"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"testing"
)

func TestBufferManager(t *testing.T) {

	var FILE_NAME string = "testfile"
	var TEST_OFFSET uint64 = 5
	var BLOCK_SIZE uint64 = 20
	require.Greater(t, BLOCK_SIZE, TEST_OFFSET)
	var err error

	file_manager, _ := fm.NewFileManager("buffertest", BLOCK_SIZE)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")

	for i := 0; i < 3; i++ {
		_, err = file_manager.Append(FILE_NAME)
		if err != nil {
			return
		}
	}

	bm := NewBufferManager(file_manager, log_manager, 3)

	buff1, err := bm.Pin(fm.NewBlockId(FILE_NAME, 1))
	require.Nil(t, err)
	p := buff1.Contents()
	expected := uint64(123)
	p.SetInt(TEST_OFFSET, expected)
	buff1.SetModified(1, 0) //这里两个参数先不要管

	_, err = bm.Pin(fm.NewBlockId(FILE_NAME, 2))
	require.Nil(t, err)
	_, err = bm.Pin(fm.NewBlockId(FILE_NAME, 3))
	require.Nil(t, err)
	/*
		|blk1 |blk2 |blk3 |
		|buff1|buff2|buff3|
		|pin1 |pin1 |pin1 |
		free:0
	*/
	bm.Unpin(buff1)
	/*
		|blk1 |blk2 |blk3 |
		|buff1|buff2|buff3|
		|pin0 |pin1 |pin1 |
		free:1
	*/
	_, err = bm.Pin(fm.NewBlockId(FILE_NAME, 4))
	require.Nil(t, err)
	/*
			|blk4 |blk2 |blk3 |
			|buff1|buff2|buff3|
			|pin1 |pin1 |pin1 |
			free:0
		This will trigger a flush, now the blk1 is updated
	*/

	page := fm.NewPageBySize(BLOCK_SIZE)
	b1 := fm.NewBlockId(FILE_NAME, 1)
	file_manager.Read(b1, page)
	actual := page.GetInt(TEST_OFFSET)
	require.Equal(t, expected, actual)

	//
	//bm.Unpin(buff2)
	///*
	//	|blk4 |blk2 |blk3 |
	//	|buff1|buff2|buff3|
	//	|pin1 |pin0 |pin1 |
	//	free:1
	//*/
	//
	//buff2, err = bm.Pin(fm.NewBlockId(FILE_NAME, 1))
	//require.Nil(t, err)
	///*
	//	|blk4 |blk1 |blk3 |
	//	|buff1|buff2|buff3|
	//	|pin1 |pin1 |pin1 |
	//	free:0, trigger flush, now the blk2 is updated
	//*/
	//
	//p2 := buff2.Contents()
	//p2.SetInt(80, 9999)
	//buff2.SetModified(1, 0)
	//bm.Unpin(buff2) //注意这里不会将buff2的数据写入磁盘
	///*
	//	|blk4 |blk1 |blk3 |
	//	|buff1|buff2|buff3|
	//	|pin1 |pin0 |pin1 |
	//	free:1
	//*/

}
