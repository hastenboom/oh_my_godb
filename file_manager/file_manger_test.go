package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFileManger(t *testing.T) {
	var err error
	fm, _ := NewFileManager("file_test", 400)

	blk := NewBlockId("testFile", 2)

	/*prepare cache page*/
	page1 := NewPageBySize(fm.BlockSize())

	/*
			|....|hello world|
		         ↑
		      offset1:88
	*/
	offset1 := uint64(88)
	s1_exp := "hello world"
	page1.SetString(offset1, s1_exp)
	size := MaxLengthForStr(s1_exp)

	/*
			|....|hello world|321|
		                     ↑
		                 offset2:88
	*/
	offset2 := offset1 + size
	int_exp := uint64(321)
	page1.SetInt(offset2, int_exp)

	//write cache page to disk
	_, err = fm.Write(blk, page1)
	if err != nil {
		return
	}

	//read cache page from disk
	page2 := NewPageBySize(fm.BlockSize())
	_, err = fm.Read(blk, page2)
	if err != nil {
		return
	}

	s1_act := page2.GetString(offset1)
	int_act := page2.GetInt(offset2)

	require.Equal(t, s1_exp, s1_act)
	require.Equal(t, int_exp, int_act)
}
