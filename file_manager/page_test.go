package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetAndGetInt(t *testing.T) {
	page := NewPageBySize(256)

	val := uint64(1234)
	offset := uint64(23)
	page.SetInt(offset, val)

	val_got := page.GetInt(offset)
	require.Equal(t, val, val_got)
}

func TestSetAndGetByteArr(t *testing.T) {

	page := NewPageBySize(256)
	byteExpected := []byte{1, 2, 3, 4, 5, 6}
	offset := uint64(111)
	page.SetBytes(offset, byteExpected)

	byteActual := page.GetBytes(offset)

	require.Equal(t, byteExpected, byteActual)
}

func TestSetAndGetString(t *testing.T) {
	expected := "hello, 世界"
	page := NewPageBySize(256)
	offset := uint64(11)
	page.SetString(offset, expected)
	actual := page.GetString(offset)
	require.Equal(t, expected, actual)
}

func TestMaxLenForStr(t *testing.T) {
	s := "hello, 世界"
	exptected := uint64(len([]byte(s)))
	_ = NewPageBySize(256)
	actual := MaxLengthForStr(s)
	require.Equal(t, exptected+8, actual)
}

func TestGetContents(t *testing.T) {
	exp := []byte{12, 3, 4}
	page := NewPageByBytes(exp)
	act := page.contents()

	require.Equal(t, exp, act)

}
