package file_manager

import (
	"encoding/binary"
)

/*
Tha page structure used to show how data is stored in the page.
|Int|Int|Int|BytesLen|   Bytes  |StrLen|  Str   |
|8B |8B |8B |   8B   |BytesLen B|  8B  |StrLen B|
*/

type Page struct {
	buffer []byte
}

func NewPageBySize(blockSize uint64) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

func NewPageByBytes(buffer []byte) *Page {
	return &Page{
		buffer: buffer,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	return binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
}

func (p *Page) SetInt(offset uint64, value uint64) {
	binary.LittleEndian.PutUint64(
		p.buffer[offset:offset+8], value)
}

func uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func (p *Page) GetBytes(offset uint64) []byte {
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8]) //读取数组长度
	newBuf := make([]byte, len)
	copy(newBuf, p.buffer[offset+8:])
	return newBuf
}

func (p *Page) SetBytes(offset uint64, b []byte) {
	//首先写入数组的长度，然后再写入数组内容
	length := uint64(len(b))
	lenBuf := uint64ToByteArray(length)
	copy(p.buffer[offset:], lenBuf) //写入长度
	copy(p.buffer[offset+8:], b)
}

/*!*/
func (p *Page) GetString(offset uint64) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset uint64, s string) {
	strBytes := []byte(s)
	p.SetBytes(offset, strBytes)
}

/*
MaxLengthForStr also the len of the String; As the string might contain other lang which used UTF-8
*/
func MaxLengthForStr(s string) uint64 {
	strBytes := []byte(s)
	return 8 + uint64(len(strBytes))
}

func (p *Page) contents() []byte {
	return p.buffer
}
