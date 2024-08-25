package file_manager

import (
	"crypto/sha256"
	"fmt"
)

/*
BlockId describes the position of the file with filePath
*/
type BlockId struct {
	filePath string
	blkNum   uint64
}

func NewBlockId(filePath string, blkNum uint64) *BlockId {
	return &BlockId{filePath, blkNum}
}

func (b *BlockId) GetFilePath() string {
	return b.filePath
}

func (b *BlockId) SetFilePath(filePath string) {
	b.filePath = filePath
}

func (b *BlockId) BlkNum() uint64 {
	return b.blkNum
}

func (b *BlockId) SetBlkNum(blkNum uint64) {
	b.blkNum = blkNum
}

func (b *BlockId) Equals(other *BlockId) bool {
	return b.filePath == other.filePath && b.blkNum == other.blkNum
}

func (b *BlockId) HashCode() string {
	return asSha256(*b)
}

func asSha256(obj any) string {
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("%v", obj)))
	return fmt.Sprintf("%x", hash.Sum(nil))
}
