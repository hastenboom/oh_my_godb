package buffer_manager

import (
	"errors"
	fm "oh_my_godb/file_manager"
	lm "oh_my_godb/log_manager"
	"sync"
	"time"
)

const (
	MAX_TIME = 3 // max time to wait for a buffer allocation
)

/*
BufferManager also the refCounter BufferManager

- Pin(), the reading strategy and writing strategy, finds buffer with 0 pins(counts) and binds this buffer to the block.
If and only if the buffer wants to bind another block, will this buffer trigger the Writing, also flush.

- Unpin(), doesn't contain the writing strategy, just reduce the count by 1, if the count is 0,
increase the numAvailable by 1, and notify all waiting threads.
*/
type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable uint32
	mu           sync.Mutex
}

func NewBufferManager(fm *fm.FileManager, lm *lm.LogFileManager, numBuffer uint32) *BufferManager {
	bufferManager := &BufferManager{
		numAvailable: numBuffer,
		mu:           sync.Mutex{},
	}

	for i := uint32(0); i < numBuffer; i++ {
		buffer := NewBuffer(fm, lm)
		bufferManager.bufferPool = append(bufferManager.bufferPool, buffer)
	}

	return bufferManager
}

func (b *BufferManager) Available() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.numAvailable
}

func (b *BufferManager) FlushAll(txNum int32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, buffer := range b.bufferPool {
		if buffer.ModifyingTx() == txNum {
			buffer.Flush()
		}
	}

}

/*
Pin binds a block to a buffer and returns the buffer. Consumer.
*/
func (b *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {

	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	buff := b.tryPin(blk)

	//retry
	for buff == nil && b.waitingTooLong(start) == false {
		time.Sleep(MAX_TIME * time.Second)
		buff = b.tryPin(blk)
		if buff == nil {
			return nil, errors.New("no buffer available, potential deadlock")
		}
	}

	return buff, nil
}

/*
Unpin producer, just decreases the count by 1
*/
func (b *BufferManager) Unpin(buff *Buffer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if buff == nil {
		return
	}

	buff.Unpin()
	//the refCount == 0
	if !buff.IsPinned() {
		//buff.Flush()
		b.numAvailable++
		//TODO:notifyAll
	}

}

func (b *BufferManager) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed > MAX_TIME {
		return true
	}
	return false
}

func (b *BufferManager) tryPin(blk *fm.BlockId) *Buffer {
	// check if the block is already in the buffer pool
	buff := b.findExistingBuffer(blk)
	//the blk doesn't exist in mem
	if buff == nil {
		// get a free buffer
		buff = b.chooseUnpinBuffer()

		// no free buffer available
		if buff == nil {
			return nil
		}
		/*这里会触发flush*/
		buff.AssignToBlock(blk)
	}

	// unpinned buff, a free buff
	if buff.IsPinned() == false {
		// free buff count -= 1
		b.numAvailable -= 1
	}

	buff.Pin()

	return buff
}

// findExistingBuffer checks if the block is already in the buffer pool
func (b *BufferManager) findExistingBuffer(blk *fm.BlockId) *Buffer {
	for _, buffer := range b.bufferPool {
		block := buffer.Block()
		if block != nil && block.Equals(blk) {
			return buffer
		}
	}
	return nil
}

func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	for _, buffer := range b.bufferPool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}
