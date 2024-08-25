package tx

import (
	bm "oh_my_godb/buffer_manager"
	fm "oh_my_godb/file_manager"
)

/*
BufferList 只有bufferMgr是真正管理buffer的，这里只是增加额外的注册信息，
当blk申请到由bufferMgr管理的内存后，会映射 blk -> buffer 以及 blk -> pins
*/
type BufferList struct {
	buffers   map[*fm.BlockId]*bm.Buffer
	bufferMgr *bm.BufferManager
	pins      []*fm.BlockId
}

func NewBufferList(bufferMgr *bm.BufferManager) *BufferList {
	return &BufferList{
		bufferMgr: bufferMgr,
		buffers:   make(map[*fm.BlockId]*bm.Buffer),
		pins:      make([]*fm.BlockId, 0),
	}
}

func (b *BufferList) getBuffer(blk *fm.BlockId) *bm.Buffer {
	buff, _ := b.buffers[blk]
	return buff
}

func (b *BufferList) Pin(blk *fm.BlockId) error {
	// once the buffer has been pinned, add it into the buffers to follow
	buff, err := b.bufferMgr.Pin(blk)
	if err != nil {
		return err
	}

	b.buffers[blk] = buff
	b.pins = append(b.pins, blk)

	return nil
}

func (b *BufferList) Unpin(blk *fm.BlockId) {
	buff, exists := b.buffers[blk]
	if !exists {
		return
	}

	delete(b.buffers, blk)

	b.bufferMgr.Unpin(buff)
	for idx, pinedBlk := range b.pins {
		if pinedBlk.Equals(blk) {
			b.pins = append(b.pins[:idx], b.pins[idx+1:]...)
			break
		}
	}
}

func (b *BufferList) UnpinAll() {
	for _, blk := range b.pins {
		buffer := b.buffers[blk]
		b.bufferMgr.Unpin(buffer)
		//b.Unpin(blk)
	}

	b.buffers = make(map[*fm.BlockId]*bm.Buffer)
	b.pins = make([]*fm.BlockId, 0)
}
