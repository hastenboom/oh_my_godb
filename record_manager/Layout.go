package record_manager

import (
	fm "oh_my_godb/file_manager"
	"oh_my_godb/tx"
)

const (
	BYTES_OF_INT = 8
)

type Layout struct {
	schema   SchemaInterface
	offsets  map[string]int //field offset
	slotSize int
}

func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := &Layout{
		schema:   schema,
		offsets:  make(map[string]int),
		slotSize: 0,
	}

	fields := schema.Fields()
	pos := int(tx.UINT64_LEN) // 0, the slot is not used; 1 is used

	for i := 0; i < len(fields); i++ {
		layout.offsets[fields[i]] = pos
		pos += layout.lengthInBytes(fields[i])
	}

	layout.slotSize = pos // whole slot size

	return layout
}

func NewLayout(schema SchemaInterface, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}

}

func (l *Layout) Schema() SchemaInterface {
	return l.schema
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}

func (l *Layout) Offset(filedName string) int {
	offset, ok := l.offsets[filedName]
	if !ok {
		return -1
	}

	return offset
}

func (l *Layout) lengthInBytes(fieldName string) int {
	fieldType := l.schema.Type(fieldName)
	//p := fm.NewPageBySize(1)
	if fieldType == INTEGER {
		return BYTES_OF_INT
	} else {
		fieldLen := l.schema.Length(fieldName)
		dummyStr := string(make([]byte, fieldLen))
		return int(fm.MaxLengthForStr(dummyStr))
	}
}
