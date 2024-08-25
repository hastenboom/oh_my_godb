package record_manager

import fm "oh_my_godb/file_manager"

/*
SchemaInterface schema describes the table metadata :
how may columns,
their names,
data types,
data lengths, etc.
*/
type SchemaInterface interface {
	AddField(field_name string, field_type FIELD_TYPE, length int)
	AddIntField(field_name string)
	AddStringField(field_name string, length int)
	Add(field_name string, sch SchemaInterface)
	AddAll(sch SchemaInterface)
	Fields() []string
	HasFields(field_name string) bool
	Type(field_name string) FIELD_TYPE
	Length(field_name string) int
}

type LayoutInterface interface {
	Schema() SchemaInterface
	Offset(fieldName string) int
	SlotSize() int
}

type RecordManager interface {
	Block() *fm.BlockId
	GetInt(slot int, fieldName string) int
	SetInt(slot int, fieldName string, value int)
	GetString(slot int, fieldName string) string
	SetString(slot int, fieldName string, value string)
	Format() // set default value for the record
	Delete(slot int)
	NextAfter(slot int) int //TODO: ??? next valid slot value
	InsertAfter(slot int) int
}
