package metadata_manager

import (
	rm "oh_my_godb/record_manager"
	"oh_my_godb/tx"
)

/*
When creating a new table, two special tables are created:
1.tblcat(tableName string,slotSize int)
2.fdlcat(tableName string, filedName string, type FIELD_TYPE, length, offset)



*/

const (
	MAX_NAME = 16
)

type TableManager struct {
	tcatLayout *rm.Layout
	fcatLayout *rm.Layout
}

func NewTableManager(isNew bool, txn *tx.Transaction) *TableManager {
	tableMgr := &TableManager{}

	tcatSchema := rm.NewSchema()
	tcatSchema.AddStringField("tblName", MAX_NAME)
	tcatSchema.AddIntField("slotSize")

	tableMgr.tcatLayout = rm.NewLayoutWithSchema(tcatSchema)

	fcatSchema := rm.NewSchema()
	fcatSchema.AddStringField("tblName", MAX_NAME)
	fcatSchema.AddStringField("fldName", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tableMgr.fcatLayout = rm.NewLayoutWithSchema(fcatSchema)

	if isNew {
		tableMgr.CreateTable("tblcat", tcatSchema, txn)
		tableMgr.CreateTable("fdlcat", fcatSchema, txn)
	}

	return tableMgr
}

func (t *TableManager) CreateTable(s string, schema *rm.Schema, txn *tx.Transaction) {
	layout := rm.NewLayoutWithSchema(schema)

	tcat := rm.NewTableScan(txn, "tblcat", t.tcatLayout)

}
