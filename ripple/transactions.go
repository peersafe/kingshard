// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package ripple

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type TableFiled map[string]string

// table object
type TableEntry struct {
	Table TableFiled
}

func NewTableEntry() *TableEntry {
	tableEntry := new(TableEntry)
	tableEntry.Table = make(TableFiled)
	return tableEntry
}

func (entry *TableEntry) isExist(fieldName string) bool {
	_, ok := entry.Table[fieldName]
	return ok
}

func (entry *TableEntry) AddFiled(fieldName string, fieldValue string) bool {
	if entry.isExist(fieldName) == true {
		return false
	}

	entry.Table[fieldName] = fieldValue
	return true
}

func (entry *TableEntry) AddTableName(name string, encode bool) bool {
	var ok bool
	if encode {
		ok = entry.AddFiled("TableName", hex.EncodeToString([]byte(name)))
	} else {
		ok = entry.AddFiled("TableName", name)
	}
	return ok
}

func (entry *TableEntry) AddTableNewName(name string, encode bool) bool {
	var ok bool
	if encode {
		ok = entry.AddFiled("TableNewName", hex.EncodeToString([]byte(name)))
	} else {
		ok = entry.AddFiled("TableNewName", name)
	}
	return ok
}

func (entry *TableEntry) AddAlias(alias string) bool {
	return entry.AddFiled("Alias", alias)
}

func (entry *TableEntry) AddNameInDB(id string) bool {
	return entry.AddFiled("NameInDB", id)
}

func (entry *TableEntry) TableName() (string, bool) {
	tableName, ok := entry.Table["TableName"]
	return tableName, ok
}

// json readable
type JsonItem map[string]interface{}

func NewJsonItem() *JsonItem {
	item := make(JsonItem)
	return &item
}

func (item *JsonItem) AddField(fieldName string, fieldValue interface{}) bool {
	_, ok := (*item)[fieldName]
	if ok == true {
		return false
	}

	(*item)[fieldName] = fieldValue
	return true
}

func (item *JsonItem) FieldValue(fieldName string) (interface{}, bool) {
	value, ok := (*item)[fieldName]
	return value, ok
}

func (item *JsonItem) DeleteField(fieldName string) bool {
	delete(*item, fieldName)
	return true
}

// conditions of query
type ConditionExprs []JsonItem

func NewConditionExprs() ConditionExprs {
	expr := make([]JsonItem, 0)
	return expr
}

func (exprs *ConditionExprs) AddOneExpr(fieldName string, rippleOp string, realValue interface{}) {
	value := NewJsonItem()
	value.AddField(rippleOp, realValue)

	cond := NewJsonItem()
	cond.AddField(fieldName, value)

	*exprs = append(*exprs, *cond)
}

func (exprs *ConditionExprs) AddOneExprEnty(entry *JsonItem) {
	*exprs = append(*exprs, *entry)
}

func makeConditionItem(conds *ConditionExprs, and bool) (*JsonItem, error) {
	if len(*conds) == 0 {
		return nil, errors.New("conditions are empty")
	}

	item := NewJsonItem()
	if and {
		item.AddField("$and", *conds)
	} else {
		item.AddField("$or", *conds)
	}

	return item, nil
}

func MakeAndConditions(conds *ConditionExprs) (*JsonItem, error) {
	return makeConditionItem(conds, true)
}

func MakeOrConditions(conds *ConditionExprs) (*JsonItem, error) {
	return makeConditionItem(conds, false)
}

// fields of table
type RawArrayItem []string

func NewRawArrayItem() *RawArrayItem {
	array := make(RawArrayItem, 0)
	return &array
}

func (array *RawArrayItem) AddItem(item string) {
	*array = append(*array, item)
}

// and condition
type RawAndItem struct {
	And []JsonItem
}

func NewRawAndItem() *RawAndItem {
	item := new(RawAndItem)
	item.And = make([]JsonItem, 0)
	return item
}

func (item *RawAndItem) AddEqual(e *JsonItem) {
	item.And = append(item.And, *e)
}

// type of transaction
const (
	TxType_TableListSet = "TableListSet"
	TxType_SQLStatement = "SQLStatement"
)

// OpType
const (
	OpType_CreateTable  = 1
	OpType_DropTable    = 2
	OpType_Rename       = 3
	OpType_Assign       = 4
	OpType_CancelAssign = 5
	OpType_Insert       = 6
	OpType_Select       = 7
	OpType_Update       = 8
	OpType_Delete       = 9
)

type Transaction struct {
	Params JsonItem // include method,account,owner,secret ...
	Tables []TableEntry
	Raw    []interface{}
	Extern JsonItem // include limit and order
}

func NewTransaction() *Transaction {
	tx := new(Transaction)
	tx.Params = *NewJsonItem()
	tx.Tables = make([]TableEntry, 0)
	tx.Raw = make([]interface{}, 0)
	return tx
}

func (tx *Transaction) AddFieldToTxJson(fieldName string, value interface{}) bool {
	return tx.Params.AddField(fieldName, value)
}

func (tx *Transaction) SetMethod(method string) bool {
	return tx.AddFieldToTxJson("method", method)
}

func (tx *Transaction) SetOffline(offline bool) bool {
	return tx.AddFieldToTxJson("offline", offline)
}

func (tx *Transaction) SetAccount(account string) bool {
	return tx.AddFieldToTxJson("Account", account)
}

func (tx *Transaction) Account() (string, bool) {
	if account, ok := tx.Params.FieldValue("Account"); ok {
		return account.(string), ok
	}
	return "", false
}

func (tx *Transaction) SetSecret(secret string) bool {
	return tx.AddFieldToTxJson("secret", secret)
}

func (tx *Transaction) Secret() (string, bool) {
	if secret, ok := tx.Params.FieldValue("secret"); ok {
		return secret.(string), ok
	}
	return "", false
}

func (tx *Transaction) SetOwner(owner string) bool {
	return tx.AddFieldToTxJson("Owner", owner)
}

func (tx *Transaction) Owner() (string, bool) {
	if owner, ok := tx.Params.FieldValue("Owner"); ok {
		return owner.(string), ok
	}
	return "", false
}

func (tx *Transaction) SetUser(user string) bool {
	return tx.AddFieldToTxJson("User", user)
}

func (tx *Transaction) SetOpType(opType int) bool {
	return tx.AddFieldToTxJson("OpType", opType)
}

func (tx *Transaction) SetTransactionType(txType string) bool {
	return tx.AddFieldToTxJson("TransactionType", txType)
}

func (tx *Transaction) SetConfidential(confidential bool) bool {
	return tx.AddFieldToTxJson("Confidential", confidential)
}

func (tx *Transaction) SetStrictMode(strictMode bool) bool {
	return tx.AddFieldToTxJson("StrictMode", strictMode)
}

func (tx *Transaction) SetPublicKey(key string) bool {
	return tx.AddFieldToTxJson("PublicKey", key)
}

func (tx *Transaction) SetFlags(flags int) bool {
	return tx.AddFieldToTxJson("Flags", flags)
}

func (tx *Transaction) FieldValue(fieldName string) (interface{}, bool) {
	value, ok := tx.Params.FieldValue(fieldName)
	return value, ok
}

// Add table entry by only name
func (tx *Transaction) AddTableByName(tableName string, encode bool) bool {
	for _, entry := range tx.Tables {
		if exist, ok := entry.TableName(); ok {
			// Table has existed
			if exist == tableName {
				return false
			}
		}
	}

	entry := NewTableEntry()
	if ok := entry.AddTableName(tableName, encode); ok == false {
		return false
	}
	tx.Tables = append(tx.Tables, *entry)
	return true
}

// add table by entry object
func (tx *Transaction) AddTableEntry(entry *TableEntry) bool {
	tableName, ok := entry.TableName()
	if ok == false {
		return false
	}

	for _, entry := range tx.Tables {
		if exist, ok := entry.TableName(); ok {
			// Table has existed
			if exist == tableName {
				return false
			}
		}
	}

	tx.Tables = append(tx.Tables, *entry)
	return true
}

func (tx *Transaction) SetLimit(index int64, count int64) bool {

	if tx.Extern == nil {
		tx.Extern = *NewJsonItem()
	}

	_, ok := tx.Extern.FieldValue("$limit")
	if ok == true {
		return false
	}

	limit := NewJsonItem()
	limit.AddField("index", index)
	limit.AddField("total", count)
	tx.Extern.AddField("$limit", limit)
	return true
}

const (
	Order_ASC  = 1
	Order_DESC = -1
)

func (tx *Transaction) AddOneOrder(orderField string, asc int) bool {
	if tx.Extern == nil {
		tx.Extern = *NewJsonItem()
	}

	orders := make([]JsonItem, 0)
	orderArray, ok := tx.Extern.FieldValue("$order")
	if ok {
		switch reflect.TypeOf(orderArray).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(orderArray)
			fmt.Printf("Not implement xxxxxxxxx %T\n", s.Index(0))
		}
	}
	// append new one
	order := NewJsonItem()
	order.AddField(orderField, asc)
	orders = append(orders, *order)
	tx.Extern.AddField("$order", orders)
	return true
}

// we must add fields when creating a select transaction firstly
func (tx *Transaction) AddFields(fields *RawArrayItem) bool {
	if len(tx.Raw) > 0 {
		return false
	}

	if len(*fields) == 0 {
		return false
	}

	tx.Raw = append(tx.Raw, *fields)
	return true
}

// add Raw
func (tx *Transaction) AddOneRawItem(item interface{}) {
	tx.Raw = append(tx.Raw, item)
}

func (tx *Transaction) makeTxJsonValue() (map[string]interface{}, error) {
	tx_json := make(map[string]interface{}, 0)

	if len(tx.Tables) == 0 {
		return nil, errors.New("Transaction must specify tables")
	}

	// set fields
	txType, ok := tx.FieldValue("TransactionType")
	if ok == false {
		return nil, errors.New("Transaction's type must be specified")
	}
	tx_json["TransactionType"] = txType
	opType, ok := tx.FieldValue("OpType")
	if ok == false {
		return nil, errors.New("Transaction's OpType must be specified")
	}

	if len(tx.Raw) == 0 && opType != OpType_Assign && opType != OpType_CancelAssign && opType != OpType_Rename && opType != OpType_DropTable {
		return nil, errors.New("Query statement must specifiy raw")
	}
	tx_json["OpType"] = opType

	account, ok := tx.FieldValue("Account")
	if opType != OpType_Select && ok == false {
		return nil, errors.New("Not-get's transaction must specify account")
	}
	if ok {
		tx_json["Account"] = account
	}

	owner, ok := tx.FieldValue("Owner")
	if ok {
		tx_json["Owner"] = owner
	}

	user, ok := tx.FieldValue("User")
	if ok {
		tx_json["User"] = user
	}

	confidential, ok := tx.FieldValue("Confidential")
	if ok {
		tx_json["Confidential"] = confidential
	}
	strictMode, ok := tx.FieldValue("StrictMode")
	if ok {
		tx_json["StrictMode"] = strictMode
	}

	flags, ok := tx.FieldValue("Flags")
	if ok {
		tx_json["Flags"] = flags
	} else {
		tx_json["Flags"] = 65536
	}

	publickey, ok := tx.FieldValue("PublicKey")
	if ok {
		tx_json["PublicKey"] = publickey
	}

	// set tables
	if tx.Tables[0].isExist("TableName") == false {
		return nil, errors.New("tableName must be specified")
	}

	if opType == OpType_Rename {
		if tx.Tables[0].isExist("TableNewName") == false {
			return nil, errors.New("Either old or new table must be specified in rename")
		}
	}

	tx_json["Tables"] = tx.Tables
	// set raws
	if tx.Extern != nil {
		tx.Raw = append(tx.Raw, tx.Extern)
	}

	if len(tx.Raw) > 0 {
		json_raw, err := json.Marshal(tx.Raw)
		if err != nil {
			return nil, err
		}

		tx_json["Raw"] = hex.EncodeToString(json_raw)
	}

	return tx_json, nil
}

func (tx *Transaction) makeOneParamValue(tx_json *map[string]interface{}) (map[string]interface{}, error) {
	param := make(map[string]interface{}, 0)
	var err error = nil

	offline, ok := tx.FieldValue("offline")
	if ok {
		param["offline"] = offline
	}
	secret, ok := tx.FieldValue("secret")
	if ok {
		param["secret"] = secret
	}
	param["tx_json"] = *tx_json
	return param, err
}

func (tx *Transaction) BuildHttpRequest(method string) ([]byte, error) {
	transaction := make(map[string]interface{}, 0)
	if len(method) == 0 {
		return nil, errors.New("Transaction must specify method")
	}
	transaction["method"] = method

	tx_json, ok := tx.makeTxJsonValue()
	if ok != nil {
		return nil, ok
	}

	param, ok := tx.makeOneParamValue(&tx_json)
	if ok != nil {
		return nil, ok
	}
	params := make([]interface{}, 0)
	params = append(params, param)
	transaction["params"] = params

	json, ok := json.Marshal(transaction)
	if ok != nil {
		return nil, ok
	}
	return json, nil
}

func (tx *Transaction) BuildWSPrepare() ([]byte, error) {
	transaction := make(map[string]interface{}, 0)
	transaction["command"] = "t_prepare"
	tx_json, err := tx.makeTxJsonValue()
	if err != nil {
		return nil, err
	}
	transaction["tx_json"] = tx_json
	json, err := json.Marshal(transaction)
	if err != nil {
		return nil, err
	}
	return json, nil
}

func (tx *Transaction) BuildWSRequestByTxJson(tx_json map[string]interface{}) ([]byte, error) {
	transaction := make(map[string]interface{}, 0)
	transaction["command"] = "submit"
	secret, ok := tx.FieldValue("secret")
	if ok {
		transaction["secret"] = secret
	}
	if tx_json_value, ok := tx_json["tx_json"]; ok {
		transaction["tx_json"] = tx_json_value
	} else {
		return nil, errors.New("be short of tx_json")
	}

	json, err := json.Marshal(transaction)
	if err != nil {
		return nil, err
	}
	return json, nil
}

func (tx *Transaction) BuildWSRequest() ([]byte, error) {
	v, err := tx.makeTxJsonValue()
	if err != nil {
		return nil, err
	}

	tx_json := make(map[string]interface{}, 0)
	tx_json["tx_json"] = v
	return tx.BuildWSRequestByTxJson(tx_json)
}

func (tx *Transaction) BuildDBNameRequest() ([]byte, error) {
	if len(tx.Tables) == 0 {
		return nil, fmt.Errorf("table must be specified.")
	}

	transaction := make(map[string]interface{}, 0)
	transaction["command"] = "g_dbname"

	tx_json := make(map[string]interface{}, 0)

	if account, ok := tx.FieldValue("Account"); ok == false {
		return nil, fmt.Errorf("Account must be specified.")
	} else {
		tx_json["Account"] = account
	}

	tableEntry := tx.Tables[0]
	if tableName, ok := tableEntry.TableName(); ok == false {
		return nil, fmt.Errorf("tableName must be specified.")
	} else {
		tx_json["TableName"] = tableName
	}

	transaction["tx_json"] = tx_json
	json, err := json.Marshal(transaction)
	if err != nil {
		return nil, err
	}
	return json, nil
}
