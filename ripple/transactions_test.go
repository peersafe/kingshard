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
	"fmt"
	"testing"
)

const (
	Test_Account = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
	Test_Secret  = "snoPBrXtMeMyMHUVTgbuqAfg1SUTb"
)

func TestCreateTableTransaction(t *testing.T) {
	return
	tx := NewTransaction()

	//set params
	tx.SetOffline(false)
	tx.SetSecret(Test_Secret)
	// set tx_json
	tx.SetTransactionType(TxType_TableListSet)
	tx.SetAccount(Test_Account)
	tx.AddTableByName("user", true)
	tx.SetOpType(OpType_CreateTable)

	// set raw
	id := NewJsonItem()
	id.AddField("field", "id")
	id.AddField("type", "int")
	id.AddField("length", 11)
	id.AddField("PK", 1)
	id.AddField("NN", 1)
	tx.AddOneRawItem(id)

	age := NewJsonItem()
	age.AddField("field", "age")
	age.AddField("type", "int")
	age.AddField("length", 11)
	tx.AddOneRawItem(age)

	json, ok := tx.BuildHttpRequest("t_create")
	if ok != nil {
		t.Fatal(ok)
	}

	fmt.Println("CreateTableTransaction:\n\t", string(json))
}

func TestSelectTransaction(t *testing.T) {
	return
	tx := NewTransaction()

	//set params
	tx.SetOffline(false)
	tx.SetSecret(Test_Secret)
	// set tx_json
	tx.SetTransactionType(TxType_SQLStatement)
	tx.SetOwner(Test_Account)
	tx.AddTableByName("user", true)
	tx.SetOpType(OpType_Select)

	// fields of query
	fields := NewRawArrayItem()
	fields.AddItem("id")
	fields.AddItem("age")
	tx.AddFields(fields)

	// set conditions of query
	conds := NewConditionExprs()
	conds.AddOneExpr("id", "$gt", 10)
	conds.AddOneExpr("age", "$gt", 100)
	conds.AddOneExpr("user", "$eq", "peersafe")
	and, ok := MakeAndConditions(&conds)
	if ok != nil {
		t.Fatal(ok)
	}
	tx.AddOneRawItem(and)

	tx.SetLimit(0, 10)
	tx.AddOneOrder("id", Order_ASC)
	tx.AddOneOrder("age", Order_DESC)

	json, ok := tx.BuildHttpRequest("t_create")
	if ok != nil {
		t.Fatal(ok)
	}

	fmt.Println("SelectTransaction:\n\t", string(json))
}
