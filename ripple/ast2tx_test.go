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
	"flag"
	"fmt"
	"net/url"
	"testing"

	"github.com/flike/kingshard/sqlparser"
	"github.com/gorilla/websocket"
)

var tx_OpType map[string]int = map[string]int{"r_create": OpType_CreateTable, "r_drop": OpType_DropTable, "r_rename": OpType_Rename, "r_insert": OpType_Insert, "r_update": OpType_Update, "r_delete": OpType_Delete, "r_get": OpType_Select}

func isTablelistSet(method string) bool {
	if method == "t_create" || method == "t_drop" || method == "t_rename" || method == "t_assign" || method == "t_cancelassign" {
		return true
	}
	return false
}

func testConvert(query_sql string, method string, t *testing.T) {
	st, ok := sqlparser.Parse(query_sql)
	if ok != nil {
		t.Fatal(ok)
	}
	tx := NewTransaction()
	//set params
	tx.SetOffline(false)
	tx.SetSecret(Test_Secret)
	// set tx_json
	//if isTablelistSet(method) {
	//	tx.SetTransactionType(TxType_TableListSet)
	//} else {
	//	tx.SetTransactionType(TxType_SQLStatement)
	//}
	if method != "r_get" {
		tx.SetAccount(Test_Account)
	}
	tx.SetOwner(Test_Account)
	//tx.AddTableByName("user")
	//tx.SetOpType(tx_OpType[method])

	err := Ast2Tx(&st, tx)
	if err != nil {
		t.Fatal(err)
	}
	json, ok := tx.BuildWSRequest()
	if ok != nil {
		msg := fmt.Errorf("%s,current method: %s", ok.Error(), method)
		t.Fatal(msg.Error())
	}
	fmt.Println("Ast2Tx execute successfully. \n\t", string(json))
}

func TestAst2Tx(t *testing.T) {
	return
	{
		query_sql := "select id, age from user where id >= 10"
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "select id, age from user where id >= 10 and age < 20"
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "select id, age from user where id >= 10 and age < 20 and user = 'peersafe'"
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "select id, age from user where id >= 10 or age < 20 or user = 'peersafe'"
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "select id, age from user where (id = 100 and user = 'peersafe') or age = 'wage'"
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "select id, age from user where (id = 100 and user = 'peersafe') or (age > 10 and user = 'peersafe')"
		testConvert(query_sql, "r_get", t)
	}
	{
		query_sql := "select id, age from user where id >= 10 order by id asc,age desc limit 0, 10 "
		testConvert(query_sql, "r_get", t)
	}

	{
		query_sql := "insert into user(id,age,user) values(1,10,'peersafe')"
		testConvert(query_sql, "t_insert", t)
	}

	{
		query_sql := "insert into user(id,age,user) values(1,10,peersafe)"
		testConvert(query_sql, "t_insert", t)
	}

	{
		query_sql := "delete from user where id > 0 and age < 100"
		testConvert(query_sql, "t_delete", t)
	}

	{
		query_sql := "update user set user = peersafe, class = 0 where id > 0 and age < 100"
		testConvert(query_sql, "t_update", t)
	}
	/*
		{
			query_sql := "CREATE TABLE IF NOT EXISTS t_user (uid int(11) NOT NULL,username varchar(30) NOT NULL,password char(32) NOT NULL);"
			testConvert(query_sql, "t_create", t)
		}
	*/
	{
		query_sql := "drop table if exists user"
		testConvert(query_sql, "t_drop", t)
	}

	{
		query_sql := "ALTER TABLE old_table RENAME new_table"
		testConvert(query_sql, "t_rename", t)
	}

	{
		query_sql := "rename TABLE old_table to new_table"
		testConvert(query_sql, "t_drop", t)
	}
}

func TestGetTableName(t *testing.T) {
	addr := flag.String("addr", "192.168.0.155:6006", "http service address")
	flag.Parse()
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/"}
	ws_client, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	tableName := "deparment"
	nameInDB, err := GetNameInDB(tableName, "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", ws_client)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Printf("TableName: %s, TableNameInDB: %s\n", tableName, string(nameInDB))
}
