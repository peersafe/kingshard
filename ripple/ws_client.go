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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/flike/kingshard/core/golog"
	"github.com/gorilla/websocket"
)

func (tx *Transaction) WriteToChainSQL(ws_conn *websocket.Conn) error {
	prepare, err := tx.BuildWSPrepare()
	if err != nil {
		return err
	}

	// send prepare
	golog.Info("ripple", "WriteToChainSQL:request", string(prepare), 0)
	if err := ws_conn.WriteMessage(websocket.TextMessage, prepare); err != nil {
		return err
	}
	_, prepare_response_byte, err := ws_conn.ReadMessage()
	if err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}
	golog.Info("ripple", "WriteToChainSQL:response", string(prepare_response_byte), 0)

	// parse prepare response
	var prepare_response RippleResponse
	if err := json.Unmarshal(prepare_response_byte, &prepare_response); err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}
	if prepare_response.Result["status"] != "success" {
		return fmt.Errorf(string(prepare_response_byte))
	}

	// send request
	request, err := tx.BuildWSRequestByTxJson(prepare_response.Result)
	if err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}

	golog.Info("ripple", "WriteToChainSQL:request", string(request), 0)
	if err := ws_conn.WriteMessage(websocket.TextMessage, request); err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}
	_, response, err := ws_conn.ReadMessage()
	if err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}
	golog.Info("ripple", "WriteToChainSQL:response", string(response), 0)
	var result RippleResponse
	if err := json.Unmarshal(response, &result); err != nil {
		golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
		return err
	}

	if result.Result["engine_result"] != "tesSUCCESS" {
		if result_msg, ok := result.Result["engine_result_message"]; ok {
			return errors.New(result_msg.(string))
		} else {
			rs, err := json.Marshal(&result)
			if err != nil {
				golog.Error("ripple", "WriteToChainSQL", err.Error(), 0)
				return err
			}
			return errors.New(string(rs))
		}
	}

	return nil
}

func PreBuild(tx *Transaction, ws_conn *websocket.Conn) bool {
	// fill with NameInDB
	for _, tableEntry := range tx.Tables {
		if tableEntry.isExist("NameInDB") {
			continue
		}
		var tableName, ower string
		var ok bool
		if tableName, ok = tableEntry.TableName(); ok == false {
			continue
		}

		if ower, ok = tx.Owner(); ok == false {
			continue
		}

		var nameInDB []byte
		var err error
		if nameInDB, err = GetNameInDB(tableName, ower, ws_conn); err != nil {
			golog.Error("ripple", "PreBuild", err.Error(), 0)
			return false
		}

		tableEntry.AddNameInDB(string(nameInDB))
	}
	return true
}

func (tx *Transaction) SimpleWriteToChainSQL(ws_conn *websocket.Conn) error {
	// send request
	request, err := tx.BuildWSRequest()
	if err != nil {
		golog.Error("ripple", "SimpleWriteToChainSQL", err.Error(), 0)
		return err
	}

	golog.Info("ripple", "SimpleWriteToChainSQL:request", string(request), 0)
	if err := ws_conn.WriteMessage(websocket.TextMessage, request); err != nil {
		golog.Error("ripple", "SimpleWriteToChainSQL", err.Error(), 0)
		return err
	}
	_, response, err := ws_conn.ReadMessage()
	if err != nil {
		golog.Error("ripple", "SimpleWriteToChainSQL", err.Error(), 0)
		return err
	}
	golog.Info("ripple", "SimpleWriteToChainSQL:response", string(response), 0)
	var result RippleResponse
	if err := json.Unmarshal(response, &result); err != nil {
		golog.Error("ripple", "SimpleWriteToChainSQL", err.Error(), 0)
		return err
	}

	if result.Result["engine_result"] != "tesSUCCESS" {
		if result_msg, ok := result.Result["engine_result_message"]; ok {
			return errors.New(result_msg.(string))
		} else {
			rs, err := json.Marshal(&result)
			if err != nil {
				golog.Error("ripple", "SimpleWriteToChainSQL", err.Error(), 0)
				return err
			}
			return errors.New(string(rs))
		}

	}

	return nil
}

func GetNameInDB(tableName string, owner string, ws_conn *websocket.Conn) ([]byte, error) {
	tx := NewTransaction()
	tx.SetAccount(owner)
	tx.AddTableByName(tableName, false)

	request, err := tx.BuildDBNameRequest()
	if err != nil {
		return nil, err
	}

	if err := ws_conn.WriteMessage(websocket.TextMessage, request); err != nil {
		golog.Error("ripple", "GetNameInDB", err.Error(), 0)
		return nil, err
	}
	//golog.Info("ripple", "GetNameInDB:request", string(request), 0)

	_, response, err := ws_conn.ReadMessage()

	if err != nil {
		golog.Error("ripple", "GetNameInDB", err.Error(), 0)
		return nil, err
	}

	//golog.Info("ripple", "GetNameInDB:response", string(response), 0)
	var result RippleResponse
	if err := json.Unmarshal(response, &result); err != nil {
		golog.Error("ripple", "GetNameInDB", err.Error(), 0)
		return nil, err
	}
	if result.Result["engine_result"] == "tesSUCCESS" || result.Status == "success" {
		return []byte(result.Result["nameInDB"].(string)), nil
	} else {
		return nil, fmt.Errorf("%s", string(response))
	}

	return nil, errors.New("Unkown error on GetNameInDB")
}
