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
	"fmt"
	"time"

	"github.com/flike/kingshard/core/golog"
	"github.com/gorilla/websocket"
)

const (
	Subscribe   = "subscribe"
	Unsubscribe = "unsubscribe"

	SubScribeType_table       = "table"
	SubScribeType_transaction = "transaction"

	SubcribeResult_VALIDATE_SUCCESS = "validate_success"
	SubcribeResult_VALIDATE_TIMEOUT = "validate_timeout"

	SubcribeResult_DB_SUCCESS = "db_success"
	SubcribeResult_DB_ERROR   = "db_error"
	SubcribeResult_DB_TIMEOUT = "db_timeout"
)

type SubscribeResponse struct {
	Owner       string
	Status      string
	TableName   string
	Type        string
	Transaction map[string]interface{}
}

type SubscribeCmd map[string]interface{}

func NewSubscribeCmd() *SubscribeCmd {
	item := make(SubscribeCmd, 1)
	return &item
}

func (cmd *SubscribeCmd) addField(fieldName string, fieldValue string) bool {
	//_, ok := (*cmd)[fieldName]
	//if ok == true {
	//	return false
	//}

	(*cmd)[fieldName] = fieldValue
	return true
}

func (cmd *SubscribeCmd) hasExist(fieldName string) bool {
	_, ok := (*cmd)[fieldName]
	return ok
}

// command is either subscribe or unsubscribe
func (cmd *SubscribeCmd) SetCommand(command string) bool {
	return cmd.addField("command", command)
}

func (cmd *SubscribeCmd) SetOwner(owner string) bool {
	return cmd.addField("owner", owner)
}

func (cmd *SubscribeCmd) SetTableName(tablename string) bool {
	return cmd.addField("tablename", tablename)
}

func (cmd *SubscribeCmd) SetTransaction(transaction string) bool {
	return cmd.addField("transaction", transaction)
}

func buildSubScribeTableCmd(cmd *SubscribeCmd) ([]byte, error) {
	if cmd.hasExist("command") == false {
		return nil, fmt.Errorf("command must be specified in subscribe")
	}
	if cmd.hasExist("owner") == false {
		return nil, fmt.Errorf("owner must be specified in subscribe")
	}
	if cmd.hasExist("tablename") == false {
		return nil, fmt.Errorf("tablename must be specified in subscribe")
	}

	subsribe, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	return subsribe, nil
}

func buildSubScribeTransactionCmd(cmd *SubscribeCmd) ([]byte, error) {
	if cmd.hasExist("command") == false {
		return nil, fmt.Errorf("command must be specified in subscribe")
	}
	if cmd.hasExist("transaction") == false {
		return nil, fmt.Errorf("transaction must be specified in subscribe")
	}

	subsribe, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	return subsribe, nil
}

func BuildCmd(cmd *SubscribeCmd, subType string) ([]byte, error) {
	if subType == SubScribeType_table {
		return buildSubScribeTableCmd(cmd)
	} else if subType == SubScribeType_transaction {
		return buildSubScribeTransactionCmd(cmd)
	}
	return nil, fmt.Errorf("Not support subType.[%s]", subType)
}

type SubscribeCallBack func(msg []byte)

type ChainSQLEvent struct {
	WsClient  *websocket.Conn
	OnEvnet   SubscribeCallBack
	Completed string
	stop      bool
}

func NewChainSQLEvent(ws_conn *websocket.Conn, cb SubscribeCallBack) *ChainSQLEvent {
	return &ChainSQLEvent{WsClient: ws_conn, OnEvnet: cb, stop: false}
}

func (event *ChainSQLEvent) sendScribleRequest(request []byte) error {
	golog.Info("ripple", "SyncWaitTxResult", "subscribe:"+string(request), 0)
	response, err := PushMessage(event.WsClient, request)
	if err != nil {
		//golog.Error("ripple", "SyncWaitTxResult", "send request of subscribe unsuccessfully.["+err.Error()+"]", 0)
		return err
	}

	golog.Info("ripple", "SyncWaitTxResult", "subscribe:"+string(response), 0)
	var subcribe_response SubscribeResponse
	ok := json.Unmarshal(response, &subcribe_response)
	if ok != nil {
		//golog.Error("ripple", "SyncWaitTxResult", "Unmarshal failure", 0)
		return ok
	}

	if subcribe_response.Status != "success" {
		return fmt.Errorf("Subscribe unsuccessfully")
	}

	return nil
}

type waitChannel struct {
	data []byte
	err  error
}

func (event *ChainSQLEvent) wait(seconds time.Duration) error {
	timeout := make(chan bool, 1)
	go func() {
		defer close(timeout)
		time.Sleep(seconds * time.Second) // wait for 15s
		if timeout != nil {
			timeout <- true
		}
	}()

	channel := make(chan waitChannel, 1)
	defer close(channel)
	go func() {
		for {
			_, recieve_data, err := event.WsClient.ReadMessage()
			var result waitChannel
			if err != nil {
				result.data = nil
				result.err = err
			} else {
				result.data = recieve_data
				result.err = nil
			}
			if event.stop {
				golog.Info("ripple", "SyncWaitTxResult", string(recieve_data), 0)
				return
			} else {
				channel <- result
			}
		}
	}()

	for {
		select {
		case result := <-channel:
			if result.err != nil {
				return result.err
			}
			var response SubscribeResponse
			err := json.Unmarshal(result.data, &response)
			if err != nil {
				golog.Error("ripple", "SyncWaitTxResult", "Unmarshal unsuccessfully.", 0)
				return err
			}

			if response.Status == event.Completed {
				return nil
			} else if response.Status == SubcribeResult_VALIDATE_TIMEOUT ||
				response.Status == SubcribeResult_DB_ERROR ||
				response.Status == SubcribeResult_DB_TIMEOUT {
				return fmt.Errorf(response.Status)
			}
		case <-timeout:
			return fmt.Errorf("Timeout")
		}
	}
	return nil
}

func (event *ChainSQLEvent) SubscribeTable(owner, tableName string) error {
	return fmt.Errorf("SubscribeTable dosen't be implemented.")
}

func (event *ChainSQLEvent) SyncWaitTxResult(txId string, seconds time.Duration) error {
	cmd := NewSubscribeCmd()
	cmd.SetTransaction(txId)
	cmd.SetCommand(Subscribe)
	request, err := BuildCmd(cmd, SubScribeType_transaction)
	if err != nil {
		golog.Error("ripple", "SyncWaitTxResult", "Build Subscribe failure on transaction", 0)
		return err
	}

	if err := event.sendScribleRequest(request); err != nil {
		golog.Error("ripple", "SyncWaitTxResult", "subscribe "+txId+" failure. "+err.Error(), 0)
		return err
	}

	// wait for result of specified txid
	ok := event.wait(seconds)
	// cancel subscribe
	event.cancelTxSubscribe(txId)
	return ok
}

func (event *ChainSQLEvent) cancelTxSubscribe(txId string) error {
	var err error
	var request []byte

	cmd := NewSubscribeCmd()
	cmd.SetTransaction(txId)
	cmd.SetCommand(Unsubscribe)
	request, err = BuildCmd(cmd, SubScribeType_transaction)
	golog.Info("ripple", "cancelTxSubscribe", string(request), 0)
	if err == nil {
		event.stop = true
		err = event.WsClient.WriteMessage(websocket.TextMessage, request)
	}
	return err
}
