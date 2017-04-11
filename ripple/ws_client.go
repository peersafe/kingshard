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
	"time"

	"github.com/flike/kingshard/core/golog"
	"github.com/gorilla/websocket"
)

type RippleResponse struct {
	Result map[string]interface{}
	Status string
	Type   string
}

// push message into webserver and return response from webserver
func PushMessage(ws *websocket.Conn, data []byte) ([]byte, error) {
	timeout := make(chan bool, 1)
	prepare := make(chan bool, 1)
	recvData := make(chan []byte, 1)
	recvError := make(chan error, 1)

	go func() {
		defer close(timeout)
		time.Sleep(15e9) // wait for 15s
		if timeout != nil {
			timeout <- true
		}
	}()

	go func() {
		defer close(recvError)
		prepare <- true
		_, response, err := ws.ReadMessage()
		if err != nil {
			recvError <- err
			return
		}
		//fmt.Printf("PushMessage <- : %s\n", string(response))
		recvData <- response
	}()

	defer close(recvData)
	defer close(prepare)

	for {
		select {
		case <-prepare:
			//fmt.Printf("PushMessage -> : %s\n", string(data))
			if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
				golog.Error("ripple", "WriteMessage", string(data), 0)
				return nil, err
			}
		case data := <-recvData:
			return data, nil
		case err := <-recvError:
			return nil, err
		case <-timeout:
			return nil, errors.New("Timeout when receive data.")
		}
	}
	return nil, errors.New("Unkown error on PushMessage.")
}

func (result *RippleResponse) isSuccess() (bool, error) {
	if engine_result, ok := result.Result["engine_result"]; ok {
		if engine_result == "tesSUCCESS" {
			return true, nil
		} else {
			if result_msg, ok := result.Result["engine_result_message"]; ok {
				return false, errors.New(result_msg.(string))
			} else {
				j, _ := json.Marshal(result)
				return false, errors.New(string(j))
			}
		}
	} else if status, ok := result.Result["status"]; ok {
		if status == "success" {
			return true, nil
		} else {
			j, _ := json.Marshal(result)
			return false, errors.New(string(j))
		}
	} else if result.Status == "success" {
		return true, nil
	} else {
		j, _ := json.Marshal(result)
		return false, errors.New(string(j))
	}
}

func (result *RippleResponse) Hash() []byte {
	tx_json, ok := result.Result["tx_json"].(map[string]interface{})
	if ok == false {
		return nil
	}

	hash, ok := tx_json["hash"]
	if ok == false {
		return nil
	}

	return []byte(hash.(string))
}

func writePrepareToChainSQL(tx *Transaction, ws_conn *websocket.Conn) (*RippleResponse, error) {
	prepare, err := tx.BuildWSPrepare()
	if err != nil {
		return nil, err
	}

	// send prepare
	golog.Info("ripple", "writePrepareToChainSQL:request", string(prepare), 0)
	response, err := PushMessage(ws_conn, prepare)
	if err != nil {
		return nil, err
	}
	golog.Info("ripple", "writePrepareToChainSQL:response", string(response), 0)

	// parse prepare's response
	var prepare_response RippleResponse
	if err := json.Unmarshal(response, &prepare_response); err != nil {
		golog.Error("ripple", "writePrepareToChainSQL", err.Error(), 0)
		return nil, err
	}

	ok, err := prepare_response.isSuccess()
	if ok == false {
		return nil, err
	}

	return &prepare_response, nil
}

// return (hash,nil) if success,otherwise return(nil,error)
func writeWSRequestToChainSQL(tx *Transaction, prepare_response *RippleResponse, ws_conn *websocket.Conn) ([]byte, error) {
	// send request
	request, err := tx.BuildWSRequestByTxJson(prepare_response.Result)
	if err != nil {
		golog.Error("ripple", "writeWSRequestToChainSQL", err.Error(), 0)
		return nil, err
	}

	golog.Info("ripple", "writeWSRequestToChainSQL:request", string(request), 0)
	response, err := PushMessage(ws_conn, request)
	if err != nil {
		return nil, err
	}
	golog.Info("ripple", "writeWSRequestToChainSQL:response", string(response), 0)

	var result RippleResponse
	if err := json.Unmarshal(response, &result); err != nil {
		golog.Error("ripple", "writeWSRequestToChainSQL", err.Error(), 0)
		return nil, err
	}

	ok, err := result.isSuccess()
	if ok == false {
		return nil, err
	}

	return result.Hash(), nil
}

func implWriteToChainSQL(tx *Transaction, ws_conn *websocket.Conn,
	sync bool, seconds time.Duration, completed string) error {

	var err error
	var response *RippleResponse
	var hash []byte

	response, err = writePrepareToChainSQL(tx, ws_conn)
	if err != nil {
		return err
	}

	if hash, err = writeWSRequestToChainSQL(tx, response, ws_conn); err != nil {
		return err
	}

	if sync {
		event := NewChainSQLEvent(ws_conn, nil)
		if len(completed) == 0 {
			completed = "db_success"
		}
		event.Completed = completed
		if seconds <= 0 {
			seconds = 15
		}
		err = event.SyncWaitTxResult(string(hash), seconds)
	}
	return err
}

func (tx *Transaction) WriteToChainSQL(ws_conn *websocket.Conn) error {
	return implWriteToChainSQL(tx, ws_conn, false, 0, "")
}

func (tx *Transaction) SyncWriteToChainSQL(ws_conn *websocket.Conn,
	seconds time.Duration, completed string) error {

	return implWriteToChainSQL(tx, ws_conn, true, seconds, completed)
}

func GetNameInDB(tableName string, owner string, ws_conn *websocket.Conn) ([]byte, error) {
	tx := NewTransaction()
	tx.SetAccount(owner)
	tx.AddTableByName(tableName, false)

	request, err := tx.BuildDBNameRequest()
	if err != nil {
		return nil, err
	}

	golog.Info("ripple", "GetNameInDB", string(request), 0)
	response, err := PushMessage(ws_conn, request)
	if err != nil {
		return nil, err
	}
	golog.Info("ripple", "GetNameInDB", string(response), 0)

	var result RippleResponse
	if err := json.Unmarshal(response, &result); err != nil {
		golog.Error("ripple", "GetNameInDB", err.Error(), 0)
		return nil, err
	}

	if ok, _ := result.isSuccess(); ok {
		return []byte(result.Result["nameInDB"].(string)), nil
	}

	return nil, fmt.Errorf("%s", string(response))
}
