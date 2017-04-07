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
	"net/url"

	"github.com/flike/kingshard/config"
	"github.com/flike/kingshard/core/golog"
	"github.com/gorilla/websocket"
)

const (
	Subscribe   = "subscribe"
	Unsubscribe = "unsubscribe"

	SubScribeType_table       = "table"
	SubScribeType_transaction = "transaction"
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

///////////////////////////////////////
type SubscribeCallBack func(response *SubscribeResponse)

type SubScriber struct {
	cfg     *config.Config
	ws_conn *websocket.Conn
	cmd     *SubscribeCmd
	stop    bool
	OnEvnet SubscribeCallBack
}

func NewSubScriber(cfg *config.Config) *SubScriber {
	s := SubScriber{cfg: cfg, stop: false}
	s.cmd = NewSubscribeCmd()
	return &s
}

func (s *SubScriber) Start(owner, tableName string, cb SubscribeCallBack) error {
	// init websocket
	var ok error
	//subscribeAddr := flag.String("addr", s.cfg.WSAddr, "http service address")
	//flag.Parse()
	u := url.URL{Scheme: "ws", Host: s.cfg.WSAddr, Path: "/"}
	s.ws_conn, _, ok = websocket.DefaultDialer.Dial(u.String(), nil)
	if ok != nil {
		return ok
	}
	s.OnEvnet = cb

	s.cmd = NewSubscribeCmd()
	s.cmd.SetCommand("subscribe")
	s.cmd.SetOwner(owner)
	s.cmd.SetTableName(tableName)
	cmd, err := BuildCmd(s.cmd, SubScribeType_table)
	if err != nil {
		return err
	}

	if _, err := PushMessage(s.ws_conn, cmd); err != nil {
		s.ws_conn.Close()
		s.Stop()
		golog.Error("ripple", "subscribe.start ->", err.Error(), 0)
		return err
	}

	go func() {
		for s.stop == false {
			_, data, err := s.ws_conn.ReadMessage()
			if err == nil {
				if len(data) > 0 {
					s.OnChainSQLEvnet(data)
				}
			} else {
				golog.Error("ripple", "subscribe.start <-", err.Error(), 0)
				return
			}
		}
		golog.Info("ripple", "subscribe routine", "subscriber has stopped.", 0)
	}()

	return nil
}

func (s *SubScriber) Stop() {
	s.stop = true

	s.cmd.SetCommand("unsubscribe")
	cmd, ok := BuildCmd(s.cmd, SubScribeType_table)
	if ok != nil {
		return
	}

	if _, err := PushMessage(s.ws_conn, cmd); err != nil {
		s.ws_conn.Close()
		s.Stop()
		golog.Error("ripple", "subscribe.start ->", err.Error(), 0)
	}

	s.ws_conn.Close()
}

func (s *SubScriber) OnChainSQLEvnet(data []byte) {
	var result SubscribeResponse
	err := json.Unmarshal(data, &result)
	if err != nil {
		result.Status = "Unmarshal error. [" + err.Error() + "]"
	}

	if s.OnEvnet != nil {
		s.OnEvnet(&result)
	}
}
