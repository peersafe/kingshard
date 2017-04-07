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
	"testing"

	"github.com/flike/kingshard/config"
)

func subscribeCallBack(result *SubscribeResponse) {
	//fmt.Printf("subscribeCallBack: %s\n", string(data))
	fmt.Printf("Owner:%s,Status:%s,Type:%s\n", result.Owner, result.Status, result.Type)
}

func TestSubScribe(t *testing.T) {
	cfg, _ := config.ParseConfigFile("F:\\work\\ks.yaml")
	s := NewSubScriber(cfg)
	s.Start("rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "peersafe", subscribeCallBack)
	fmt.Println("listen ...")

	wait := make(chan int, 1)
	<-wait
	fmt.Println("stop ...")
}
