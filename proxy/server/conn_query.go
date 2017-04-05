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

package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/core/hack"
	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/proxy/router"
	"github.com/flike/kingshard/ripple"
	"github.com/flike/kingshard/sqlparser"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}
			return
		}
	}()

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号
	hasHandled, err := c.preHandleShard(sql)
	if err != nil {
		golog.Error("server", "preHandleShard", err.Error(), 0,
			"sql", sql,
			"hasHandled", hasHandled,
		)
		return err
	}
	if hasHandled {
		return nil
	}

	var stmt sqlparser.Statement
	//解析sql语句,得到的stmt是一个interface
	if c.current_use != nil && len(c.current_use.Account) > 0 {
		stmt, err = sqlparser.Parse2(sql, c.current_use.Account, c.ws_conn, ripple.GetNameInDB)
	} else {
		stmt, err = sqlparser.Parse(sql)
	}

	if err != nil {
		golog.Error("server", "parse", err.Error(), 0, "hasHandled", hasHandled, "sql", sql)
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		return c.handleSelect(v, nil)
	case *sqlparser.Insert:
		return c.handleExec(stmt, nil)
	case *sqlparser.Update:
		return c.handleExec(stmt, nil)
	case *sqlparser.Delete:
		return c.handleExec(stmt, nil)
	case *sqlparser.Replace:
		return c.handleExec(stmt, nil)
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	case *sqlparser.Begin:
		return c.handleBegin()
	case *sqlparser.Commit:
		return c.handleCommit()
	case *sqlparser.Rollback:
		return c.handleRollback()
	case *sqlparser.Admin:
		return c.handleAdmin(v)
	case *sqlparser.AdminHelp:
		return c.handleAdminHelp(v)
	case *sqlparser.UseDB:
		return c.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		return c.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		return c.handleExec(stmt, nil)
	case *sqlparser.DDL:
		return c.handleDDL(v)
	default:
		return fmt.Errorf("statement %T not support now", v)
	}

	return nil
}

func (c *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	if !c.isInTransaction() {
		if fromSlave {
			co, err = n.GetSlaveConn()
			if err != nil {
				co, err = n.GetMasterConn()
			}
		} else {
			co, err = n.GetMasterConn()
		}
		if err != nil {
			golog.Error("server", "getBackendConn", err.Error(), 0)
			return
		}
	} else {
		var ok bool
		co, ok = c.txConns[n]

		if !ok {
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			if !c.isAutoCommit() {
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else {
				if err = co.Begin(); err != nil {
					return
				}
			}

			c.txConns[n] = co
		}
	}

	if err = co.UseDB(c.db); err != nil {
		//reset the database to null
		c.db = ""
		return
	}

	if err = co.SetCharset(c.charset, c.collation); err != nil {
		return
	}

	return
}

//获取shard的conn，第一个参数表示是不是select
func (c *ClientConn) getShardConns(fromSlave bool, plan *router.Plan) (map[string]*backend.BackendConn, error) {
	var err error
	if plan == nil || len(plan.RouteNodeIndexs) == 0 {
		return nil, errors.ErrNoRouteNode
	}

	nodesCount := len(plan.RouteNodeIndexs)
	nodes := make([]*backend.Node, 0, nodesCount)
	for i := 0; i < nodesCount; i++ {
		nodeIndex := plan.RouteNodeIndexs[i]
		nodes = append(nodes, c.proxy.GetNode(plan.Rule.Nodes[nodeIndex]))
	}
	if c.isInTransaction() {
		if 1 < len(nodes) {
			return nil, errors.ErrTransInMulti
		}
		//exec in multi node
		if len(c.txConns) == 1 && c.txConns[nodes[0]] == nil {
			return nil, errors.ErrTransInMulti
		}
	}
	conns := make(map[string]*backend.BackendConn)
	var co *backend.BackendConn
	for _, n := range nodes {
		co, err = c.getBackendConn(n, fromSlave)
		if err != nil {
			break
		}

		conns[n.Cfg.Name] = co
	}

	return conns, err
}

func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	if strings.ToLower(c.proxy.logSql[c.proxy.logSqlIndex]) != golog.LogSqlOff &&
		execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
		c.proxy.counter.IncrSlowLogTotal()
		golog.OutputSql(state, "%.1fms - %s->%s:%s",
			execTime,
			c.c.RemoteAddr(),
			conn.GetAddr(),
			sql,
		)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

func (c *ClientConn) executeInMultiNodes(conns map[string]*backend.BackendConn, sqls map[string][]string, args []interface{}) ([]*mysql.Result, error) {
	if len(conns) != len(sqls) {
		golog.Error("ClientConn", "executeInMultiNodes", errors.ErrConnNotEqual.Error(), c.connectionId,
			"conns", conns,
			"sqls", sqls,
		)
		return nil, errors.ErrConnNotEqual
	}

	var wg sync.WaitGroup

	if len(conns) == 0 {
		return nil, errors.ErrNoPlan
	}

	wg.Add(len(conns))

	resultCount := 0
	for _, sqlSlice := range sqls {
		resultCount += len(sqlSlice)
	}

	rs := make([]interface{}, resultCount)

	f := func(rs []interface{}, i int, execSqls []string, co *backend.BackendConn) {
		var state string
		for _, v := range execSqls {
			startTime := time.Now().UnixNano()
			r, err := co.Execute(v, args...)
			if err != nil {
				state = "ERROR"
				rs[i] = err
			} else {
				state = "OK"
				rs[i] = r
			}
			execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
			if c.proxy.logSql[c.proxy.logSqlIndex] != golog.LogSqlOff &&
				execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
				c.proxy.counter.IncrSlowLogTotal()
				golog.OutputSql(state, "%.1fms - %s->%s:%s",
					execTime,
					c.c.RemoteAddr(),
					co.GetAddr(),
					v,
				)
			}
			i++
		}
		wg.Done()
	}

	offsert := 0
	for nodeName, co := range conns {
		s := sqls[nodeName] //[]string
		go f(rs, offsert, s, co)
		offsert += len(s)
	}

	wg.Wait()

	var err error
	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		r[i] = rs[i].(*mysql.Result)
	}

	return r, err
}

func (c *ClientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	if rollback {
		conn.Rollback()
	}

	conn.Close()
}

func (c *ClientConn) closeShardConns(conns map[string]*backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			co.Rollback()
		}
		co.Close()
	}
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.NonStarExpr:
			if e.As != nil {
				r.Fields[i].Name = e.As
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

// Native SQL convert to ChainSQL and push to ChainSQL's node
func (c *ClientConn) exeSQLWriteToChainSQL(stmt sqlparser.Statement) error {
	tx := ripple.NewTransaction()

	var as_account string
	var as_secret string
	var use_account string

	if c.current_as == nil {
		as_account = c.proxy.cfg.Account
		as_secret = c.proxy.cfg.Secret
	} else {
		as_account = c.current_as.Account
		as_secret = c.current_as.Secret
	}
	if len(as_account) == 0 || len(as_secret) == 0 {
		return fmt.Errorf("Please use admin's commad provide opretor's account and secret")
	}

	tx.SetAccount(as_account)
	tx.SetSecret(as_secret)

	if c.current_use == nil {
		use_account = c.proxy.cfg.Account
	} else {
		use_account = c.current_use.Account
	}
	if len(use_account) == 0 {
		return fmt.Errorf("Please use admin's commad switch owner who owns tables")
	}

	tx.SetOwner(use_account)

	var err error
	switch v := stmt.(type) {
	case *sqlparser.Insert:
		err = ripple.HandleInsert(v, tx)
	case *sqlparser.Update:
		err = ripple.HandleUpdate(v, tx)
	case *sqlparser.Delete:
		err = ripple.HandleDelete(v, tx)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	if err != nil {
		return err
	}

	err = tx.WriteToChainSQL(c.ws_conn)

	if err == nil {
		result := &mysql.Result{
			Status:       0,
			InsertId:     0,
			AffectedRows: 1,

			Resultset: &mysql.Resultset{},
		}
		return c.writeOK(result)
	}

	return err

}

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	return c.exeSQLWriteToChainSQL(stmt)
}

func (c *ClientConn) handleDDL(ddl *sqlparser.DDL) error {
	tx := ripple.NewTransaction()

	var as_account string
	var as_secret string

	if c.current_as == nil {
		as_account = c.proxy.cfg.Account
		as_secret = c.proxy.cfg.Secret
	} else {
		as_account = c.current_as.Account
		as_secret = c.current_as.Secret
	}
	if len(as_account) == 0 || len(as_secret) == 0 {
		return fmt.Errorf("Please use admin's commad provide opretor's account and secret")
	}
	tx.SetAccount(as_account)
	tx.SetSecret(as_secret)

	// get nameInDB
	var use_account string
	if c.current_use == nil {
		use_account = c.proxy.cfg.Account
	} else {
		use_account = c.current_use.Account
	}
	if len(use_account) == 0 {
		return fmt.Errorf("Please use admin's commad switch owner who owns tables")
	}
	nameInDB, err := ripple.GetNameInDB(string(ddl.Table), use_account, c.ws_conn)
	if err != nil {
		return err
	}

	if err := ripple.HandleDDL(ddl, tx, nameInDB); err != nil {
		return err
	}

	if err := tx.SimpleWriteToChainSQL(c.ws_conn); err != nil {
		return err
	}

	result := &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 1,

		Resultset: &mysql.Resultset{},
	}
	return c.writeOK(result)
}

func (c *ClientConn) mergeExecResult(rs []*mysql.Result) error {
	r := new(mysql.Result)
	for _, v := range rs {
		r.Status |= v.Status
		r.AffectedRows += v.AffectedRows
		if r.InsertId == 0 {
			r.InsertId = v.InsertId
		} else if r.InsertId > v.InsertId {
			//last insert id is first gen id for multi row inserted
			//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
			r.InsertId = v.InsertId
		}
	}

	if r.InsertId > 0 {
		c.lastInsertId = int64(r.InsertId)
	}
	c.affectedRows = int64(r.AffectedRows)

	return c.writeOK(r)
}
