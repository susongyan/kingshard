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

	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/core/hack"
	"github.com/flike/kingshard/mysql"
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

			err = errors.ErrInternalServer
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
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
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
		if c.user == "root" {
			return c.handleAdmin(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.AdminHelp:
		if c.user == "root" {
			return c.handleAdminHelp(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.UseDB:
		return c.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		return c.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		return c.handleExec(stmt, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

// 获取shard的conn，第一个参数表示是不是select
func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	return c.newEmptyResultsetForSelectExprs(stmt.SelectExprs)
}

func (c *ClientConn) newEmptyResultsetForSelectExprs(exprs sqlparser.SelectExprs) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(exprs))

	for i, expr := range exprs {
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

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	plan, err := c.schema.rule.BuildPlan(c.db, stmt)
	if err != nil {
		return err
	}
	conns, err := c.getShardConns(false, plan)
	defer c.closeShardConns(conns, err != nil)
	if err != nil {
		golog.Error("ClientConn", "handleExec", err.Error(), c.connectionId)
		return err
	}
	if conns == nil {
		return c.writeOK(nil)
	}

	var rs []*mysql.Result

	rs, err = c.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	if err == nil {
		err = c.mergeExecResult(rs)
	}

	return err
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
