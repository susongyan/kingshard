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
	"strings"
	"sync"
	"time"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/proxy/router"
)

// SessionExecutor is the protocol-neutral execution core. ClientConn keeps the
// MySQL packet protocol, while session state, backend routing and transaction
// orchestration start to live here.
type SessionExecutor struct {
	proxy        *Server
	clientAddr   string
	connectionID uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	nodes  map[string]*backend.Node
	schema *Schema

	txConns map[*backend.Node]*backend.BackendConn

	lastInsertId int64
	affectedRows int64

	configVer uint32
}

func newSessionExecutor(proxy *Server, clientAddr string, connectionID uint32) *SessionExecutor {
	s := &SessionExecutor{
		proxy:        proxy,
		clientAddr:   clientAddr,
		connectionID: connectionID,
		status:       mysql.SERVER_STATUS_AUTOCOMMIT,
		charset:      mysql.DEFAULT_CHARSET,
		collation:    mysql.DEFAULT_COLLATION_ID,
		txConns:      make(map[*backend.Node]*backend.BackendConn),
	}
	s.syncConfigSnapshot()
	return s
}

func (s *SessionExecutor) syncConfigSnapshot() {
	s.proxy.configUpdateMutex.RLock()
	defer s.proxy.configUpdateMutex.RUnlock()
	s.nodes = s.proxy.nodes
	s.configVer = s.proxy.configVer
}

func (s *SessionExecutor) reloadConfig() error {
	s.proxy.configUpdateMutex.RLock()
	defer s.proxy.configUpdateMutex.RUnlock()
	s.schema = s.proxy.GetSchema(s.user)
	if s.schema == nil {
		return fmt.Errorf("schema of user [%s] is null or user is deleted", s.user)
	}
	s.nodes = s.proxy.nodes

	return nil
}

func (s *SessionExecutor) isInTransaction() bool {
	return s.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!s.isAutoCommit()
}

func (s *SessionExecutor) isAutoCommit() bool {
	return s.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (s *SessionExecutor) commit() (err error) {
	s.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range s.txConns {
		if e := co.Commit(); e != nil {
			err = e
		}
		co.Close()
	}

	s.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}

func (s *SessionExecutor) rollback() (err error) {
	s.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range s.txConns {
		if e := co.Rollback(); e != nil {
			err = e
		}
		co.Close()
	}

	s.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}

func (s *SessionExecutor) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	if !s.isInTransaction() {
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
		co, ok = s.txConns[n]

		if !ok {
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			if !s.isAutoCommit() {
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else {
				if err = co.Begin(); err != nil {
					return
				}
			}

			s.txConns[n] = co
		}
	}

	if err = co.UseDB(s.db); err != nil {
		s.db = ""
		return
	}

	if err = co.SetCharset(s.charset, s.collation); err != nil {
		return
	}

	return
}

func (s *SessionExecutor) getShardConns(fromSlave bool, plan *router.Plan) (map[string]*backend.BackendConn, error) {
	var err error
	if plan == nil || len(plan.RouteNodeIndexs) == 0 {
		return nil, errors.ErrNoRouteNode
	}

	nodesCount := len(plan.RouteNodeIndexs)
	nodes := make([]*backend.Node, 0, nodesCount)
	for i := 0; i < nodesCount; i++ {
		nodeIndex := plan.RouteNodeIndexs[i]
		nodes = append(nodes, s.proxy.GetNode(plan.Rule.Nodes[nodeIndex]))
	}
	if s.isInTransaction() {
		if len(nodes) > 1 {
			return nil, errors.ErrTransInMulti
		}
		if len(s.txConns) == 1 && s.txConns[nodes[0]] == nil {
			return nil, errors.ErrTransInMulti
		}
	}
	conns := make(map[string]*backend.BackendConn)
	var co *backend.BackendConn
	for _, n := range nodes {
		co, err = s.getBackendConn(n, fromSlave)
		if err != nil {
			break
		}

		conns[n.Cfg.Name] = co
	}

	return conns, err
}

func (s *SessionExecutor) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	if s.shouldLogSQL(execTime) {
		s.proxy.counter.IncrSlowLogTotal()
		golog.OutputSql(state, "%.1fms - %s->%s:%s",
			execTime,
			s.clientAddr,
			conn.GetAddr(),
			sql,
		)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

func (s *SessionExecutor) executeInMultiNodes(conns map[string]*backend.BackendConn, sqls map[string][]string, args []interface{}) ([]*mysql.Result, error) {
	if len(conns) != len(sqls) {
		golog.Error("SessionExecutor", "executeInMultiNodes", errors.ErrConnNotEqual.Error(), s.connectionID,
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
			if s.shouldLogSQL(execTime) {
				s.proxy.counter.IncrSlowLogTotal()
				golog.OutputSql(state, "%.1fms - %s->%s:%s",
					execTime,
					s.clientAddr,
					co.GetAddr(),
					v,
				)
			}
			i++
		}
		wg.Done()
	}

	offset := 0
	for nodeName, co := range conns {
		execSQLs := sqls[nodeName]
		go f(rs, offset, execSQLs, co)
		offset += len(execSQLs)
	}

	wg.Wait()

	var err error
	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		if rs[i] != nil {
			r[i] = rs[i].(*mysql.Result)
		}
	}

	return r, err
}

func (s *SessionExecutor) closeConn(conn *backend.BackendConn, rollback bool) {
	if s.isInTransaction() {
		return
	}
	defer conn.Close()
	if rollback {
		conn.Rollback()
	}
}

func (s *SessionExecutor) closeShardConns(conns map[string]*backend.BackendConn, rollback bool) {
	if s.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			co.Rollback()
		}
		co.Close()
	}
}

func (s *SessionExecutor) shouldLogSQL(execTime float64) bool {
	return strings.ToLower(s.proxy.logSql[s.proxy.logSqlIndex]) != golog.LogSqlOff &&
		execTime >= float64(s.proxy.slowLogTime[s.proxy.slowLogTimeIndex])
}
