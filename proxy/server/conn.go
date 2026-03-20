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
	"net"
	"sync"

	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/mysql"
)

// client <-> proxy
type ClientConn struct {
	sync.Mutex

	*SessionExecutor

	frontend FrontendProtocol

	frontendState interface{}

	c net.Conn

	connectionId uint32

	closed bool

	stmtId uint32

	stmts map[uint32]*Stmt //prepare相关,client端到proxy的stmt

}

var baseConnId uint32 = 10000

func (c *ClientConn) IsAllowConnect() bool {
	clientHost, _, err := net.SplitHostPort(c.c.RemoteAddr().String())
	if err != nil {
		fmt.Println(err)
	}
	clientIP := net.ParseIP(clientHost)

	current, _, _ := c.proxy.allowipsIndex.Get()
	ipVec := c.proxy.allowips[current]
	if ipVecLen := len(ipVec); ipVecLen == 0 {
		return true
	}
	for _, ip := range ipVec {
		if ip.Match(clientIP) {
			return true
		}
	}

	golog.Error("server", "IsAllowConnect", "error", mysql.ER_ACCESS_DENIED_ERROR,
		"ip address", c.c.RemoteAddr().String(), " access denied by kindshard.")
	return false
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) clean() {
	if c.txConns != nil && len(c.txConns) > 0 {
		for _, co := range c.txConns {
			co.Close()
		}
	}
}
