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

	"github.com/flike/kingshard/mysql"
)

const (
	DefaultFrontendType  = "mysql"
	PostgresFrontendType = "postgres"
)

// FrontendProtocol isolates wire-level command dispatch and response encoding.
// SessionExecutor remains protocol-neutral while each frontend adapter decides
// how to decode commands and encode responses.
type FrontendProtocol interface {
	Name() string
	InitConn(*ClientConn)
	Handshake(*ClientConn) error
	Run(*ClientConn)
	ReadPacket(*ClientConn) ([]byte, error)
	WritePacket(*ClientConn, []byte) error
	WritePacketBatch(*ClientConn, []byte, []byte, bool) ([]byte, error)
	ResetSequence(*ClientConn)
	Dispatch(*ClientConn, []byte) error
	WriteOK(*ClientConn, *mysql.Result) error
	WriteError(*ClientConn, error) error
	WriteEOF(*ClientConn, uint16) error
	WriteEOFBatch(*ClientConn, []byte, uint16, bool) ([]byte, error)
	WriteResultset(*ClientConn, uint16, *mysql.Resultset) error
	WriteFieldList(*ClientConn, uint16, []*mysql.Field) error
	WritePrepare(*ClientConn, *Stmt) error
}

func NormalizeFrontendType(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", DefaultFrontendType:
		return DefaultFrontendType
	case PostgresFrontendType, "postgresql":
		return PostgresFrontendType
	default:
		return strings.ToLower(strings.TrimSpace(name))
	}
}

func NewFrontendProtocol(name string) (FrontendProtocol, error) {
	switch NormalizeFrontendType(name) {
	case DefaultFrontendType:
		return mysqlFrontendProtocol{}, nil
	case PostgresFrontendType:
		return postgresFrontendProtocol{}, nil
	default:
		return nil, fmt.Errorf("unsupported frontend type %q", name)
	}
}

func (c *ClientConn) dispatch(data []byte) error {
	return c.frontend.Dispatch(c, data)
}

func (c *ClientConn) initFrontend() {
	c.frontend.InitConn(c)
}

func (c *ClientConn) Handshake() error {
	return c.frontend.Handshake(c)
}

func (c *ClientConn) Run() {
	c.frontend.Run(c)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.frontend.ReadPacket(c)
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.frontend.WritePacket(c, data)
}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.frontend.WritePacketBatch(c, total, data, direct)
}

func (c *ClientConn) resetSequence() {
	c.frontend.ResetSequence(c)
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	return c.frontend.WriteOK(c, r)
}

func (c *ClientConn) writeError(err error) error {
	return c.frontend.WriteError(c, err)
}

func (c *ClientConn) writeEOF(status uint16) error {
	return c.frontend.WriteEOF(c, status)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	return c.frontend.WriteEOFBatch(c, total, status, direct)
}

func (c *ClientConn) writeResultset(status uint16, r *mysql.Resultset) error {
	return c.frontend.WriteResultset(c, status, r)
}

func (c *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	return c.frontend.WriteFieldList(c, status, fs)
}

func (c *ClientConn) writePrepare(s *Stmt) error {
	return c.frontend.WritePrepare(c, s)
}
