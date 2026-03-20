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
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"

	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/core/hack"
	"github.com/flike/kingshard/mysql"
)

var defaultCapability uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

type mysqlFrontendProtocol struct{}

type mysqlFrontendState struct {
	pkg        *mysql.PacketIO
	capability uint32
	salt       []byte
}

func (p mysqlFrontendProtocol) InitConn(c *ClientConn) {
	salt, _ := mysql.RandomBuf(20)
	state := &mysqlFrontendState{
		pkg:  mysql.NewPacketIO(c.c),
		salt: salt,
	}
	state.pkg.Sequence = 0
	c.frontendState = state
}

func (p mysqlFrontendProtocol) Name() string {
	return "mysql"
}

func (p mysqlFrontendProtocol) Handshake(c *ClientConn) error {
	if err := p.writeInitialHandshake(c); err != nil {
		golog.Error("server", "Handshake", err.Error(),
			c.connectionId, "msg", "send initial handshake error")
		return err
	}

	if err := p.readHandshakeResponse(c); err != nil {
		golog.Error("server", "readHandshakeResponse",
			err.Error(), c.connectionId,
			"msg", "read Handshake Response error")
		return err
	}

	if err := p.WriteOK(c, nil); err != nil {
		golog.Error("server", "readHandshakeResponse",
			"write ok fail",
			c.connectionId, "error", err.Error())
		return err
	}

	p.ResetSequence(c)
	return nil
}

func (p mysqlFrontendProtocol) Run(c *ClientConn) {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			golog.Error("ClientConn", "Run",
				err.Error(), 0,
				"stack", string(buf))
		}

		c.Close()
	}()
	defer c.clean()
	for {
		data, err := p.ReadPacket(c)

		if err != nil {
			return
		}

		if c.configVer != c.proxy.configVer {
			err := c.reloadConfig()
			if nil != err {
				golog.Error("ClientConn", "Run",
					err.Error(), c.connectionId,
				)
				c.writeError(err)
				return
			}
			c.configVer = c.proxy.configVer
			golog.Debug("ClientConn", "Run",
				fmt.Sprintf("config reload ok, ver:%d", c.configVer), c.connectionId,
			)
		}

		if err := p.Dispatch(c, data); err != nil {
			c.proxy.counter.IncrErrLogTotal()
			golog.Error("ClientConn", "Run",
				err.Error(), c.connectionId,
			)
			c.writeError(err)
			if err == mysql.ErrBadConn {
				c.Close()
			}
		}

		if c.closed {
			return
		}

		p.ResetSequence(c)
	}
}

func (p mysqlFrontendProtocol) ReadPacket(c *ClientConn) ([]byte, error) {
	return mysqlFrontendStateFor(c).pkg.ReadPacket()
}

func (p mysqlFrontendProtocol) WritePacket(c *ClientConn, data []byte) error {
	return mysqlFrontendStateFor(c).pkg.WritePacket(data)
}

func (p mysqlFrontendProtocol) WritePacketBatch(c *ClientConn, total, data []byte, direct bool) ([]byte, error) {
	return mysqlFrontendStateFor(c).pkg.WritePacketBatch(total, data, direct)
}

func (p mysqlFrontendProtocol) ResetSequence(c *ClientConn) {
	mysqlFrontendStateFor(c).pkg.Sequence = 0
}

func (p mysqlFrontendProtocol) Dispatch(c *ClientConn, data []byte) error {
	c.proxy.counter.IncrClientQPS()
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case mysql.COM_QUIT:
		c.handleRollback()
		c.Close()
		return nil
	case mysql.COM_QUERY:
		return c.handleQuery(hack.String(data))
	case mysql.COM_PING:
		return c.writeOK(nil)
	case mysql.COM_INIT_DB:
		return c.handleUseDB(hack.String(data))
	case mysql.COM_FIELD_LIST:
		return c.handleFieldList(data)
	case mysql.COM_STMT_PREPARE:
		return c.handleStmtPrepare(hack.String(data))
	case mysql.COM_STMT_EXECUTE:
		return c.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		return c.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		return c.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		return c.handleStmtReset(data)
	case mysql.COM_SET_OPTION:
		return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		golog.Error("ClientConn", "dispatch", msg, 0)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}
}

func (p mysqlFrontendProtocol) WriteOK(c *ClientConn, r *mysql.Result) error {
	state := mysqlFrontendStateFor(c)
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)
	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if state.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (p mysqlFrontendProtocol) WriteError(c *ClientConn, err error) error {
	state := mysqlFrontendStateFor(c)
	sqlErr, ok := err.(*mysql.SqlError)
	if !ok {
		sqlErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
	}

	data := make([]byte, 4, 16+len(sqlErr.Message))
	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(sqlErr.Code), byte(sqlErr.Code>>8))

	if state.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, sqlErr.State...)
	}

	data = append(data, sqlErr.Message...)

	return c.writePacket(data)
}

func (p mysqlFrontendProtocol) WriteEOF(c *ClientConn, status uint16) error {
	state := mysqlFrontendStateFor(c)
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if state.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func (p mysqlFrontendProtocol) WriteEOFBatch(c *ClientConn, total []byte, status uint16, direct bool) ([]byte, error) {
	state := mysqlFrontendStateFor(c)
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if state.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}

func (p mysqlFrontendProtocol) WriteResultset(c *ClientConn, status uint16, r *mysql.Resultset) error {
	c.affectedRows = int64(-1)
	total := make([]byte, 0, 4096)
	data := make([]byte, 4, 512)

	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data = append(data, columnLen...)
	var err error
	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, false)
	if err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}

func (p mysqlFrontendProtocol) WriteFieldList(c *ClientConn, status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	var err error
	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}

func (p mysqlFrontendProtocol) WritePrepare(c *ClientConn, s *Stmt) error {
	data := make([]byte, 4, 128)
	total := make([]byte, 0, 1024)

	data = append(data, 0)
	data = append(data, mysql.Uint32ToBytes(s.id)...)
	data = append(data, mysql.Uint16ToBytes(uint16(s.columns))...)
	data = append(data, mysql.Uint16ToBytes(uint16(s.params))...)
	data = append(data, 0)
	data = append(data, 0, 0)

	var err error
	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	if s.params > 0 {
		for i := 0; i < s.params; i++ {
			data = data[0:4]
			data = append(data, []byte(paramFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}
	}

	if s.columns > 0 {
		for i := 0; i < s.columns; i++ {
			data = data[0:4]
			data = append(data, []byte(columnFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writePacketBatch(total, nil, true)
	return err
}

func (p mysqlFrontendProtocol) writeInitialHandshake(c *ClientConn) error {
	state := mysqlFrontendStateFor(c)
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)

	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	// connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	// auth-plugin-data-part-1
	data = append(data, state.salt[0:8]...)

	// filter [00]
	data = append(data, 0)

	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(defaultCapability), byte(defaultCapability>>8))

	// charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	// status
	data = append(data, byte(c.status), byte(c.status>>8))

	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(defaultCapability>>16), byte(defaultCapability>>24))

	// filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	// auth-plugin-data-part-2
	data = append(data, state.salt[8:]...)

	// filter [00]
	data = append(data, 0)

	return p.WritePacket(c, data)
}

func (p mysqlFrontendProtocol) readHandshakeResponse(c *ClientConn) error {
	state := mysqlFrontendStateFor(c)
	data, err := p.ReadPacket(c)

	if err != nil {
		return err
	}

	pos := 0

	// capability
	state.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	// skip max packet size
	pos += 4

	// charset, skip, if you want to use another charset, use set names
	// c.collation = CollationId(data[pos])
	pos++

	// skip reserved 23[00]
	pos += 23

	// user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	// auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	// check user
	if _, ok := c.proxy.users[c.user]; !ok {
		golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"client_user", c.user,
			"config_set_user", c.user,
			"password", c.proxy.users[c.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	// check password
	checkAuth := mysql.CalcPassword(state.salt, []byte(c.proxy.users[c.user]))
	if !bytes.Equal(auth, checkAuth) {
		golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", c.user,
			"config_set_user", c.user,
			"password", c.proxy.users[c.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if state.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(db) + 1
	}
	c.db = db

	return nil
}

func mysqlFrontendStateFor(c *ClientConn) *mysqlFrontendState {
	state, ok := c.frontendState.(*mysqlFrontendState)
	if !ok || state == nil {
		panic("mysql frontend state is not initialized")
	}
	return state
}
