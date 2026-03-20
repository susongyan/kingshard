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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/sqlparser"
)

const (
	postgresProtocolVersion30   int32 = 196608
	postgresSSLRequestCode      int32 = 80877103
	postgresCancelRequestCode   int32 = 80877102
	postgresAuthOK              int32 = 0
	postgresAuthCleartext       int32 = 3
	postgresReadyForQueryIdle   byte  = 'I'
	postgresReadyForQueryTxn    byte  = 'T'
	postgresSeverityError             = "ERROR"
	postgresSeverityFatal             = "FATAL"
	postgresFeatureNotSupported       = "0A000"
	postgresProtocolViolation         = "08P01"
	postgresInternalError             = "XX000"
	postgresInvalidAuth               = "28000"
	postgresInvalidPassword           = "28P01"
)

type postgresFrontendProtocol struct{}

type postgresFrontendState struct {
	reader          *bufio.Reader
	writer          *bufio.Writer
	secretKey       int32
	authenticated   bool
	awaitingSync    bool
	ignoreTillSync  bool
	commandTag      postgresCommandTag
	rowDescOverride []*mysql.Field
	statements      map[string]postgresPreparedStatement
	portals         map[string]postgresPortal
}

type postgresCommandTag struct {
	base        string
	includeRows bool
	insert      bool
}

type postgresPreparedStatement struct {
	name           string
	originalQuery  string
	rewrittenQuery string
	statement      sqlparser.Statement
	commandTag     postgresCommandTag
	paramCount     int
	paramOrder     []int
	paramTypes     []int32
	resultFields   []*mysql.Field
}

type postgresPortal struct {
	name          string
	statementName string
	args          []interface{}
	resultFormats []int16
}

type postgresProtocolError struct {
	severity string
	code     string
	message  string
}

func (e *postgresProtocolError) Error() string {
	return e.message
}

func (t postgresCommandTag) complete(rows uint64) string {
	switch {
	case t.insert:
		return fmt.Sprintf("INSERT 0 %d", rows)
	case t.includeRows:
		return fmt.Sprintf("%s %d", t.base, rows)
	case t.base == "":
		return "OK"
	default:
		return t.base
	}
}

func newPostgresProtocolError(code, message string) error {
	return &postgresProtocolError{
		severity: postgresSeverityError,
		code:     code,
		message:  message,
	}
}

func newPostgresFatalError(code, message string) error {
	return &postgresProtocolError{
		severity: postgresSeverityFatal,
		code:     code,
		message:  message,
	}
}

func (p postgresFrontendProtocol) Name() string {
	return PostgresFrontendType
}

func (p postgresFrontendProtocol) InitConn(c *ClientConn) {
	c.frontendState = &postgresFrontendState{
		reader:     bufio.NewReader(c.c),
		writer:     bufio.NewWriter(c.c),
		secretKey:  int32(c.connectionId ^ 0x4b534847),
		statements: make(map[string]postgresPreparedStatement),
		portals:    make(map[string]postgresPortal),
	}
}

func (p postgresFrontendProtocol) Handshake(c *ClientConn) error {
	params, err := p.readStartup(c)
	if err != nil {
		return err
	}

	user := params["user"]
	if user == "" {
		return newPostgresFatalError(postgresInvalidAuth, "startup packet is missing user")
	}

	password, ok := c.proxy.users[user]
	if !ok {
		return newPostgresFatalError(postgresInvalidAuth, fmt.Sprintf("unknown user %q", user))
	}

	c.user = user
	c.db = params["database"]
	if c.db == "" {
		c.db = params["dbname"]
	}

	if password != "" {
		if err := p.sendAuthenticationRequest(c, postgresAuthCleartext); err != nil {
			return err
		}

		clientPassword, err := p.readPasswordMessage(c)
		if err != nil {
			return err
		}
		if clientPassword != password {
			return newPostgresFatalError(postgresInvalidPassword, "password authentication failed")
		}
	}

	if err := p.sendAuthenticationRequest(c, postgresAuthOK); err != nil {
		return err
	}
	if err := p.sendParameterStatus(c, "server_version", "16.0-kingshard"); err != nil {
		return err
	}
	if err := p.sendParameterStatus(c, "server_encoding", "UTF8"); err != nil {
		return err
	}
	if err := p.sendParameterStatus(c, "client_encoding", "UTF8"); err != nil {
		return err
	}
	if err := p.sendParameterStatus(c, "DateStyle", "ISO, MDY"); err != nil {
		return err
	}
	if err := p.sendParameterStatus(c, "integer_datetimes", "on"); err != nil {
		return err
	}
	if err := p.sendBackendKeyData(c, int32(c.connectionId), postgresFrontendStateFor(c).secretKey); err != nil {
		return err
	}

	postgresFrontendStateFor(c).authenticated = true
	return p.sendReadyForQuery(c, postgresReadyForQueryIdle)
}

func (p postgresFrontendProtocol) Run(c *ClientConn) {
	defer c.Close()
	defer c.clean()

	for {
		data, err := p.ReadPacket(c)
		if err != nil {
			return
		}

		if c.configVer != c.proxy.configVer {
			err := c.reloadConfig()
			if err != nil {
				_ = c.writeError(err)
				return
			}
			c.configVer = c.proxy.configVer
		}

		if err := p.Dispatch(c, data); err != nil {
			c.proxy.counter.IncrErrLogTotal()
			if writeErr := c.writeError(err); writeErr != nil {
				return
			}
		}

		if c.closed {
			return
		}
	}
}

func (p postgresFrontendProtocol) ReadPacket(c *ClientConn) ([]byte, error) {
	state := postgresFrontendStateFor(c)

	msgType, err := state.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(state.reader, lengthBuf); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 4 {
		return nil, newPostgresProtocolError(postgresProtocolViolation, fmt.Sprintf("invalid message length %d", length))
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(state.reader, payload); err != nil {
		return nil, err
	}

	return append([]byte{msgType}, payload...), nil
}

func (p postgresFrontendProtocol) WritePacket(c *ClientConn, data []byte) error {
	state := postgresFrontendStateFor(c)
	if _, err := state.writer.Write(data); err != nil {
		return err
	}
	return state.writer.Flush()
}

func (p postgresFrontendProtocol) WritePacketBatch(c *ClientConn, total, data []byte, direct bool) ([]byte, error) {
	total = append(total, data...)
	if !direct {
		return total, nil
	}
	return nil, p.WritePacket(c, total)
}

func (p postgresFrontendProtocol) ResetSequence(*ClientConn) {}

func (p postgresFrontendProtocol) Dispatch(c *ClientConn, data []byte) error {
	state := postgresFrontendStateFor(c)
	msgType := data[0]
	payload := data[1:]

	if state.ignoreTillSync {
		switch msgType {
		case 'S':
			state.awaitingSync = false
			state.ignoreTillSync = false
			state.commandTag = postgresCommandTag{}
			return p.sendReadyForQuery(c, postgresReadyStatusFor(c))
		case 'H':
			return nil
		case 'X':
			c.Close()
			return nil
		default:
			return nil
		}
	}

	switch msgType {
	case 'Q':
		query, err := postgresReadCString(payload)
		if err != nil {
			return err
		}
		query = strings.TrimSpace(query)
		state.awaitingSync = false
		state.commandTag = postgresCommandTagForQuery(query)
		return c.handleQuery(query)
	case 'P':
		state.awaitingSync = true
		return p.handleParse(c, payload)
	case 'B':
		state.awaitingSync = true
		return p.handleBind(c, payload)
	case 'D':
		state.awaitingSync = true
		return p.handleDescribe(c, payload)
	case 'C':
		state.awaitingSync = true
		return p.handleClose(c, payload)
	case 'E':
		state.awaitingSync = true
		return p.handleExecute(c, payload)
	case 'S':
		state.awaitingSync = false
		state.ignoreTillSync = false
		state.commandTag = postgresCommandTag{}
		return p.sendReadyForQuery(c, postgresReadyStatusFor(c))
	case 'H':
		return nil
	case 'X':
		c.Close()
		return nil
	default:
		return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("frontend message %q is not implemented yet", msgType))
	}
}

func (p postgresFrontendProtocol) WriteOK(c *ClientConn, r *mysql.Result) error {
	state := postgresFrontendStateFor(c)
	state.rowDescOverride = nil
	rows := uint64(0)
	if r != nil {
		rows = r.AffectedRows
	}
	tag := state.commandTag.complete(rows)
	state.commandTag = postgresCommandTag{}

	if err := p.sendCommandComplete(c, tag); err != nil {
		return err
	}
	if state.awaitingSync {
		return nil
	}
	return p.sendReadyForQuery(c, postgresReadyStatusFor(c))
}

func (p postgresFrontendProtocol) WriteError(c *ClientConn, err error) error {
	pgErr := postgresProtocolErrorFrom(err)
	if writeErr := p.sendErrorResponse(c, pgErr.severity, pgErr.code, pgErr.message); writeErr != nil {
		return writeErr
	}

	state := postgresFrontendStateFor(c)
	state.commandTag = postgresCommandTag{}
	state.rowDescOverride = nil
	if !state.authenticated || pgErr.severity == postgresSeverityFatal {
		return nil
	}
	if state.awaitingSync {
		state.ignoreTillSync = true
		return nil
	}

	return p.sendReadyForQuery(c, postgresReadyStatusFor(c))
}

func (p postgresFrontendProtocol) WriteEOF(*ClientConn, uint16) error {
	return newPostgresProtocolError(postgresFeatureNotSupported, "mysql EOF packets are not used by the postgres frontend")
}

func (p postgresFrontendProtocol) WriteEOFBatch(*ClientConn, []byte, uint16, bool) ([]byte, error) {
	return nil, newPostgresProtocolError(postgresFeatureNotSupported, "mysql EOF batches are not used by the postgres frontend")
}

func (p postgresFrontendProtocol) WriteResultset(c *ClientConn, _ uint16, r *mysql.Resultset) error {
	state := postgresFrontendStateFor(c)
	rows, err := postgresResultRows(r)
	if err != nil {
		return err
	}

	fields := r.Fields
	if len(state.rowDescOverride) == len(r.Fields) && len(state.rowDescOverride) > 0 {
		fields = state.rowDescOverride
	}
	state.rowDescOverride = nil

	if err := p.sendRowDescription(c, fields); err != nil {
		return err
	}

	for _, row := range rows {
		if err := p.sendDataRow(c, row); err != nil {
			return err
		}
	}

	tag := state.commandTag.complete(uint64(len(rows)))
	state.commandTag = postgresCommandTag{}
	if err := p.sendCommandComplete(c, tag); err != nil {
		return err
	}

	if state.awaitingSync {
		return nil
	}
	return p.sendReadyForQuery(c, postgresReadyStatusFor(c))
}

func (p postgresFrontendProtocol) WriteFieldList(*ClientConn, uint16, []*mysql.Field) error {
	return newPostgresProtocolError(postgresFeatureNotSupported, "field list encoding is not implemented for the postgres frontend")
}

func (p postgresFrontendProtocol) WritePrepare(*ClientConn, *Stmt) error {
	return newPostgresProtocolError(postgresFeatureNotSupported, "prepared statement encoding is not implemented for the postgres frontend")
}

func (p postgresFrontendProtocol) readStartup(c *ClientConn) (map[string]string, error) {
	for {
		payload, code, err := p.readStartupPacket(c)
		if err != nil {
			return nil, err
		}

		switch code {
		case postgresSSLRequestCode:
			if err := p.WritePacket(c, []byte{'N'}); err != nil {
				return nil, err
			}
		case postgresCancelRequestCode:
			return nil, newPostgresFatalError(postgresFeatureNotSupported, "cancel requests are not implemented yet")
		case postgresProtocolVersion30:
			return postgresParseStartupParameters(payload)
		default:
			return nil, newPostgresFatalError(postgresProtocolViolation, fmt.Sprintf("unsupported startup protocol %d", code))
		}
	}
}

func (p postgresFrontendProtocol) readStartupPacket(c *ClientConn) ([]byte, int32, error) {
	state := postgresFrontendStateFor(c)

	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(state.reader, lengthBuf); err != nil {
		return nil, 0, err
	}

	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 8 {
		return nil, 0, newPostgresFatalError(postgresProtocolViolation, fmt.Sprintf("invalid startup packet length %d", length))
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(state.reader, payload); err != nil {
		return nil, 0, err
	}

	code := int32(binary.BigEndian.Uint32(payload[:4]))
	return payload[4:], code, nil
}

func (p postgresFrontendProtocol) readPasswordMessage(c *ClientConn) (string, error) {
	data, err := p.ReadPacket(c)
	if err != nil {
		return "", err
	}
	if data[0] != 'p' {
		return "", newPostgresFatalError(postgresProtocolViolation, fmt.Sprintf("expected password message, got %q", data[0]))
	}

	password, err := postgresReadCString(data[1:])
	if err != nil {
		return "", err
	}
	return password, nil
}

func (p postgresFrontendProtocol) handleParse(c *ClientConn, payload []byte) error {
	name, rest, err := postgresReadCStringWithRest(payload)
	if err != nil {
		return err
	}
	query, rest, err := postgresReadCStringWithRest(rest)
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return newPostgresProtocolError(postgresProtocolViolation, "parse message is truncated")
	}
	declaredCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) < declaredCount*4 {
		return newPostgresProtocolError(postgresProtocolViolation, "parse parameter type list is truncated")
	}

	rewrittenQuery, paramOrder, logicalParamCount, err := postgresRewriteBindPlaceholders(query)
	if err != nil {
		return err
	}
	if declaredCount != 0 && declaredCount != logicalParamCount {
		return newPostgresProtocolError(postgresProtocolViolation, fmt.Sprintf("parse declared %d parameters but query uses %d", declaredCount, logicalParamCount))
	}

	paramTypes := make([]int32, logicalParamCount)
	for i := 0; i < declaredCount; i++ {
		paramTypes[i] = int32(binary.BigEndian.Uint32(rest[i*4 : (i+1)*4]))
	}

	stmt, err := sqlparser.Parse(strings.TrimRight(strings.TrimSpace(rewrittenQuery), ";"))
	if err != nil {
		return err
	}

	state := postgresFrontendStateFor(c)
	prepared := postgresPreparedStatement{
		name:           name,
		originalQuery:  query,
		rewrittenQuery: rewrittenQuery,
		statement:      stmt,
		commandTag:     postgresCommandTagForQuery(query),
		paramCount:     logicalParamCount,
		paramOrder:     paramOrder,
		paramTypes:     paramTypes,
	}
	if err := p.resolvePreparedStatementMetadata(c, &prepared); err != nil {
		return err
	}
	state.statements[name] = prepared
	return p.sendParseComplete(c)
}

func (p postgresFrontendProtocol) handleBind(c *ClientConn, payload []byte) error {
	portalName, rest, err := postgresReadCStringWithRest(payload)
	if err != nil {
		return err
	}
	statementName, rest, err := postgresReadCStringWithRest(rest)
	if err != nil {
		return err
	}

	state := postgresFrontendStateFor(c)
	statement, ok := state.statements[statementName]
	if !ok {
		return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("prepared statement %q was not found", statementName))
	}

	paramFormats, rest, err := postgresReadFormatCodes(rest)
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return newPostgresProtocolError(postgresProtocolViolation, "bind message is truncated before parameters")
	}
	paramCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if paramCount != statement.paramCount {
		return newPostgresProtocolError(postgresProtocolViolation, fmt.Sprintf("bind supplied %d parameters but statement expects %d", paramCount, statement.paramCount))
	}

	rawArgs := make([]interface{}, statement.paramCount)
	for i := 0; i < statement.paramCount; i++ {
		if len(rest) < 4 {
			return newPostgresProtocolError(postgresProtocolViolation, "bind parameter payload is truncated")
		}
		valueLen := int(int32(binary.BigEndian.Uint32(rest[:4])))
		rest = rest[4:]
		if valueLen == -1 {
			rawArgs[i] = nil
			continue
		}
		if valueLen < 0 || len(rest) < valueLen {
			return newPostgresProtocolError(postgresProtocolViolation, "bind parameter value is truncated")
		}

		formatCode, err := postgresFormatCodeFor(paramFormats, i)
		if err != nil {
			return err
		}
		value, err := postgresDecodeBindValue(rest[:valueLen], formatCode, statement.paramTypes[i])
		if err != nil {
			return err
		}
		rawArgs[i] = value
		rest = rest[valueLen:]
	}

	resultFormats, rest, err := postgresReadFormatCodes(rest)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return newPostgresProtocolError(postgresProtocolViolation, "bind message has trailing payload")
	}
	for i := 0; i < len(resultFormats); i++ {
		if resultFormats[i] != 0 {
			return newPostgresProtocolError(postgresFeatureNotSupported, "binary result formats are not implemented for the postgres frontend")
		}
	}

	state.portals[portalName] = postgresPortal{
		name:          portalName,
		statementName: statementName,
		args:          postgresExpandBindArgs(rawArgs, statement.paramOrder),
		resultFormats: resultFormats,
	}
	return p.sendBindComplete(c)
}

func (p postgresFrontendProtocol) handleDescribe(c *ClientConn, payload []byte) error {
	if len(payload) < 1 {
		return newPostgresProtocolError(postgresProtocolViolation, "describe message is truncated")
	}
	name, _, err := postgresReadCStringWithRest(payload[1:])
	if err != nil {
		return err
	}

	state := postgresFrontendStateFor(c)
	switch payload[0] {
	case 'S':
		statement, ok := state.statements[name]
		if !ok {
			return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("prepared statement %q was not found", name))
		}
		if err := p.resolvePreparedStatementMetadata(c, &statement); err != nil {
			return err
		}
		state.statements[name] = statement
		if err := p.sendParameterDescription(c, statement.paramTypes); err != nil {
			return err
		}
		fields, err := p.describePreparedStatement(c, &statement)
		if err != nil {
			return err
		}
		state.statements[name] = statement
		if len(fields) == 0 {
			return p.sendNoData(c)
		}
		return p.sendRowDescription(c, fields)
	case 'P':
		portal, ok := state.portals[name]
		if !ok {
			return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("portal %q was not found", name))
		}
		statement, ok := state.statements[portal.statementName]
		if !ok {
			return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("prepared statement %q was not found", portal.statementName))
		}
		if err := p.resolvePreparedStatementMetadata(c, &statement); err != nil {
			return err
		}
		state.statements[portal.statementName] = statement
		fields, err := p.describePreparedStatement(c, &statement)
		if err != nil {
			return err
		}
		if len(fields) == 0 {
			return p.sendNoData(c)
		}
		return p.sendRowDescription(c, fields)
	default:
		return newPostgresProtocolError(postgresProtocolViolation, fmt.Sprintf("unsupported describe target %q", payload[0]))
	}
}

func (p postgresFrontendProtocol) handleClose(c *ClientConn, payload []byte) error {
	if len(payload) < 1 {
		return newPostgresProtocolError(postgresProtocolViolation, "close message is truncated")
	}

	name, _, err := postgresReadCStringWithRest(payload[1:])
	if err != nil {
		return err
	}

	state := postgresFrontendStateFor(c)
	switch payload[0] {
	case 'S':
		delete(state.statements, name)
	case 'P':
		delete(state.portals, name)
	default:
		return newPostgresProtocolError(postgresProtocolViolation, fmt.Sprintf("unsupported close target %q", payload[0]))
	}

	return p.sendCloseComplete(c)
}

func (p postgresFrontendProtocol) handleExecute(c *ClientConn, payload []byte) error {
	portalName, rest, err := postgresReadCStringWithRest(payload)
	if err != nil {
		return err
	}
	if len(rest) < 4 {
		return newPostgresProtocolError(postgresProtocolViolation, "execute message is truncated")
	}

	state := postgresFrontendStateFor(c)
	portal, ok := state.portals[portalName]
	if !ok {
		return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("portal %q was not bound", portalName))
	}
	statement, ok := state.statements[portal.statementName]
	if !ok {
		return newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("prepared statement %q was not found", portal.statementName))
	}
	if err := p.resolvePreparedStatementMetadata(c, &statement); err != nil {
		return err
	}
	state.statements[portal.statementName] = statement

	state.commandTag = statement.commandTag
	state.rowDescOverride = statement.resultFields
	defer func() {
		state.rowDescOverride = nil
	}()
	return c.executePreparedStatement(statement.statement, strings.TrimSpace(statement.rewrittenQuery), portal.args)
}

func (p postgresFrontendProtocol) sendAuthenticationRequest(c *ClientConn, authType int32) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(authType))
	return p.sendMessage(c, 'R', payload)
}

func (p postgresFrontendProtocol) sendParameterStatus(c *ClientConn, key, value string) error {
	payload := make([]byte, 0, len(key)+len(value)+2)
	payload = append(payload, key...)
	payload = append(payload, 0)
	payload = append(payload, value...)
	payload = append(payload, 0)
	return p.sendMessage(c, 'S', payload)
}

func (p postgresFrontendProtocol) sendBackendKeyData(c *ClientConn, processID, secretKey int32) error {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[0:4], uint32(processID))
	binary.BigEndian.PutUint32(payload[4:8], uint32(secretKey))
	return p.sendMessage(c, 'K', payload)
}

func (p postgresFrontendProtocol) sendReadyForQuery(c *ClientConn, status byte) error {
	return p.sendMessage(c, 'Z', []byte{status})
}

func (p postgresFrontendProtocol) sendParseComplete(c *ClientConn) error {
	return p.sendMessage(c, '1', nil)
}

func (p postgresFrontendProtocol) sendBindComplete(c *ClientConn) error {
	return p.sendMessage(c, '2', nil)
}

func (p postgresFrontendProtocol) sendCloseComplete(c *ClientConn) error {
	return p.sendMessage(c, '3', nil)
}

func (p postgresFrontendProtocol) sendNoData(c *ClientConn) error {
	return p.sendMessage(c, 'n', nil)
}

func (p postgresFrontendProtocol) sendCommandComplete(c *ClientConn, tag string) error {
	payload := append([]byte(tag), 0)
	return p.sendMessage(c, 'C', payload)
}

func (p postgresFrontendProtocol) sendParameterDescription(c *ClientConn, paramTypes []int32) error {
	payload := make([]byte, 0, 2+len(paramTypes)*4)
	payload = postgresAppendInt16(payload, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		payload = postgresAppendInt32(payload, oid)
	}
	return p.sendMessage(c, 't', payload)
}

func (p postgresFrontendProtocol) sendRowDescription(c *ClientConn, fields []*mysql.Field) error {
	payload := make([]byte, 0, 128)
	payload = postgresAppendInt16(payload, int16(len(fields)))

	for _, field := range fields {
		name := postgresFieldName(field)
		tableOID, attrNumber, oid, size, modifier, format := postgresFieldDescription(field)

		payload = append(payload, name...)
		payload = append(payload, 0)
		payload = postgresAppendUint32(payload, tableOID)
		payload = postgresAppendUint16(payload, attrNumber)
		payload = postgresAppendUint32(payload, oid)
		payload = postgresAppendInt16(payload, size)
		payload = postgresAppendInt32(payload, modifier)
		payload = postgresAppendInt16(payload, format)
	}

	return p.sendMessage(c, 'T', payload)
}

func (p postgresFrontendProtocol) sendDataRow(c *ClientConn, row []interface{}) error {
	payload := make([]byte, 0, 128)
	payload = postgresAppendInt16(payload, int16(len(row)))

	for _, value := range row {
		if value == nil {
			payload = postgresAppendInt32(payload, -1)
			continue
		}

		text, err := postgresTextValue(value)
		if err != nil {
			return err
		}

		payload = postgresAppendInt32(payload, int32(len(text)))
		payload = append(payload, text...)
	}

	return p.sendMessage(c, 'D', payload)
}

func (p postgresFrontendProtocol) sendErrorResponse(c *ClientConn, severity, code, message string) error {
	payload := make([]byte, 0, len(severity)+len(code)+len(message)+8)
	payload = append(payload, 'S')
	payload = append(payload, severity...)
	payload = append(payload, 0)
	payload = append(payload, 'C')
	payload = append(payload, code...)
	payload = append(payload, 0)
	payload = append(payload, 'M')
	payload = append(payload, message...)
	payload = append(payload, 0)
	payload = append(payload, 0)
	return p.sendMessage(c, 'E', payload)
}

func (p postgresFrontendProtocol) sendMessage(c *ClientConn, msgType byte, payload []byte) error {
	packet := make([]byte, 1, len(payload)+5)
	packet[0] = msgType

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(payload)+4))
	packet = append(packet, length...)
	packet = append(packet, payload...)

	return p.WritePacket(c, packet)
}

func postgresParseStartupParameters(data []byte) (map[string]string, error) {
	params := make(map[string]string)
	for len(data) > 0 {
		if data[0] == 0 {
			return params, nil
		}

		key, rest, err := postgresReadCStringWithRest(data)
		if err != nil {
			return nil, err
		}
		value, rest, err := postgresReadCStringWithRest(rest)
		if err != nil {
			return nil, err
		}
		params[key] = value
		data = rest
	}

	return params, newPostgresFatalError(postgresProtocolViolation, "startup parameters are truncated")
}

func postgresReadCString(data []byte) (string, error) {
	value, _, err := postgresReadCStringWithRest(data)
	return value, err
}

func postgresReadCStringWithRest(data []byte) (string, []byte, error) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), data[i+1:], nil
		}
	}

	return "", nil, newPostgresFatalError(postgresProtocolViolation, "cstring terminator was not found")
}

func postgresReadFormatCodes(data []byte) ([]int16, []byte, error) {
	if len(data) < 2 {
		return nil, nil, newPostgresProtocolError(postgresProtocolViolation, "format code list is truncated")
	}

	count := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	need := count * 2
	if len(data) < need {
		return nil, nil, newPostgresProtocolError(postgresProtocolViolation, "format code list payload is truncated")
	}

	codes := make([]int16, count)
	for i := 0; i < count; i++ {
		codes[i] = int16(binary.BigEndian.Uint16(data[i*2 : (i+1)*2]))
	}

	return codes, data[need:], nil
}

func postgresFormatCodeFor(codes []int16, index int) (int16, error) {
	switch len(codes) {
	case 0:
		return 0, nil
	case 1:
		return codes[0], nil
	default:
		if index >= len(codes) {
			return 0, newPostgresProtocolError(postgresProtocolViolation, "parameter format code count mismatch")
		}
		return codes[index], nil
	}
}

func postgresExpandBindArgs(rawArgs []interface{}, order []int) []interface{} {
	if len(order) == 0 {
		return nil
	}

	args := make([]interface{}, len(order))
	for i, paramIndex := range order {
		args[i] = rawArgs[paramIndex-1]
	}

	return args
}

func postgresDecodeBindValue(data []byte, formatCode int16, oid int32) (interface{}, error) {
	switch formatCode {
	case 0:
		return postgresDecodeTextBindValue(string(data), oid), nil
	case 1:
		return postgresDecodeBinaryBindValue(data, oid)
	default:
		return nil, newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("parameter format %d is not implemented", formatCode))
	}
}

func postgresDecodeTextBindValue(text string, oid int32) interface{} {
	switch oid {
	case 16:
		switch strings.ToLower(text) {
		case "t", "true", "1":
			return true
		case "f", "false", "0":
			return false
		}
	case 20, 21, 23:
		if value, err := strconv.ParseInt(text, 10, 64); err == nil {
			return value
		}
	case 700, 701, 1700:
		if value, err := strconv.ParseFloat(text, 64); err == nil {
			return value
		}
	}

	switch strings.ToLower(text) {
	case "t", "true":
		return true
	case "f", "false":
		return false
	default:
		return text
	}
}

func postgresDecodeBinaryBindValue(data []byte, oid int32) (interface{}, error) {
	switch oid {
	case 16:
		if len(data) != 1 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid bool parameter length")
		}
		return data[0] != 0, nil
	case 21:
		if len(data) != 2 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid int2 parameter length")
		}
		return int64(int16(binary.BigEndian.Uint16(data))), nil
	case 23:
		if len(data) != 4 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid int4 parameter length")
		}
		return int64(int32(binary.BigEndian.Uint32(data))), nil
	case 20:
		if len(data) != 8 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid int8 parameter length")
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case 700:
		if len(data) != 4 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid float4 parameter length")
		}
		bits := binary.BigEndian.Uint32(data)
		return float64FromFloat32Bits(bits), nil
	case 701:
		if len(data) != 8 {
			return nil, newPostgresProtocolError(postgresProtocolViolation, "invalid float8 parameter length")
		}
		return float64FromBits(binary.BigEndian.Uint64(data)), nil
	case 17:
		return data, nil
	default:
		return nil, newPostgresProtocolError(postgresFeatureNotSupported, fmt.Sprintf("binary parameter oid %d is not implemented", oid))
	}
}

func postgresRewriteBindPlaceholders(query string) (string, []int, int, error) {
	var builder strings.Builder
	order := make([]int, 0, 8)
	maxIndex := 0

	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			next := postgresConsumeQuoted(query, i, '\'')
			builder.WriteString(query[i:next])
			i = next
		case '"':
			next := postgresConsumeQuoted(query, i, '"')
			builder.WriteString(query[i:next])
			i = next
		case '`':
			next := postgresConsumeQuoted(query, i, '`')
			builder.WriteString(query[i:next])
			i = next
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				next := postgresConsumeLineComment(query, i)
				builder.WriteString(query[i:next])
				i = next
				continue
			}
			builder.WriteByte(query[i])
			i++
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				next := postgresConsumeBlockComment(query, i)
				builder.WriteString(query[i:next])
				i = next
				continue
			}
			builder.WriteByte(query[i])
			i++
		case '$':
			if index, next, ok := postgresConsumePlaceholder(query, i); ok {
				order = append(order, index)
				if index > maxIndex {
					maxIndex = index
				}
				builder.WriteByte('?')
				i = next
				continue
			}
			if next, ok := postgresConsumeDollarQuote(query, i); ok {
				builder.WriteString(query[i:next])
				i = next
				continue
			}
			builder.WriteByte(query[i])
			i++
		default:
			builder.WriteByte(query[i])
			i++
		}
	}

	return builder.String(), order, maxIndex, nil
}

func postgresConsumeQuoted(query string, start int, delim byte) int {
	i := start + 1
	for i < len(query) {
		if query[i] == delim {
			i++
			if delim == '\'' && i < len(query) && query[i] == '\'' {
				i++
				continue
			}
			return i
		}
		if query[i] == '\\' && delim == '\'' && i+1 < len(query) {
			i += 2
			continue
		}
		i++
	}
	return len(query)
}

func postgresConsumeLineComment(query string, start int) int {
	i := start + 2
	for i < len(query) && query[i] != '\n' {
		i++
	}
	return i
}

func postgresConsumeBlockComment(query string, start int) int {
	i := start + 2
	for i+1 < len(query) {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(query)
}

func postgresConsumePlaceholder(query string, start int) (int, int, bool) {
	i := start + 1
	if i >= len(query) || query[i] < '0' || query[i] > '9' {
		return 0, start, false
	}
	index := 0
	for i < len(query) && query[i] >= '0' && query[i] <= '9' {
		index = index*10 + int(query[i]-'0')
		i++
	}
	if index <= 0 {
		return 0, start, false
	}
	return index, i, true
}

func postgresConsumeDollarQuote(query string, start int) (int, bool) {
	i := start + 1
	for i < len(query) && ((query[i] >= 'a' && query[i] <= 'z') || (query[i] >= 'A' && query[i] <= 'Z') || (query[i] >= '0' && query[i] <= '9') || query[i] == '_') {
		i++
	}
	if i >= len(query) || query[i] != '$' {
		return 0, false
	}
	delim := query[start : i+1]
	end := strings.Index(query[i+1:], delim)
	if end < 0 {
		return len(query), true
	}
	return i + 1 + end + len(delim), true
}

func (p postgresFrontendProtocol) describePreparedStatement(c *ClientConn, statement *postgresPreparedStatement) ([]*mysql.Field, error) {
	if statement == nil {
		return nil, nil
	}
	if err := p.resolvePreparedStatementMetadata(c, statement); err != nil {
		return nil, err
	}
	if len(statement.resultFields) > 0 {
		return statement.resultFields, nil
	}

	if !statement.commandTag.includeRows {
		return nil, nil
	}

	switch statement.statement.(type) {
	case *sqlparser.Select:
	default:
		return nil, nil
	}

	defaultRule := c.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return nil, nil
	}
	defaultNode := c.proxy.GetNode(defaultRule.Nodes[0])
	if defaultNode == nil {
		return nil, nil
	}

	conn, err := c.getBackendConn(defaultNode, true)
	if err != nil {
		return nil, err
	}
	defer c.closeConn(conn, false)

	prepared, err := conn.Prepare(statement.rewrittenQuery)
	if err != nil {
		statement.resultFields = postgresResultFieldsFromStatement(statement.statement, c)
		return statement.resultFields, nil
	}
	defer prepared.Close()

	if typed, ok := prepared.(interface{ ColumnFields() []*mysql.Field }); ok {
		statement.resultFields = typed.ColumnFields()
	}
	if len(statement.resultFields) == 0 {
		statement.resultFields = postgresResultFieldsFromStatement(statement.statement, c)
	}

	return statement.resultFields, nil
}

func (p postgresFrontendProtocol) resolvePreparedStatementMetadata(c *ClientConn, statement *postgresPreparedStatement) error {
	if statement == nil {
		return nil
	}
	if postgresParamTypesResolved(statement.paramTypes) && (!statement.commandTag.includeRows || len(statement.resultFields) > 0) {
		return nil
	}
	if c.schema == nil || c.schema.rule == nil || c.schema.rule.DefaultRule == nil {
		return nil
	}
	defaultRule := c.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return nil
	}
	defaultNode := c.proxy.GetNode(defaultRule.Nodes[0])
	if defaultNode == nil {
		return nil
	}

	conn, err := c.getBackendConn(defaultNode, true)
	if err != nil {
		return err
	}
	defer c.closeConn(conn, false)

	describer, ok := conn.ManagedConn.(backend.QueryDescriber)
	if !ok {
		return nil
	}

	description, err := describer.DescribeQuery(statement.rewrittenQuery, postgresUint32OIDs(statement.paramTypes))
	if err != nil {
		return err
	}
	if description == nil {
		return nil
	}

	if len(description.ParamOIDs) > 0 {
		statement.paramTypes = postgresInt32OIDs(description.ParamOIDs)
	}
	if len(description.Fields) > 0 {
		statement.resultFields = description.Fields
	}
	if len(statement.resultFields) == 0 {
		statement.resultFields = postgresResultFieldsFromStatement(statement.statement, c)
	}

	return nil
}

func postgresResultFieldsFromStatement(stmt sqlparser.Statement, c *ClientConn) []*mysql.Field {
	if c == nil {
		return nil
	}

	switch typed := stmt.(type) {
	case *sqlparser.Select:
		resultset := c.newEmptyResultset(typed)
		return resultset.Fields
	case *sqlparser.SimpleSelect:
		resultset := c.newEmptyResultsetForSelectExprs(typed.SelectExprs)
		return resultset.Fields
	default:
		return nil
	}
}

func float64FromFloat32Bits(bits uint32) float64 {
	return float64(math.Float32frombits(bits))
}

func float64FromBits(bits uint64) float64 {
	return math.Float64frombits(bits)
}

func postgresProtocolErrorFrom(err error) *postgresProtocolError {
	if err == nil {
		return &postgresProtocolError{
			severity: postgresSeverityError,
			code:     postgresInternalError,
			message:  "unknown error",
		}
	}

	if pgErr, ok := err.(*postgresProtocolError); ok {
		return pgErr
	}

	return &postgresProtocolError{
		severity: postgresSeverityError,
		code:     postgresInternalError,
		message:  err.Error(),
	}
}

func postgresCommandTagForQuery(query string) postgresCommandTag {
	stmt, err := sqlparser.Parse(strings.TrimRight(strings.TrimSpace(query), ";"))
	if err == nil {
		switch stmt.(type) {
		case *sqlparser.Select, *sqlparser.SimpleSelect:
			return postgresCommandTag{base: "SELECT", includeRows: true}
		case *sqlparser.Insert:
			return postgresCommandTag{base: "INSERT", insert: true}
		case *sqlparser.Update:
			return postgresCommandTag{base: "UPDATE", includeRows: true}
		case *sqlparser.Delete:
			return postgresCommandTag{base: "DELETE", includeRows: true}
		case *sqlparser.Replace:
			return postgresCommandTag{base: "REPLACE", includeRows: true}
		case *sqlparser.Set:
			return postgresCommandTag{base: "SET"}
		case *sqlparser.Begin:
			return postgresCommandTag{base: "BEGIN"}
		case *sqlparser.Commit:
			return postgresCommandTag{base: "COMMIT"}
		case *sqlparser.Rollback:
			return postgresCommandTag{base: "ROLLBACK"}
		case *sqlparser.UseDB:
			return postgresCommandTag{base: "USE"}
		case *sqlparser.Truncate:
			return postgresCommandTag{base: "TRUNCATE TABLE"}
		case *sqlparser.Admin, *sqlparser.AdminHelp:
			return postgresCommandTag{base: "SELECT", includeRows: true}
		}
	}

	token := ""
	if fields := strings.Fields(strings.ToUpper(query)); len(fields) > 0 {
		token = fields[0]
	}

	switch token {
	case "SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN":
		return postgresCommandTag{base: token, includeRows: true}
	case "INSERT":
		return postgresCommandTag{base: "INSERT", insert: true}
	case "UPDATE", "DELETE", "REPLACE":
		return postgresCommandTag{base: token, includeRows: true}
	case "SET", "BEGIN", "COMMIT", "ROLLBACK", "USE", "TRUNCATE":
		return postgresCommandTag{base: token}
	default:
		return postgresCommandTag{base: "OK"}
	}
}

func postgresReadyStatusFor(c *ClientConn) byte {
	if c.status&mysql.SERVER_STATUS_IN_TRANS > 0 {
		return postgresReadyForQueryTxn
	}
	return postgresReadyForQueryIdle
}

func postgresResultRows(r *mysql.Resultset) ([][]interface{}, error) {
	if len(r.Values) > 0 {
		return r.Values, nil
	}

	rows := make([][]interface{}, len(r.RowDatas))
	for i := range r.RowDatas {
		values, err := r.RowDatas[i].Parse(r.Fields, false)
		if err != nil {
			return nil, err
		}
		rows[i] = values
	}

	return rows, nil
}

func postgresTextValue(v interface{}) ([]byte, error) {
	switch value := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	case bool:
		if value {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int:
		return []byte(strconv.Itoa(value)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(value, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(value, 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(value), 'f', -1, 32)), nil
	case float64:
		return []byte(strconv.FormatFloat(value, 'f', -1, 64)), nil
	default:
		return []byte(fmt.Sprint(value)), nil
	}
}

func postgresFieldName(field *mysql.Field) []byte {
	if field == nil {
		return []byte("?column?")
	}
	if len(field.Name) > 0 {
		return field.Name
	}
	if len(field.OrgName) > 0 {
		return field.OrgName
	}
	return []byte("?column?")
}

func postgresFieldDescription(field *mysql.Field) (uint32, uint16, uint32, int16, int32, int16) {
	if field == nil {
		return 0, 0, 25, -1, -1, 0
	}

	if field.PostgresTypeOID != 0 {
		return field.PostgresTableOID,
			field.PostgresTableAttributeNumber,
			field.PostgresTypeOID,
			field.PostgresTypeSize,
			field.PostgresTypeModifier,
			field.PostgresFormat
	}

	switch field.Type {
	case mysql.MYSQL_TYPE_TINY:
		return 0, 0, 21, 2, -1, 0
	case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
		return 0, 0, 21, 2, -1, 0
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		return 0, 0, 23, 4, -1, 0
	case mysql.MYSQL_TYPE_LONGLONG:
		return 0, 0, 20, 8, -1, 0
	case mysql.MYSQL_TYPE_FLOAT:
		return 0, 0, 700, 4, -1, 0
	case mysql.MYSQL_TYPE_DOUBLE:
		return 0, 0, 701, 8, -1, 0
	case mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_DECIMAL:
		return 0, 0, 1700, -1, -1, 0
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return 0, 0, 1082, 4, -1, 0
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME:
		return 0, 0, 1114, 8, -1, 0
	case mysql.MYSQL_TYPE_TIME:
		return 0, 0, 1083, 8, -1, 0
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
		return 0, 0, 17, -1, -1, 0
	case mysql.MYSQL_TYPE_BIT:
		return 0, 0, 1560, -1, -1, 0
	default:
		return 0, 0, 25, -1, -1, 0
	}
}

func postgresAppendInt16(dst []byte, value int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(value))
	return append(dst, buf...)
}

func postgresAppendUint16(dst []byte, value uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, value)
	return append(dst, buf...)
}

func postgresAppendInt32(dst []byte, value int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return append(dst, buf...)
}

func postgresAppendUint32(dst []byte, value uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	return append(dst, buf...)
}

func postgresUint32OIDs(oids []int32) []uint32 {
	result := make([]uint32, len(oids))
	for i, oid := range oids {
		result[i] = uint32(oid)
	}
	return result
}

func postgresInt32OIDs(oids []uint32) []int32 {
	result := make([]int32, len(oids))
	for i, oid := range oids {
		result[i] = int32(oid)
	}
	return result
}

func postgresParamTypesResolved(oids []int32) bool {
	for _, oid := range oids {
		if oid == 0 {
			return false
		}
	}
	return true
}

func postgresFrontendStateFor(c *ClientConn) *postgresFrontendState {
	state, ok := c.frontendState.(*postgresFrontendState)
	if !ok || state == nil {
		panic("postgres frontend state is not initialized")
	}
	return state
}
