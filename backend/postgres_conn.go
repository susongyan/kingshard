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

package backend

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"

	"github.com/flike/kingshard/mysql"
)

var _ ManagedConn = (*postgresConn)(nil)
var _ QueryDescriber = (*postgresConn)(nil)
var _ PreparedStatement = (*postgresStmt)(nil)

type postgresConn struct {
	mu sync.Mutex

	db *sql.DB
	tx *sql.Tx

	metadataConn *pgconn.PgConn

	addr     string
	user     string
	password string
	database string

	charset   string
	collation mysql.CollationId

	autoCommit bool

	pushTimestamp int64
	lastErr       error

	stmtID   uint32
	prepared map[uint32]*postgresStmt
}

type postgresStmt struct {
	conn *postgresConn
	stmt *sql.Stmt

	id    uint32
	query string

	params  int
	columns int

	paramOIDs    []uint32
	columnFields []*mysql.Field
}

func newPostgresConn(addr string, user string, password string, db string) (*postgresConn, error) {
	conn := &postgresConn{
		addr:       addr,
		user:       user,
		password:   password,
		database:   db,
		charset:    mysql.DEFAULT_CHARSET,
		collation:  mysql.DEFAULT_COLLATION_ID,
		autoCommit: true,
		prepared:   make(map[uint32]*postgresStmt),
	}
	if conn.database == "" {
		conn.database = "postgres"
	}

	dsn := postgresBuildDSN(addr, user, password, conn.database)
	dbh, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	dbh.SetMaxOpenConns(1)
	dbh.SetMaxIdleConns(1)

	conn.db = dbh
	if err := conn.Ping(); err != nil {
		dbh.Close()
		return nil, err
	}
	return conn, nil
}

func (c *postgresConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, stmt := range c.prepared {
		if stmt != nil && stmt.stmt != nil {
			_ = stmt.stmt.Close()
		}
		delete(c.prepared, id)
	}
	if c.tx != nil {
		_ = c.tx.Rollback()
		c.tx = nil
	}
	if c.metadataConn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = c.metadataConn.Close(ctx)
		cancel()
		c.metadataConn = nil
	}
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		c.lastErr = err
		return err
	}
	return nil
}

func (c *postgresConn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.tx != nil {
		_, err = c.tx.ExecContext(ctx, "select 1")
	} else {
		err = c.db.PingContext(ctx)
	}
	c.lastErr = err
	return err
}

func (c *postgresConn) GetAddr() string {
	return c.addr
}

func (c *postgresConn) GetDB() string {
	return c.database
}

func (c *postgresConn) Execute(command string, args ...interface{}) (*mysql.Result, error) {
	query := command
	if len(args) > 0 {
		var err error
		query, _, err = postgresRewriteQuestionPlaceholders(command)
		if err != nil {
			c.lastErr = err
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.autoCommit && c.tx == nil {
		if err := c.beginTxLocked(); err != nil {
			c.lastErr = err
			return nil, err
		}
	}

	if postgresStatementReturnsRows(query) {
		result, err := c.queryLocked(query, args...)
		c.lastErr = err
		return result, err
	}

	result, err := c.execLocked(query, args...)
	c.lastErr = err
	return result, err
}

func (c *postgresConn) Begin() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tx != nil {
		return nil
	}
	err := c.beginTxLocked()
	c.lastErr = err
	return err
}

func (c *postgresConn) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tx == nil {
		return nil
	}
	err := c.tx.Commit()
	c.tx = nil
	c.lastErr = err
	return err
}

func (c *postgresConn) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tx == nil {
		return nil
	}
	err := c.tx.Rollback()
	c.tx = nil
	c.lastErr = err
	return err
}

func (c *postgresConn) SetAutoCommit(n uint8) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch n {
	case 0:
		c.autoCommit = false
		c.lastErr = nil
		return nil
	case 1:
		if c.tx != nil {
			if err := c.tx.Commit(); err != nil {
				c.tx = nil
				c.lastErr = err
				return err
			}
			c.tx = nil
		}
		c.autoCommit = true
		c.lastErr = nil
		return nil
	default:
		err := fmt.Errorf("invalid autocommit value %d", n)
		c.lastErr = err
		return err
	}
}

func (c *postgresConn) IsAutoCommit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.autoCommit
}

func (c *postgresConn) IsInTransaction() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tx != nil
}

func (c *postgresConn) SetCharset(charset string, collation mysql.CollationId) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if charset == "" {
		charset = mysql.DEFAULT_CHARSET
	}
	c.charset = charset
	if collation == 0 {
		c.collation = mysql.DEFAULT_COLLATION_ID
	} else {
		c.collation = collation
	}
	c.lastErr = nil
	return nil
}

func (c *postgresConn) GetCharset() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.charset
}

func (c *postgresConn) UseDB(dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if dbName == "" || dbName == c.database {
		c.lastErr = nil
		return nil
	}
	if c.tx != nil {
		err := fmt.Errorf("cannot switch postgres database from %q to %q during an active transaction", c.database, dbName)
		c.lastErr = err
		return err
	}

	err := c.reconnectLocked(dbName)
	c.lastErr = err
	return err
}

func (c *postgresConn) FieldList(table string, wildcard string) ([]*mysql.Field, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	query := `
select column_name, data_type
from information_schema.columns
where table_schema = current_schema()
  and table_name = $1
  and column_name like $2
order by ordinal_position`
	rows, err := c.queryRowsLocked(query, table, wildcardOrPercent(wildcard))
	if err != nil {
		c.lastErr = err
		return nil, err
	}
	defer rows.Close()

	fields := make([]*mysql.Field, 0, 8)
	for rows.Next() {
		var name string
		var dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			c.lastErr = err
			return nil, err
		}

		field := &mysql.Field{Name: []byte(name)}
		postgresApplyTypeName(field, dataType)
		fields = append(fields, field)
	}
	if err := rows.Err(); err != nil {
		c.lastErr = err
		return nil, err
	}

	c.lastErr = nil
	return fields, nil
}

func (c *postgresConn) Prepare(query string) (PreparedStatement, error) {
	rewritten, params, err := postgresRewriteQuestionPlaceholders(query)
	if err != nil {
		c.lastErr = err
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.autoCommit && c.tx == nil {
		if err := c.beginTxLocked(); err != nil {
			c.lastErr = err
			return nil, err
		}
	}

	var stmt *sql.Stmt
	if c.tx != nil {
		stmt, err = c.tx.Prepare(rewritten)
	} else {
		stmt, err = c.db.Prepare(rewritten)
	}
	if err != nil {
		c.lastErr = err
		return nil, err
	}

	id := atomic.AddUint32(&c.stmtID, 1)
	ps := &postgresStmt{
		conn:   c,
		stmt:   stmt,
		id:     id,
		query:  rewritten,
		params: params,
	}
	if description, err := c.describeQueryLocked(rewritten, nil); err == nil && description != nil {
		if len(description.ParamOIDs) > 0 {
			ps.paramOIDs = append([]uint32(nil), description.ParamOIDs...)
			ps.params = len(description.ParamOIDs)
		}
		if len(description.Fields) > 0 {
			ps.columnFields = description.Fields
			ps.columns = len(description.Fields)
		}
	}
	c.prepared[id] = ps
	c.lastErr = nil
	return ps, nil
}

func (c *postgresConn) ClosePrepare(id uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stmt := c.prepared[id]
	if stmt == nil {
		c.lastErr = nil
		return nil
	}
	delete(c.prepared, id)
	err := stmt.stmt.Close()
	c.lastErr = err
	return err
}

func (c *postgresConn) SessionState() SessionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return SessionState{
		Database:      c.database,
		Charset:       c.charset,
		Collation:     c.collation,
		AutoCommit:    c.autoCommit,
		InTransaction: c.tx != nil,
	}
}

func (c *postgresConn) SyncSession(state SessionState) error {
	if err := c.UseDB(state.Database); err != nil {
		return err
	}

	if err := c.SetCharset(state.Charset, state.Collation); err != nil {
		return err
	}

	if state.AutoCommit {
		return c.SetAutoCommit(1)
	}
	return c.SetAutoCommit(0)
}

func (c *postgresConn) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

func (c *postgresConn) PushTimestamp() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pushTimestamp
}

func (c *postgresConn) SetPushTimestamp(ts int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushTimestamp = ts
}

func (s *postgresStmt) ParamNum() int {
	return s.params
}

func (s *postgresStmt) ColumnNum() int {
	return s.columns
}

func (s *postgresStmt) GetId() uint32 {
	return s.id
}

func (s *postgresStmt) Execute(args ...interface{}) (*mysql.Result, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if !s.conn.autoCommit && s.conn.tx == nil {
		if err := s.conn.beginTxLocked(); err != nil {
			s.conn.lastErr = err
			return nil, err
		}
	}

	var (
		result *mysql.Result
		err    error
	)
	if postgresStatementReturnsRows(s.query) {
		result, err = s.queryLocked(args...)
	} else {
		result, err = s.execLocked(args...)
	}
	s.conn.lastErr = err
	return result, err
}

func (s *postgresStmt) Close() error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	delete(s.conn.prepared, s.id)
	err := s.stmt.Close()
	s.conn.lastErr = err
	return err
}

func (s *postgresStmt) ColumnFields() []*mysql.Field {
	return s.columnFields
}

func (s *postgresStmt) ParamOIDs() []uint32 {
	return append([]uint32(nil), s.paramOIDs...)
}

func (c *postgresConn) beginTxLocked() error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	c.tx = tx
	return nil
}

func (c *postgresConn) reconnectLocked(dbName string) error {
	dsn := postgresBuildDSN(c.addr, c.user, c.password, dbName)
	dbh, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	dbh.SetMaxOpenConns(1)
	dbh.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dbh.PingContext(ctx); err != nil {
		dbh.Close()
		return err
	}

	for id, stmt := range c.prepared {
		if stmt != nil && stmt.stmt != nil {
			_ = stmt.stmt.Close()
		}
		delete(c.prepared, id)
	}
	if c.metadataConn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = c.metadataConn.Close(ctx)
		cancel()
		c.metadataConn = nil
	}
	if c.db != nil {
		_ = c.db.Close()
	}

	c.db = dbh
	c.database = dbName
	c.tx = nil
	return nil
}

func (c *postgresConn) DescribeQuery(query string, paramOIDs []uint32) (*QueryDescription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	description, err := c.describeQueryLocked(query, paramOIDs)
	c.lastErr = err
	return description, err
}

func (c *postgresConn) describeQueryLocked(query string, paramOIDs []uint32) (*QueryDescription, error) {
	metadataConn, err := c.ensureMetadataConnLocked()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	statement, err := metadataConn.Prepare(ctx, "", query, paramOIDs)
	if err != nil {
		c.closeMetadataConnLocked()
		return nil, err
	}

	return &QueryDescription{
		ParamOIDs: append([]uint32(nil), statement.ParamOIDs...),
		Fields:    postgresFieldsFromNativeDescriptions(statement.Fields),
	}, nil
}

func (c *postgresConn) ensureMetadataConnLocked() (*pgconn.PgConn, error) {
	if c.metadataConn != nil && !c.metadataConn.IsClosed() {
		return c.metadataConn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metadataConn, err := pgconn.Connect(ctx, postgresBuildDSN(c.addr, c.user, c.password, c.database))
	if err != nil {
		return nil, err
	}

	c.metadataConn = metadataConn
	return c.metadataConn, nil
}

func (c *postgresConn) closeMetadataConnLocked() {
	if c.metadataConn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = c.metadataConn.Close(ctx)
	cancel()
	c.metadataConn = nil
}

func (c *postgresConn) execLocked(query string, args ...interface{}) (*mysql.Result, error) {
	var (
		res sql.Result
		err error
	)

	if c.tx != nil {
		res, err = c.tx.Exec(query, args...)
	} else {
		res, err = c.db.Exec(query, args...)
	}
	if err != nil {
		return nil, err
	}

	affected, _ := res.RowsAffected()
	return &mysql.Result{
		Status:       c.mysqlStatusLocked(),
		AffectedRows: uint64(affected),
	}, nil
}

func (c *postgresConn) queryLocked(query string, args ...interface{}) (*mysql.Result, error) {
	var describedFields []*mysql.Field
	if len(args) == 0 {
		if description, err := c.describeQueryLocked(query, nil); err == nil && description != nil {
			describedFields = description.Fields
		}
	}

	rows, err := c.queryRowsLocked(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return postgresResultFromRows(rows, c.mysqlStatusLocked(), describedFields)
}

func (c *postgresConn) queryRowsLocked(query string, args ...interface{}) (*sql.Rows, error) {
	if c.tx != nil {
		return c.tx.Query(query, args...)
	}
	return c.db.Query(query, args...)
}

func (s *postgresStmt) execLocked(args ...interface{}) (*mysql.Result, error) {
	res, err := s.stmt.Exec(args...)
	if err != nil {
		return nil, err
	}
	affected, _ := res.RowsAffected()
	return &mysql.Result{
		Status:       s.conn.mysqlStatusLocked(),
		AffectedRows: uint64(affected),
	}, nil
}

func (s *postgresStmt) queryLocked(args ...interface{}) (*mysql.Result, error) {
	rows, err := s.stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := postgresResultFromRows(rows, s.conn.mysqlStatusLocked(), s.columnFields)
	if err == nil && result != nil && result.Resultset != nil {
		s.columnFields = result.Fields
		s.columns = len(result.Fields)
	}
	return result, err
}

func (c *postgresConn) mysqlStatusLocked() uint16 {
	status := uint16(0)
	if c.autoCommit {
		status |= mysql.SERVER_STATUS_AUTOCOMMIT
	}
	if c.tx != nil {
		status |= mysql.SERVER_STATUS_IN_TRANS
	}
	return status
}

func postgresBuildDSN(addr, user, password, db string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   addr,
		Path:   "/" + db,
	}
	values := url.Values{}
	values.Set("sslmode", "disable")
	u.RawQuery = values.Encode()
	return u.String()
}

func postgresStatementReturnsRows(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(lower, "select"),
		strings.HasPrefix(lower, "show"),
		strings.HasPrefix(lower, "with"),
		strings.HasPrefix(lower, "explain"),
		strings.HasPrefix(lower, "values"):
		return true
	case strings.Contains(lower, " returning "):
		return true
	default:
		return false
	}
}

func postgresResultFromRows(rows *sql.Rows, status uint16, describedFields []*mysql.Field) (*mysql.Result, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := postgresFieldsFromColumnTypes(columnTypes)
	if len(describedFields) == len(fields) && len(describedFields) > 0 {
		fields = describedFields
	}
	names := make(map[string]int, len(fields))
	for i := range fields {
		names[string(fields[i].Name)] = i
	}

	resultset := &mysql.Resultset{
		Fields:     fields,
		FieldNames: names,
		Values:     make([][]interface{}, 0, 16),
		RowDatas:   make([]mysql.RowData, 0, 16),
	}

	raw := make([]interface{}, len(fields))
	dest := make([]interface{}, len(fields))
	for i := range raw {
		dest[i] = &raw[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		values := make([]interface{}, len(fields))
		for i := range raw {
			values[i] = postgresNormalizeValue(raw[i])
		}
		rowData, err := postgresEncodeRow(values)
		if err != nil {
			return nil, err
		}

		resultset.Values = append(resultset.Values, values)
		resultset.RowDatas = append(resultset.RowDatas, rowData)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &mysql.Result{
		Status:    status,
		Resultset: resultset,
	}, nil
}

func postgresFieldsFromColumnTypes(columnTypes []*sql.ColumnType) []*mysql.Field {
	fields := make([]*mysql.Field, len(columnTypes))
	for i, ct := range columnTypes {
		fields[i] = postgresFieldFromColumnType(ct)
	}
	return fields
}

func postgresFieldsFromNativeDescriptions(descriptions []pgproto3.FieldDescription) []*mysql.Field {
	fields := make([]*mysql.Field, len(descriptions))
	for i, description := range descriptions {
		fields[i] = postgresFieldFromNativeDescription(description)
	}
	return fields
}

func postgresFieldFromColumnType(ct *sql.ColumnType) *mysql.Field {
	field := &mysql.Field{
		Name:         []byte(ct.Name()),
		Charset:      33,
		ColumnLength: 1024,
		Type:         mysql.MYSQL_TYPE_VAR_STRING,
	}

	if nullable, ok := ct.Nullable(); ok && !nullable {
		field.Flag |= mysql.NOT_NULL_FLAG
	}
	if length, ok := ct.Length(); ok {
		field.ColumnLength = uint32(length)
	}

	postgresApplyTypeName(field, ct.DatabaseTypeName())
	return field
}

func postgresFieldFromNativeDescription(description pgproto3.FieldDescription) *mysql.Field {
	field := &mysql.Field{
		Name:                         append([]byte(nil), description.Name...),
		Charset:                      33,
		ColumnLength:                 1024,
		Type:                         mysql.MYSQL_TYPE_VAR_STRING,
		PostgresTableOID:             description.TableOID,
		PostgresTableAttributeNumber: description.TableAttributeNumber,
		PostgresTypeOID:              description.DataTypeOID,
		PostgresTypeSize:             description.DataTypeSize,
		PostgresTypeModifier:         description.TypeModifier,
		PostgresFormat:               description.Format,
	}

	postgresApplyTypeOID(field, description.DataTypeOID)
	return field
}

func postgresApplyTypeName(field *mysql.Field, typeName string) {
	switch strings.ToUpper(typeName) {
	case "BOOL", "BOOLEAN":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_TINY
		field.Flag |= mysql.BINARY_FLAG
	case "INT2", "SMALLINT":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_SHORT
		field.Flag |= mysql.BINARY_FLAG
	case "INT4", "INTEGER", "SERIAL":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONG
		field.Flag |= mysql.BINARY_FLAG
	case "INT8", "BIGINT", "BIGSERIAL":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag |= mysql.BINARY_FLAG
	case "FLOAT4", "REAL":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_FLOAT
		field.Flag |= mysql.BINARY_FLAG
	case "FLOAT8", "DOUBLE PRECISION":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_DOUBLE
		field.Flag |= mysql.BINARY_FLAG
	case "NUMERIC", "DECIMAL":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_NEWDECIMAL
		field.Flag |= mysql.BINARY_FLAG
	case "DATE":
		field.Type = mysql.MYSQL_TYPE_DATE
	case "TIME", "TIMETZ":
		field.Type = mysql.MYSQL_TYPE_TIME
	case "TIMESTAMP", "TIMESTAMPTZ":
		field.Type = mysql.MYSQL_TYPE_TIMESTAMP
	case "BYTEA":
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_BLOB
		field.Flag |= mysql.BINARY_FLAG
	default:
		field.Type = mysql.MYSQL_TYPE_VAR_STRING
	}
}

func postgresApplyTypeOID(field *mysql.Field, oid uint32) {
	switch oid {
	case 16:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_TINY
		field.Flag |= mysql.BINARY_FLAG
	case 20:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag |= mysql.BINARY_FLAG
	case 21:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_SHORT
		field.Flag |= mysql.BINARY_FLAG
	case 23:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONG
		field.Flag |= mysql.BINARY_FLAG
	case 700:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_FLOAT
		field.Flag |= mysql.BINARY_FLAG
	case 701:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_DOUBLE
		field.Flag |= mysql.BINARY_FLAG
	case 1700:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_NEWDECIMAL
		field.Flag |= mysql.BINARY_FLAG
	case 1082:
		field.Type = mysql.MYSQL_TYPE_DATE
	case 1083, 1266:
		field.Type = mysql.MYSQL_TYPE_TIME
	case 1114, 1184:
		field.Type = mysql.MYSQL_TYPE_TIMESTAMP
	case 17:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_BLOB
		field.Flag |= mysql.BINARY_FLAG
	default:
		field.Type = mysql.MYSQL_TYPE_VAR_STRING
	}
}

func postgresNormalizeValue(v interface{}) interface{} {
	switch value := v.(type) {
	case nil:
		return nil
	case bool:
		if value {
			return int64(1)
		}
		return int64(0)
	case int64, float64, []byte, string:
		return value
	case time.Time:
		return value.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprint(value)
	}
}

func postgresEncodeRow(values []interface{}) (mysql.RowData, error) {
	row := make([]byte, 0, 128)
	for _, value := range values {
		if value == nil {
			row = append(row, 0xfb)
			continue
		}

		b, err := postgresTextBytes(value)
		if err != nil {
			return nil, err
		}
		row = append(row, mysql.PutLengthEncodedString(b)...)
	}
	return mysql.RowData(row), nil
}

func postgresTextBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64), nil
	default:
		return nil, fmt.Errorf("unsupported postgres row value type %T", value)
	}
}

func postgresRewriteQuestionPlaceholders(query string) (string, int, error) {
	var builder strings.Builder
	count := 0

	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			next := postgresScanQuoted(query, i, '\'')
			builder.WriteString(query[i:next])
			i = next
		case '"':
			next := postgresScanQuoted(query, i, '"')
			builder.WriteString(query[i:next])
			i = next
		case '`':
			next := postgresScanQuoted(query, i, '`')
			builder.WriteString(query[i:next])
			i = next
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				next := postgresScanLineComment(query, i)
				builder.WriteString(query[i:next])
				i = next
				continue
			}
			builder.WriteByte(query[i])
			i++
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				next := postgresScanBlockComment(query, i)
				builder.WriteString(query[i:next])
				i = next
				continue
			}
			builder.WriteByte(query[i])
			i++
		case '?':
			count++
			builder.WriteByte('$')
			builder.WriteString(strconv.Itoa(count))
			i++
		case '$':
			if next, ok := postgresScanDollarQuote(query, i); ok {
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

	return builder.String(), count, nil
}

func postgresScanQuoted(query string, start int, delim byte) int {
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
		if delim == '\'' && query[i] == '\\' && i+1 < len(query) {
			i += 2
			continue
		}
		i++
	}
	return len(query)
}

func postgresScanLineComment(query string, start int) int {
	i := start + 2
	for i < len(query) && query[i] != '\n' {
		i++
	}
	return i
}

func postgresScanBlockComment(query string, start int) int {
	i := start + 2
	for i+1 < len(query) {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(query)
}

func postgresScanDollarQuote(query string, start int) (int, bool) {
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

func wildcardOrPercent(wildcard string) string {
	if wildcard == "" {
		return "%"
	}
	return wildcard
}
