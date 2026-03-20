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
	"fmt"
	"strings"
	"sync"

	"github.com/flike/kingshard/mysql"
)

const (
	DefaultBackendType  = "mysql"
	PostgresBackendType = "postgres"
)

// SessionState is a protocol-neutral skeleton for session synchronization.
// The current MySQL path still consumes mysql.Result/mysql.Field, but the pool
// and driver entry points no longer need to assume a concrete wire protocol.
type SessionState struct {
	Database      string
	Charset       string
	Collation     mysql.CollationId
	AutoCommit    bool
	InTransaction bool
}

// BackendField is a minimal backend-agnostic column description that future
// protocol adapters can map to their own metadata.
type BackendField struct {
	Name string
	Raw  interface{}
}

// BackendResult is a minimal backend-agnostic execution result skeleton.
type BackendResult struct {
	AffectedRows uint64
	InsertID     uint64
	Status       uint16
	Fields       []*BackendField
	Values       [][]interface{}
	Raw          interface{}
}

func NewBackendResult(r *mysql.Result) *BackendResult {
	if r == nil {
		return nil
	}

	result := &BackendResult{
		AffectedRows: r.AffectedRows,
		InsertID:     r.InsertId,
		Status:       r.Status,
		Raw:          r,
	}

	if len(r.Fields) > 0 {
		result.Fields = make([]*BackendField, len(r.Fields))
		for i, field := range r.Fields {
			result.Fields[i] = &BackendField{
				Name: string(field.Name),
				Raw:  field,
			}
		}
	}

	if len(r.Values) > 0 {
		result.Values = r.Values
	}

	return result
}

type PreparedStatement interface {
	ParamNum() int
	ColumnNum() int
	GetId() uint32
	Execute(args ...interface{}) (*mysql.Result, error)
	Close() error
}

// ManagedConn is the backend pool contract. This is the first decoupling seam:
// DB/Node now depend on this interface instead of the concrete MySQL Conn.
type ManagedConn interface {
	Close() error
	Ping() error
	GetAddr() string
	GetDB() string
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Begin() error
	Commit() error
	Rollback() error
	SetAutoCommit(n uint8) error
	IsAutoCommit() bool
	IsInTransaction() bool
	SetCharset(charset string, collation mysql.CollationId) error
	GetCharset() string
	UseDB(dbName string) error
	FieldList(table string, wildcard string) ([]*mysql.Field, error)
	Prepare(query string) (PreparedStatement, error)
	ClosePrepare(id uint32) error
	SessionState() SessionState
	SyncSession(SessionState) error
	LastError() error
	PushTimestamp() int64
	SetPushTimestamp(ts int64)
}

type Driver interface {
	Name() string
	Open(addr string, user string, password string, db string) (ManagedConn, error)
}

var (
	driverMu sync.RWMutex
	drivers  = make(map[string]Driver)
)

func RegisterDriver(driver Driver) {
	if driver == nil {
		panic("backend: nil driver")
	}

	name := NormalizeBackendType(driver.Name())
	if name == "" {
		panic("backend: empty driver name")
	}

	driverMu.Lock()
	defer driverMu.Unlock()
	drivers[name] = driver
}

func GetDriver(name string) (Driver, error) {
	driverMu.RLock()
	defer driverMu.RUnlock()

	driver := drivers[NormalizeBackendType(name)]
	if driver == nil {
		return nil, fmt.Errorf("backend driver %q is not registered", name)
	}

	return driver, nil
}

func NormalizeBackendType(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return DefaultBackendType
	}
	return name
}
