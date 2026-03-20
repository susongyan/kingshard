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

var _ Driver = mysqlDriver{}
var _ ManagedConn = (*Conn)(nil)

type mysqlDriver struct{}

func (d mysqlDriver) Name() string {
	return DefaultBackendType
}

func (d mysqlDriver) Open(addr string, user string, password string, db string) (ManagedConn, error) {
	conn := new(Conn)
	if err := conn.Connect(addr, user, password, db); err != nil {
		return nil, err
	}
	return conn, nil
}

func init() {
	RegisterDriver(mysqlDriver{})
}
