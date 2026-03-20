package server

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/config"
	"github.com/jackc/pgconn"
	_ "github.com/lib/pq"
)

func requirePostgresFrontendIntegrationConfig(t *testing.T) *pgconn.Config {
	t.Helper()

	for _, key := range []string{"PG_INTEGRATION_DSN", "POSTGRES_DSN", "PGX_TEST_CONN_STRING"} {
		if dsn := strings.TrimSpace(os.Getenv(key)); dsn != "" {
			cfg, err := pgconn.ParseConfig(dsn)
			if err != nil {
				t.Fatalf("parse %s: %v", key, err)
			}
			return cfg
		}
	}

	t.Skip("set PG_INTEGRATION_DSN to run PostgreSQL frontend integration tests")
	return nil
}

func newPostgresFrontendIntegrationDB(t *testing.T) (*pgconn.Config, *sql.DB) {
	t.Helper()

	cfg := requirePostgresFrontendIntegrationConfig(t)
	db, err := sql.Open("postgres", postgresIntegrationDSN(cfg))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("db.Ping: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return cfg, db
}

func postgresIntegrationDSN(cfg *pgconn.Config) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port)),
		Path:   "/" + cfg.Database,
	}

	values := url.Values{}
	values.Set("sslmode", "disable")
	u.RawQuery = values.Encode()
	return u.String()
}

func newPostgresFrontendIntegrationTable(t *testing.T, db *sql.DB) string {
	t.Helper()

	tableName := fmt.Sprintf("ks_pg_proxy_int_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf("drop table if exists %s", tableName)); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`create table %s (
id integer primary key,
note text not null,
amount numeric(10,2),
active boolean not null
)`, tableName)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("insert into %s (id, note, amount, active) values (1, 'alpha', 12.34, true)", tableName)); err != nil {
		t.Fatalf("seed table: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf("drop table if exists %s", tableName))
	})

	return tableName
}

func newPostgresFrontendIntegrationServer(t *testing.T, backendCfg *pgconn.Config) *Server {
	t.Helper()

	const proxyUser = "pgproxy"
	const proxyPassword = "pgproxy_pass"

	addr := net.JoinHostPort(backendCfg.Host, fmt.Sprintf("%d", backendCfg.Port))
	cfg := &config.Config{
		Addr:         "127.0.0.1:0",
		FrontendType: PostgresFrontendType,
		LogSql:       "off",
		SlowLogTime:  1000,
		Charset:      "utf8",
		UserList: []config.UserConfig{
			{User: proxyUser, Password: proxyPassword},
		},
		Nodes: []config.NodeConfig{
			{
				Name:        "node1",
				BackendType: backend.PostgresBackendType,
				Database:    backendCfg.Database,
				User:        backendCfg.User,
				Password:    backendCfg.Password,
				Master:      addr,
				MaxConnNum:  8,
			},
		},
		SchemaList: []config.SchemaConfig{
			{
				User:    proxyUser,
				Nodes:   []string{"node1"},
				Default: "node1",
			},
		},
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	go func() {
		_ = server.Run()
	}()

	t.Cleanup(func() {
		for _, node := range server.nodes {
			node.Online = false
		}
		server.Close()
	})

	return server
}

func connectPostgresFrontendIntegrationClient(t *testing.T, server *Server, database string) *pgconn.PgConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := pgconn.ParseConfig(fmt.Sprintf("postgres://pgproxy:pgproxy_pass@%s/%s?sslmode=disable", server.listener.Addr().String(), database))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	var conn *pgconn.PgConn
	for {
		conn, err = pgconn.ConnectConfig(ctx, cfg)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("ConnectConfig: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Cleanup(func() {
		_ = conn.Close(context.Background())
	})

	return conn
}

func TestPostgresFrontendPrepareDescribeExecuteIntegration(t *testing.T) {
	backendCfg, db := newPostgresFrontendIntegrationDB(t)
	tableName := newPostgresFrontendIntegrationTable(t, db)
	server := newPostgresFrontendIntegrationServer(t, backendCfg)
	client := connectPostgresFrontendIntegrationClient(t, server, backendCfg.Database)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := fmt.Sprintf("select id, note, amount, active from %s where id = $1", tableName)
	description, err := client.Prepare(ctx, "ks_proxy_ps", query, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(description.ParamOIDs) != 1 || description.ParamOIDs[0] != 23 {
		t.Fatalf("ParamOIDs = %v, want [23]", description.ParamOIDs)
	}
	if len(description.Fields) != 4 {
		t.Fatalf("len(Fields) = %d, want 4", len(description.Fields))
	}

	wantOIDs := []uint32{23, 25, 1700, 16}
	wantAttrs := []uint16{1, 2, 3, 4}
	for i, field := range description.Fields {
		if field.DataTypeOID != wantOIDs[i] {
			t.Fatalf("field %d oid = %d, want %d", i, field.DataTypeOID, wantOIDs[i])
		}
		if field.TableOID == 0 {
			t.Fatalf("field %d table oid = 0, want non-zero", i)
		}
		if field.TableAttributeNumber != wantAttrs[i] {
			t.Fatalf("field %d attr = %d, want %d", i, field.TableAttributeNumber, wantAttrs[i])
		}
	}

	result := client.ExecPrepared(ctx, "ks_proxy_ps", [][]byte{[]byte("1")}, []int16{0}, []int16{0}).Read()
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if len(result.FieldDescriptions) != 4 {
		t.Fatalf("len(FieldDescriptions) = %d, want 4", len(result.FieldDescriptions))
	}
	if len(result.Rows) != 1 {
		t.Fatalf("len(Rows) = %d, want 1", len(result.Rows))
	}

	for i, field := range result.FieldDescriptions {
		if field.DataTypeOID != wantOIDs[i] {
			t.Fatalf("result field %d oid = %d, want %d", i, field.DataTypeOID, wantOIDs[i])
		}
	}
	row := result.Rows[0]
	if got := string(row[0]); got != "1" {
		t.Fatalf("row[0] = %q, want %q", got, "1")
	}
	if got := string(row[1]); got != "alpha" {
		t.Fatalf("row[1] = %q, want %q", got, "alpha")
	}
	if got := string(row[2]); got != "12.34" {
		t.Fatalf("row[2] = %q, want %q", got, "12.34")
	}
	if got := string(row[3]); got != "t" {
		t.Fatalf("row[3] = %q, want %q", got, "t")
	}
}

func TestPostgresFrontendPrepareTypedExpressionsIntegration(t *testing.T) {
	backendCfg, _ := newPostgresFrontendIntegrationDB(t)
	server := newPostgresFrontendIntegrationServer(t, backendCfg)
	client := connectPostgresFrontendIntegrationClient(t, server, backendCfg.Database)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	description, err := client.Prepare(ctx, "ks_proxy_expr", "select $1::int4 as id, $2::text as note, $3::numeric(10,2) as amount, $4::bool as active", nil)
	if err != nil {
		t.Fatal(err)
	}

	wantOIDs := []uint32{23, 25, 1700, 16}
	if len(description.ParamOIDs) != len(wantOIDs) {
		t.Fatalf("len(ParamOIDs) = %d, want %d", len(description.ParamOIDs), len(wantOIDs))
	}
	for i, want := range wantOIDs {
		if description.ParamOIDs[i] != want {
			t.Fatalf("param %d oid = %d, want %d", i, description.ParamOIDs[i], want)
		}
		if description.Fields[i].DataTypeOID != want {
			t.Fatalf("field %d oid = %d, want %d", i, description.Fields[i].DataTypeOID, want)
		}
		if description.Fields[i].TableOID != 0 {
			t.Fatalf("field %d table oid = %d, want 0", i, description.Fields[i].TableOID)
		}
	}

	result := client.ExecPrepared(
		ctx,
		"ks_proxy_expr",
		[][]byte{[]byte("1"), []byte("beta"), []byte("45.67"), []byte("t")},
		[]int16{0, 0, 0, 0},
		[]int16{0},
	).Read()
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if got := string(result.Rows[0][1]); got != "beta" {
		t.Fatalf("row[1] = %q, want %q", got, "beta")
	}
}
