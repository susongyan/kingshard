package backend

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
)

func requirePostgresIntegrationConfig(t *testing.T) *pgconn.Config {
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

	t.Skip("set PG_INTEGRATION_DSN to run PostgreSQL integration tests")
	return nil
}

func newPostgresIntegrationConn(t *testing.T) *postgresConn {
	t.Helper()

	cfg := requirePostgresIntegrationConfig(t)
	conn, err := newPostgresConn(net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port)), cfg.User, cfg.Password, cfg.Database)
	if err != nil {
		t.Fatalf("newPostgresConn: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

func newPostgresIntegrationTable(t *testing.T, conn *postgresConn) string {
	t.Helper()

	tableName := fmt.Sprintf("ks_pg_int_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`create table %s (
id integer primary key,
note text not null,
amount numeric(10,2),
active boolean not null
)`, tableName)
	if _, err := conn.Execute(fmt.Sprintf("drop table if exists %s", tableName)); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := conn.Execute(createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Execute(fmt.Sprintf("insert into %s (id, note, amount, active) values (1, 'alpha', 12.34, true)", tableName)); err != nil {
		t.Fatalf("seed table: %v", err)
	}

	t.Cleanup(func() {
		_, _ = conn.Execute(fmt.Sprintf("drop table if exists %s", tableName))
	})

	return tableName
}

func TestPostgresConnDescribeQueryIntegration(t *testing.T) {
	conn := newPostgresIntegrationConn(t)
	tableName := newPostgresIntegrationTable(t, conn)

	description, err := conn.DescribeQuery(
		fmt.Sprintf("select id, note, amount, active from %s where id = $1", tableName),
		nil,
	)
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
		if field.PostgresTypeOID != wantOIDs[i] {
			t.Fatalf("field %d oid = %d, want %d", i, field.PostgresTypeOID, wantOIDs[i])
		}
		if field.PostgresTableOID == 0 {
			t.Fatalf("field %d table oid = 0, want non-zero", i)
		}
		if field.PostgresTableAttributeNumber != wantAttrs[i] {
			t.Fatalf("field %d attr = %d, want %d", i, field.PostgresTableAttributeNumber, wantAttrs[i])
		}
	}
}

func TestPostgresConnPrepareIntegration(t *testing.T) {
	conn := newPostgresIntegrationConn(t)
	tableName := newPostgresIntegrationTable(t, conn)

	prepared, err := conn.Prepare(fmt.Sprintf("select id, note, amount, active from %s where id = ?", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	ps, ok := prepared.(*postgresStmt)
	if !ok {
		t.Fatalf("prepared type = %T, want *postgresStmt", prepared)
	}
	if ps.ParamNum() != 1 {
		t.Fatalf("ParamNum = %d, want 1", ps.ParamNum())
	}
	if got := ps.ParamOIDs(); len(got) != 1 || got[0] != 23 {
		t.Fatalf("ParamOIDs = %v, want [23]", got)
	}
	if len(ps.ColumnFields()) != 4 {
		t.Fatalf("len(ColumnFields) = %d, want 4", len(ps.ColumnFields()))
	}
	if ps.ColumnFields()[2].PostgresTypeOID != 1700 {
		t.Fatalf("amount oid = %d, want 1700", ps.ColumnFields()[2].PostgresTypeOID)
	}

	result, err := ps.Execute(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Fields) != 4 {
		t.Fatalf("len(result.Fields) = %d, want 4", len(result.Fields))
	}
	if result.Fields[0].PostgresTypeOID != 23 || result.Fields[1].PostgresTypeOID != 25 {
		t.Fatalf("result field oids = [%d %d], want [23 25]", result.Fields[0].PostgresTypeOID, result.Fields[1].PostgresTypeOID)
	}
}

func TestPostgresConnDescribeTypedExpressionsIntegration(t *testing.T) {
	conn := newPostgresIntegrationConn(t)

	description, err := conn.DescribeQuery(
		"select $1::int4 as id, $2::text as note, $3::numeric(10,2) as amount, $4::bool as active",
		nil,
	)
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
		if description.Fields[i].PostgresTypeOID != want {
			t.Fatalf("field %d oid = %d, want %d", i, description.Fields[i].PostgresTypeOID, want)
		}
		if description.Fields[i].PostgresTableOID != 0 {
			t.Fatalf("field %d table oid = %d, want 0 for expression", i, description.Fields[i].PostgresTableOID)
		}
	}
}

func TestPostgresConnDescribeQueryTempTableBoundary(t *testing.T) {
	conn := newPostgresIntegrationConn(t)
	tempTable := fmt.Sprintf("ks_pg_temp_%d", time.Now().UnixNano())

	if _, err := conn.Execute(fmt.Sprintf("create temporary table %s (id integer primary key)", tempTable)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Execute(fmt.Sprintf("insert into %s (id) values (1)", tempTable)); err != nil {
		t.Fatal(err)
	}

	result, err := conn.Execute(fmt.Sprintf("select id from %s where id = 1", tempTable))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Values) != 1 {
		t.Fatalf("len(result.Values) = %d, want 1", len(result.Values))
	}

	_, err = conn.DescribeQuery(fmt.Sprintf("select id from %s where id = $1", tempTable), []uint32{23})
	if err == nil {
		t.Fatal("expected temp table describe to fail across the dedicated metadata connection")
	}
}
