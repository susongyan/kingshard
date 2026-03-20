package server

import (
	"testing"

	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/sqlparser"
)

func TestNormalizeFrontendType(t *testing.T) {
	cases := map[string]string{
		"":           DefaultFrontendType,
		"mysql":      DefaultFrontendType,
		"MYSQL":      DefaultFrontendType,
		"postgres":   PostgresFrontendType,
		"PostgreSQL": PostgresFrontendType,
	}

	for input, want := range cases {
		if got := NormalizeFrontendType(input); got != want {
			t.Fatalf("NormalizeFrontendType(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestPostgresParseStartupParameters(t *testing.T) {
	params, err := postgresParseStartupParameters([]byte("user\x00demo\x00database\x00demo_db\x00application_name\x00psql\x00\x00"))
	if err != nil {
		t.Fatal(err)
	}

	if params["user"] != "demo" {
		t.Fatalf("user = %q, want %q", params["user"], "demo")
	}
	if params["database"] != "demo_db" {
		t.Fatalf("database = %q, want %q", params["database"], "demo_db")
	}
	if params["application_name"] != "psql" {
		t.Fatalf("application_name = %q, want %q", params["application_name"], "psql")
	}
}

func TestPostgresCommandTagForQuery(t *testing.T) {
	cases := []struct {
		query string
		want  string
		rows  uint64
	}{
		{query: "select 1", want: "SELECT 3", rows: 3},
		{query: "insert into test(id) values (1)", want: "INSERT 0 2", rows: 2},
		{query: "update test set a = 1", want: "UPDATE 4", rows: 4},
		{query: "set autocommit = 1", want: "SET", rows: 9},
		{query: "show tables", want: "SHOW 5", rows: 5},
	}

	for _, tc := range cases {
		tag := postgresCommandTagForQuery(tc.query).complete(tc.rows)
		if tag != tc.want {
			t.Fatalf("tag for %q = %q, want %q", tc.query, tag, tc.want)
		}
	}
}

func TestPostgresFieldType(t *testing.T) {
	_, _, oid, size, _, _ := postgresFieldDescription(&mysql.Field{Type: mysql.MYSQL_TYPE_LONGLONG})
	if oid != 20 || size != 8 {
		t.Fatalf("int8 oid/size = %d/%d, want 20/8", oid, size)
	}

	_, _, oid, size, _, _ = postgresFieldDescription(&mysql.Field{Type: mysql.MYSQL_TYPE_VAR_STRING})
	if oid != 25 || size != -1 {
		t.Fatalf("text oid/size = %d/%d, want 25/-1", oid, size)
	}
}

func TestPostgresFieldDescriptionUsesNativeMetadata(t *testing.T) {
	tableOID, attrNumber, oid, size, modifier, format := postgresFieldDescription(&mysql.Field{
		Name:                         []byte("price"),
		PostgresTableOID:             42,
		PostgresTableAttributeNumber: 3,
		PostgresTypeOID:              1700,
		PostgresTypeSize:             -1,
		PostgresTypeModifier:         11,
		PostgresFormat:               1,
	})
	if tableOID != 42 || attrNumber != 3 {
		t.Fatalf("table metadata = (%d,%d), want (42,3)", tableOID, attrNumber)
	}
	if oid != 1700 || size != -1 || modifier != 11 || format != 1 {
		t.Fatalf("wire metadata = (%d,%d,%d,%d), want (1700,-1,11,1)", oid, size, modifier, format)
	}
}

func TestPostgresReadFormatCodes(t *testing.T) {
	codes, rest, err := postgresReadFormatCodes([]byte{0, 2, 0, 0, 0, 1, 'x'})
	if err != nil {
		t.Fatal(err)
	}
	if len(codes) != 2 || codes[0] != 0 || codes[1] != 1 {
		t.Fatalf("codes = %v, want [0 1]", codes)
	}
	if len(rest) != 1 || rest[0] != 'x' {
		t.Fatalf("rest = %v, want [120]", rest)
	}
}

func TestPostgresTextValue(t *testing.T) {
	text, err := postgresTextValue(int64(42))
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "42" {
		t.Fatalf("text = %q, want %q", string(text), "42")
	}

	text, err = postgresTextValue(true)
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "t" {
		t.Fatalf("text = %q, want %q", string(text), "t")
	}
}

func TestPostgresRewriteBindPlaceholders(t *testing.T) {
	rewritten, order, count, err := postgresRewriteBindPlaceholders("select * from t where a = $2 and b = $1 and note = '$3'")
	if err != nil {
		t.Fatal(err)
	}
	if rewritten != "select * from t where a = ? and b = ? and note = '$3'" {
		t.Fatalf("rewritten = %q", rewritten)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("order = %v, want [2 1]", order)
	}
}

func TestPostgresExpandBindArgs(t *testing.T) {
	args := postgresExpandBindArgs([]interface{}{"a", "b"}, []int{2, 1, 2})
	if len(args) != 3 || args[0] != "b" || args[1] != "a" || args[2] != "b" {
		t.Fatalf("args = %v, want [b a b]", args)
	}
}

func TestPostgresDecodeTextBindValue(t *testing.T) {
	value := postgresDecodeTextBindValue("t", 16)
	boolean, ok := value.(bool)
	if !ok || !boolean {
		t.Fatalf("value = %#v, want true", value)
	}
}

func TestPostgresDecodeBinaryBindValue(t *testing.T) {
	value, err := postgresDecodeBinaryBindValue([]byte{0, 0, 0, 7}, 23)
	if err != nil {
		t.Fatal(err)
	}
	if value.(int64) != 7 {
		t.Fatalf("value = %#v, want 7", value)
	}
}

func TestPostgresResultFieldsFromStatement(t *testing.T) {
	stmt, err := sqlparser.Parse("select foo as bar, baz from test")
	if err != nil {
		t.Fatal(err)
	}

	fields := postgresResultFieldsFromStatement(stmt, &ClientConn{})
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2", len(fields))
	}
	if string(fields[0].Name) != "bar" {
		t.Fatalf("fields[0].Name = %q, want %q", string(fields[0].Name), "bar")
	}
	if string(fields[1].Name) != "baz" {
		t.Fatalf("fields[1].Name = %q, want %q", string(fields[1].Name), "baz")
	}

	stmt, err = sqlparser.Parse("select 1 as one")
	if err != nil {
		t.Fatal(err)
	}

	fields = postgresResultFieldsFromStatement(stmt, &ClientConn{})
	if len(fields) != 1 {
		t.Fatalf("len(simpleSelect fields) = %d, want 1", len(fields))
	}
	if string(fields[0].Name) != "one" {
		t.Fatalf("fields[0].Name = %q, want %q", string(fields[0].Name), "one")
	}
}
