package backend

import (
	"testing"

	"github.com/jackc/pgproto3/v2"
)

func TestNormalizeBackendType(t *testing.T) {
	if got := NormalizeBackendType("postgresql"); got != PostgresBackendType {
		t.Fatalf("NormalizeBackendType(postgresql) = %q, want %q", got, PostgresBackendType)
	}
}

func TestGetPostgresDriver(t *testing.T) {
	driver, err := GetDriver(PostgresBackendType)
	if err != nil {
		t.Fatal(err)
	}
	if driver.Name() != PostgresBackendType {
		t.Fatalf("driver.Name() = %q, want %q", driver.Name(), PostgresBackendType)
	}
}

func TestPostgresFieldFromNativeDescription(t *testing.T) {
	field := postgresFieldFromNativeDescription(pgproto3.FieldDescription{
		Name:                 []byte("amount"),
		TableOID:             123,
		TableAttributeNumber: 4,
		DataTypeOID:          1700,
		DataTypeSize:         -1,
		TypeModifier:         12,
		Format:               0,
	})

	if got := string(field.Name); got != "amount" {
		t.Fatalf("field.Name = %q, want %q", got, "amount")
	}
	if field.PostgresTypeOID != 1700 {
		t.Fatalf("field.PostgresTypeOID = %d, want 1700", field.PostgresTypeOID)
	}
	if field.PostgresTableOID != 123 || field.PostgresTableAttributeNumber != 4 {
		t.Fatalf("table metadata = (%d,%d), want (123,4)", field.PostgresTableOID, field.PostgresTableAttributeNumber)
	}
	if field.Type == 0 {
		t.Fatal("expected mysql field type mapping to be populated")
	}
}

func TestPostgresRewriteQuestionPlaceholders(t *testing.T) {
	rewritten, count, err := postgresRewriteQuestionPlaceholders("select '?' as literal, col from test where a = ? and note = $$?$tag$$")
	if err != nil {
		t.Fatal(err)
	}
	if rewritten != "select '?' as literal, col from test where a = $1 and note = $$?$tag$$" {
		t.Fatalf("rewritten = %q", rewritten)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}
