//go:build !cgo
// +build !cgo

package router

import (
	"strings"
	"testing"
)

func TestBuildPlanSQLPostgresWithoutCGO(t *testing.T) {
	if _, _, err := newTestRouter().BuildPlanSQL("kingshard", `select * from "test1" where id = 1`, RouteDialectPostgres, nil); err == nil {
		t.Fatal("expected postgres route parser error when cgo is disabled")
	} else if !strings.Contains(err.Error(), "cgo") {
		t.Fatalf("unexpected postgres route parser error: %v", err)
	}
}
