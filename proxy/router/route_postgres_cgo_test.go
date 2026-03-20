//go:build cgo
// +build cgo

package router

import "testing"

func TestParseRouteStatementPostgresSelect(t *testing.T) {
	stmt, err := ParseRouteStatement(`select * from "test1" where id = $1`, RouteDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Kind != RouteStatementSelect {
		t.Fatalf("kind = %s, want %s", stmt.Kind, RouteStatementSelect)
	}
	if stmt.Table.Name != "test1" {
		t.Fatalf("table = %q, want %q", stmt.Table.Name, "test1")
	}
	if stmt.PostgresTree() == nil {
		t.Fatal("postgres parse tree should be preserved")
	}
}

func TestBuildPlanSQLPostgresQuotedTable(t *testing.T) {
	r := newTestRouter()
	plan, stmt, err := r.BuildPlanSQL("kingshard", `select * from "test1" where id = 5`, RouteDialectPostgres, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Kind != RouteStatementSelect {
		t.Fatalf("kind = %s, want %s", stmt.Kind, RouteStatementSelect)
	}
	if !isListEqual(plan.RouteTableIndexs, []int{5}) {
		t.Fatalf("route table indexes = %v, want [5]", plan.RouteTableIndexs)
	}
	sqls := plan.RewrittenSqls["node2"]
	if len(sqls) != 1 {
		t.Fatalf("rewritten sql count = %d, want 1", len(sqls))
	}
	if sqls[0] == `select * from "test1" where id = 5` {
		t.Fatal("rewritten postgres sql should target a physical shard table")
	}
}
