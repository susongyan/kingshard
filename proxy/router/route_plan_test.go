package router

import (
	"testing"

	"github.com/flike/kingshard/config"
)

func TestParseRouteStatementMySQL(t *testing.T) {
	stmt, err := ParseRouteStatement("select * from test1 where id = 5", RouteDialectMySQL)
	if err != nil {
		t.Fatal(err)
	}

	if stmt.Kind != RouteStatementSelect {
		t.Fatalf("kind = %s, want %s", stmt.Kind, RouteStatementSelect)
	}
	if stmt.Table.Name != "test1" {
		t.Fatalf("table = %q, want %q", stmt.Table.Name, "test1")
	}
	if stmt.MySQLStatement() == nil {
		t.Fatal("mysql statement should be preserved")
	}
}

func TestBuildPlanSQLMySQL(t *testing.T) {
	r := newTestRouter()
	plan, stmt, err := r.BuildPlanSQL("kingshard", "select * from test1 where id = 5", RouteDialectMySQL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil {
		t.Fatal("route statement should not be nil")
	}
	if !isListEqual(plan.RouteTableIndexs, []int{5}) {
		t.Fatalf("route table indexes = %v, want [5]", plan.RouteTableIndexs)
	}
	if !isListEqual(plan.RouteNodeIndexs, []int{1}) {
		t.Fatalf("route node indexes = %v, want [1]", plan.RouteNodeIndexs)
	}
}

func TestNewRouterConfiguredFallback(t *testing.T) {
	cfg, err := config.ParseConfigData([]byte(`
schema_list:
- user: demo
  nodes: [node1, node2]
  default: node1
  fallback: node2
  shard:
  - db: kingshard
    table: test1
    key: id
    nodes: [node1, node2]
    locations: [1, 1]
    type: hash
`))
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRouter(&cfg.SchemaList[0])
	if err != nil {
		t.Fatal(err)
	}
	if got := r.GetFallbackRule("kingshard").Nodes[0]; got != "node2" {
		t.Fatalf("fallback node = %q, want %q", got, "node2")
	}
}

func TestBuildPlanForFallbackStatement(t *testing.T) {
	r := newTestRouter()
	stmt := &RouteStatement{
		Dialect:        RouteDialectPostgres,
		Kind:           RouteStatementUnknown,
		Fallback:       true,
		OriginalSQL:    `select * from "test1"`,
		FallbackReason: "test fallback",
	}

	plan, err := r.BuildPlanForStatement("kingshard", stmt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Rule == nil {
		t.Fatal("plan rule should not be nil")
	}
	if got := plan.Rule.Nodes[0]; got != "node1" {
		t.Fatalf("fallback plan node = %q, want %q", got, "node1")
	}
	if got := plan.RewrittenSqls["node1"][0]; got != stmt.OriginalSQL {
		t.Fatalf("fallback sql = %q, want %q", got, stmt.OriginalSQL)
	}
}
