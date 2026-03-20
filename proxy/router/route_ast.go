package router

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/flike/kingshard/sqlparser"
)

const (
	RouteDialectMySQL    = "mysql"
	RouteDialectPostgres = "postgres"
)

type RouteStatementKind string

const (
	RouteStatementUnknown  RouteStatementKind = "unknown"
	RouteStatementSelect   RouteStatementKind = "select"
	RouteStatementInsert   RouteStatementKind = "insert"
	RouteStatementUpdate   RouteStatementKind = "update"
	RouteStatementDelete   RouteStatementKind = "delete"
	RouteStatementReplace  RouteStatementKind = "replace"
	RouteStatementTruncate RouteStatementKind = "truncate"
)

type RouteTable struct {
	Schema string
	Name   string
	Alias  string
}

func (t RouteTable) LookupName(defaultDB string) string {
	switch {
	case t.Schema != "":
		return t.Schema + "." + t.Name
	case defaultDB != "":
		return t.Name
	default:
		return t.Name
	}
}

func (t RouteTable) MatchesQualifier(qualifier string) bool {
	if qualifier == "" {
		return true
	}
	if t.Alias != "" && qualifier == t.Alias {
		return true
	}
	return qualifier == t.Name
}

type RouteValueKind int

const (
	RouteValueUnknown RouteValueKind = iota
	RouteValueLiteral
	RouteValueParam
)

type RouteValue struct {
	Kind       RouteValueKind
	Literal    interface{}
	ParamIndex int
}

func (v RouteValue) Resolve(args []interface{}) (interface{}, bool) {
	switch v.Kind {
	case RouteValueLiteral:
		return v.Literal, true
	case RouteValueParam:
		if v.ParamIndex <= 0 || v.ParamIndex > len(args) {
			return nil, false
		}
		return args[v.ParamIndex-1], true
	default:
		return nil, false
	}
}

type RouteExpr interface {
	isRouteExpr()
}

type RouteColumnExpr struct {
	Qualifier string
	Name      string
}

func (*RouteColumnExpr) isRouteExpr() {}

type RouteValueExpr struct {
	Value RouteValue
}

func (*RouteValueExpr) isRouteExpr() {}

type RouteListExpr struct {
	Items []RouteExpr
}

func (*RouteListExpr) isRouteExpr() {}

type RouteTypeCastExpr struct {
	Expr RouteExpr
}

func (*RouteTypeCastExpr) isRouteExpr() {}

type RouteCondition interface {
	isRouteCondition()
}

type RouteBooleanCondition struct {
	Operator string
	Left     RouteCondition
	Right    RouteCondition
}

func (*RouteBooleanCondition) isRouteCondition() {}

type RouteComparisonCondition struct {
	Operator string
	Left     RouteExpr
	Right    RouteExpr
}

func (*RouteComparisonCondition) isRouteCondition() {}

type RouteRangeCondition struct {
	Operator string
	Left     RouteExpr
	From     RouteExpr
	To       RouteExpr
}

func (*RouteRangeCondition) isRouteCondition() {}

type RouteUnknownCondition struct{}

func (*RouteUnknownCondition) isRouteCondition() {}

type RouteInsertRow struct {
	Values []RouteValue
}

type RouteStatement struct {
	Dialect string
	Kind    RouteStatementKind

	Table         RouteTable
	Condition     RouteCondition
	InsertColumns []string
	InsertRows    []RouteInsertRow
	UpdateColumns []string

	ReturnsRows         bool
	RequiresSingleShard bool
	Fallback            bool
	FallbackReason      string
	OriginalSQL         string

	mysqlStatement sqlparser.Statement
	postgresTree   *pg_query.ParseResult
}

func (s *RouteStatement) MySQLStatement() sqlparser.Statement {
	if s == nil {
		return nil
	}
	return s.mysqlStatement
}

func (s *RouteStatement) PostgresTree() *pg_query.ParseResult {
	if s == nil {
		return nil
	}
	return s.postgresTree
}
