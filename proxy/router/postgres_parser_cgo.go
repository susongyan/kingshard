//go:build cgo
// +build cgo

package router

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func parsePostgresQuery(sql string) (*pg_query.ParseResult, error) {
	return pg_query.Parse(sql)
}

func deparsePostgresQuery(tree *pg_query.ParseResult) (string, error) {
	return pg_query.Deparse(tree)
}
