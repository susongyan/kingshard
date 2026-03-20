//go:build !cgo
// +build !cgo

package router

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func parsePostgresQuery(sql string) (*pg_query.ParseResult, error) {
	return nil, fmt.Errorf("postgres route parser requires cgo-enabled builds")
}

func deparsePostgresQuery(tree *pg_query.ParseResult) (string, error) {
	return "", fmt.Errorf("postgres route deparser requires cgo-enabled builds")
}
