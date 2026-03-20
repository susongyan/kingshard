package router

import (
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/flike/kingshard/sqlparser"
)

func ParseRouteStatement(sql, dialect string) (*RouteStatement, error) {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "", RouteDialectMySQL:
		return parseMySQLRouteStatement(sql)
	case RouteDialectPostgres:
		return parsePostgresRouteStatement(sql)
	default:
		return nil, fmt.Errorf("unsupported route dialect %q", dialect)
	}
}

func parseMySQLRouteStatement(sql string) (*RouteStatement, error) {
	stmt, err := sqlparser.Parse(strings.TrimRight(strings.TrimSpace(sql), ";"))
	if err != nil {
		return nil, err
	}

	routed := &RouteStatement{
		Dialect:        RouteDialectMySQL,
		OriginalSQL:    sql,
		mysqlStatement: stmt,
	}

	switch typed := stmt.(type) {
	case *sqlparser.Select:
		routed.Kind = RouteStatementSelect
		routed.Table = mysqlRouteTableFromSelect(typed)
		routed.Condition = mysqlRouteConditionFromWhere(typed.Where)
		routed.ReturnsRows = true
		if len(typed.GroupBy) > 0 || typed.Having != nil || typed.OrderBy != nil || typed.Limit != nil {
			routed.RequiresSingleShard = true
		}
	case *sqlparser.Insert:
		routed.Kind = RouteStatementInsert
		routed.Table = RouteTable{Name: sqlparser.String(typed.Table)}
		routed.InsertColumns = mysqlRouteColumns(typed.Columns)
		rows, ok := typed.Rows.(sqlparser.Values)
		if !ok {
			routed.Fallback = true
			routed.FallbackReason = "mysql insert-select uses fallback routing"
			return routed, nil
		}
		insertRows, err := mysqlRouteInsertRows(rows)
		if err != nil {
			return nil, err
		}
		routed.InsertRows = insertRows
	case *sqlparser.Update:
		routed.Kind = RouteStatementUpdate
		routed.Table = RouteTable{Name: sqlparser.String(typed.Table)}
		routed.Condition = mysqlRouteConditionFromWhere(typed.Where)
		routed.UpdateColumns = mysqlRouteUpdateColumns(typed.Exprs)
	case *sqlparser.Delete:
		routed.Kind = RouteStatementDelete
		routed.Table = RouteTable{Name: sqlparser.String(typed.Table)}
		routed.Condition = mysqlRouteConditionFromWhere(typed.Where)
	case *sqlparser.Replace:
		routed.Kind = RouteStatementReplace
		routed.Table = RouteTable{Name: sqlparser.String(typed.Table)}
		routed.InsertColumns = mysqlRouteColumns(typed.Columns)
		rows, ok := typed.Rows.(sqlparser.Values)
		if !ok {
			routed.Fallback = true
			routed.FallbackReason = "mysql replace-select uses fallback routing"
			return routed, nil
		}
		insertRows, err := mysqlRouteInsertRows(rows)
		if err != nil {
			return nil, err
		}
		routed.InsertRows = insertRows
	case *sqlparser.Truncate:
		routed.Kind = RouteStatementTruncate
		routed.Table = RouteTable{Name: sqlparser.String(typed.Table)}
	default:
		routed.Kind = RouteStatementUnknown
	}

	return routed, nil
}

func mysqlRouteTableFromSelect(stmt *sqlparser.Select) RouteTable {
	if stmt == nil || len(stmt.From) == 0 {
		return RouteTable{}
	}

	switch typed := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		table := RouteTable{Name: sqlparser.String(typed.Expr)}
		if len(typed.As) > 0 {
			table.Alias = string(typed.As)
		}
		return table
	default:
		return RouteTable{Name: sqlparser.String(typed)}
	}
}

func mysqlRouteConditionFromWhere(where *sqlparser.Where) RouteCondition {
	if where == nil {
		return nil
	}
	return mysqlRouteConditionFromBoolExpr(where.Expr)
}

func mysqlRouteConditionFromBoolExpr(expr sqlparser.BoolExpr) RouteCondition {
	switch typed := expr.(type) {
	case *sqlparser.AndExpr:
		return &RouteBooleanCondition{
			Operator: "and",
			Left:     mysqlRouteConditionFromBoolExpr(typed.Left),
			Right:    mysqlRouteConditionFromBoolExpr(typed.Right),
		}
	case *sqlparser.OrExpr:
		return &RouteBooleanCondition{
			Operator: "or",
			Left:     mysqlRouteConditionFromBoolExpr(typed.Left),
			Right:    mysqlRouteConditionFromBoolExpr(typed.Right),
		}
	case *sqlparser.ParenBoolExpr:
		return mysqlRouteConditionFromBoolExpr(typed.Expr)
	case *sqlparser.ComparisonExpr:
		return &RouteComparisonCondition{
			Operator: strings.ToLower(typed.Operator),
			Left:     mysqlRouteExpr(typed.Left),
			Right:    mysqlRouteExpr(typed.Right),
		}
	case *sqlparser.RangeCond:
		return &RouteRangeCondition{
			Operator: strings.ToLower(typed.Operator),
			Left:     mysqlRouteExpr(typed.Left),
			From:     mysqlRouteExpr(typed.From),
			To:       mysqlRouteExpr(typed.To),
		}
	default:
		return &RouteUnknownCondition{}
	}
}

func mysqlRouteExpr(expr sqlparser.ValExpr) RouteExpr {
	switch typed := expr.(type) {
	case *sqlparser.ColName:
		return &RouteColumnExpr{
			Qualifier: string(typed.Qualifier),
			Name:      string(typed.Name),
		}
	case sqlparser.StrVal:
		return &RouteValueExpr{Value: RouteValue{Kind: RouteValueLiteral, Literal: string(typed)}}
	case sqlparser.NumVal:
		value, err := strconv.ParseInt(string(typed), 10, 64)
		if err != nil {
			return &RouteValueExpr{Value: RouteValue{Kind: RouteValueLiteral, Literal: string(typed)}}
		}
		return &RouteValueExpr{Value: RouteValue{Kind: RouteValueLiteral, Literal: value}}
	case sqlparser.ValArg:
		return &RouteValueExpr{Value: RouteValue{Kind: RouteValueParam}}
	case sqlparser.ValTuple:
		items := make([]RouteExpr, 0, len(typed))
		for _, item := range typed {
			items = append(items, mysqlRouteExpr(item))
		}
		return &RouteListExpr{Items: items}
	default:
		return nil
	}
}

func mysqlRouteColumns(columns sqlparser.Columns) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		nonStar, ok := column.(*sqlparser.NonStarExpr)
		if !ok {
			continue
		}
		name, ok := nonStar.Expr.(*sqlparser.ColName)
		if !ok {
			continue
		}
		result = append(result, string(name.Name))
	}
	return result
}

func mysqlRouteInsertRows(values sqlparser.Values) ([]RouteInsertRow, error) {
	rows := make([]RouteInsertRow, 0, len(values))
	for _, value := range values {
		tuple, ok := value.(sqlparser.ValTuple)
		if !ok {
			return nil, fmt.Errorf("unsupported mysql insert tuple %T", value)
		}
		row := RouteInsertRow{Values: make([]RouteValue, 0, len(tuple))}
		for _, item := range tuple {
			valueExpr, ok := mysqlRouteExpr(item).(*RouteValueExpr)
			if !ok || valueExpr == nil {
				return nil, fmt.Errorf("unsupported mysql insert value %T", item)
			}
			row.Values = append(row.Values, valueExpr.Value)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func mysqlRouteUpdateColumns(exprs sqlparser.UpdateExprs) []string {
	columns := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		columns = append(columns, string(expr.Name.Name))
	}
	return columns
}

func parsePostgresRouteStatement(sql string) (*RouteStatement, error) {
	tree, err := parsePostgresQuery(strings.TrimSpace(sql))
	if err != nil {
		return nil, err
	}
	if tree == nil || len(tree.Stmts) != 1 || tree.Stmts[0] == nil || tree.Stmts[0].Stmt == nil {
		return nil, fmt.Errorf("postgres route parser expects exactly one statement")
	}

	routed := &RouteStatement{
		Dialect:      RouteDialectPostgres,
		OriginalSQL:  sql,
		postgresTree: tree,
	}

	node := tree.Stmts[0].Stmt
	switch {
	case node.GetSelectStmt() != nil:
		return parsePostgresSelectRouteStatement(routed, node.GetSelectStmt())
	case node.GetInsertStmt() != nil:
		return parsePostgresInsertRouteStatement(routed, node.GetInsertStmt())
	case node.GetUpdateStmt() != nil:
		return parsePostgresUpdateRouteStatement(routed, node.GetUpdateStmt())
	case node.GetDeleteStmt() != nil:
		return parsePostgresDeleteRouteStatement(routed, node.GetDeleteStmt())
	case node.GetTruncateStmt() != nil:
		return parsePostgresTruncateRouteStatement(routed, node.GetTruncateStmt())
	default:
		routed.Kind = RouteStatementUnknown
		routed.Fallback = true
		routed.FallbackReason = "postgres statement kind is not in the route subset"
		return routed, nil
	}
}

func parsePostgresSelectRouteStatement(routed *RouteStatement, stmt *pg_query.SelectStmt) (*RouteStatement, error) {
	routed.Kind = RouteStatementSelect
	routed.ReturnsRows = true

	if stmt == nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres select statement is empty"
		return routed, nil
	}
	if stmt.Op != pg_query.SetOperation_SETOP_NONE || stmt.Larg != nil || stmt.Rarg != nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres set operations use fallback routing"
		return routed, nil
	}
	if stmt.WithClause != nil || stmt.IntoClause != nil || len(stmt.WindowClause) > 0 || len(stmt.LockingClause) > 0 {
		routed.Fallback = true
		routed.FallbackReason = "postgres with/into/window/locking select uses fallback routing"
		return routed, nil
	}
	if len(stmt.FromClause) != 1 {
		routed.Fallback = true
		routed.FallbackReason = "postgres multi-from select uses fallback routing"
		return routed, nil
	}

	table, ok := postgresRouteTable(stmt.FromClause[0])
	if !ok {
		routed.Fallback = true
		routed.FallbackReason = "postgres select from target is not a simple table"
		return routed, nil
	}
	routed.Table = table
	routed.Condition = postgresRouteCondition(stmt.WhereClause)
	routed.RequiresSingleShard = stmt.DistinctClause != nil || len(stmt.GroupClause) > 0 || stmt.HavingClause != nil || len(stmt.SortClause) > 0 || stmt.LimitCount != nil || stmt.LimitOffset != nil
	if postgresSelectNeedsFallbackForQualifier(stmt, table) {
		routed.Fallback = true
		routed.FallbackReason = "postgres select uses logical table qualifiers without alias"
	}
	return routed, nil
}

func parsePostgresInsertRouteStatement(routed *RouteStatement, stmt *pg_query.InsertStmt) (*RouteStatement, error) {
	routed.Kind = RouteStatementInsert
	routed.ReturnsRows = len(stmt.GetReturningList()) > 0

	if stmt == nil || stmt.Relation == nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres insert relation is empty"
		return routed, nil
	}
	if stmt.WithClause != nil || stmt.OnConflictClause != nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres with/on conflict insert uses fallback routing"
		return routed, nil
	}
	if stmt.Relation.GetAlias() != nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres insert alias uses fallback routing"
		return routed, nil
	}

	routed.Table = RouteTable{
		Schema: stmt.Relation.GetSchemaname(),
		Name:   stmt.Relation.GetRelname(),
	}

	if len(stmt.GetCols()) == 0 {
		routed.Fallback = true
		routed.FallbackReason = "postgres insert without column list uses fallback routing"
		return routed, nil
	}
	routed.InsertColumns = make([]string, 0, len(stmt.GetCols()))
	for _, column := range stmt.GetCols() {
		target := column.GetResTarget()
		if target == nil || target.GetName() == "" {
			routed.Fallback = true
			routed.FallbackReason = "postgres insert column list is not a simple identifier"
			return routed, nil
		}
		routed.InsertColumns = append(routed.InsertColumns, target.GetName())
	}

	valuesStmt := stmt.GetSelectStmt().GetSelectStmt()
	if valuesStmt == nil || valuesStmt.Op != pg_query.SetOperation_SETOP_NONE || len(valuesStmt.ValuesLists) == 0 {
		routed.Fallback = true
		routed.FallbackReason = "postgres insert values subset only supports VALUES lists"
		return routed, nil
	}
	if len(valuesStmt.FromClause) > 0 || valuesStmt.WhereClause != nil || len(valuesStmt.SortClause) > 0 || valuesStmt.LimitCount != nil || valuesStmt.LimitOffset != nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres insert-select uses fallback routing"
		return routed, nil
	}

	routed.InsertRows = make([]RouteInsertRow, 0, len(valuesStmt.ValuesLists))
	for _, item := range valuesStmt.ValuesLists {
		list := item.GetList()
		if list == nil {
			routed.Fallback = true
			routed.FallbackReason = "postgres insert row is not a value list"
			return routed, nil
		}
		row := RouteInsertRow{Values: make([]RouteValue, 0, len(list.GetItems()))}
		for _, valueNode := range list.GetItems() {
			value, ok := postgresRouteValue(valueNode)
			if !ok {
				routed.Fallback = true
				routed.FallbackReason = "postgres insert value is outside the route subset"
				return routed, nil
			}
			row.Values = append(row.Values, value)
		}
		routed.InsertRows = append(routed.InsertRows, row)
	}
	return routed, nil
}

func parsePostgresUpdateRouteStatement(routed *RouteStatement, stmt *pg_query.UpdateStmt) (*RouteStatement, error) {
	routed.Kind = RouteStatementUpdate
	routed.ReturnsRows = len(stmt.GetReturningList()) > 0

	if stmt == nil || stmt.Relation == nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres update relation is empty"
		return routed, nil
	}
	if stmt.WithClause != nil || len(stmt.FromClause) > 0 {
		routed.Fallback = true
		routed.FallbackReason = "postgres update from/with uses fallback routing"
		return routed, nil
	}

	routed.Table = RouteTable{
		Schema: stmt.Relation.GetSchemaname(),
		Name:   stmt.Relation.GetRelname(),
	}
	if stmt.Relation.GetAlias() != nil {
		routed.Table.Alias = stmt.Relation.GetAlias().GetAliasname()
	}
	routed.Condition = postgresRouteCondition(stmt.WhereClause)
	routed.UpdateColumns = make([]string, 0, len(stmt.TargetList))
	for _, targetNode := range stmt.TargetList {
		target := targetNode.GetResTarget()
		if target == nil || target.GetName() == "" {
			routed.Fallback = true
			routed.FallbackReason = "postgres update target is not a simple identifier"
			return routed, nil
		}
		routed.UpdateColumns = append(routed.UpdateColumns, target.GetName())
	}
	if postgresUpdateNeedsFallbackForQualifier(stmt, routed.Table) {
		routed.Fallback = true
		routed.FallbackReason = "postgres update uses logical table qualifiers without alias"
	}
	return routed, nil
}

func parsePostgresDeleteRouteStatement(routed *RouteStatement, stmt *pg_query.DeleteStmt) (*RouteStatement, error) {
	routed.Kind = RouteStatementDelete
	routed.ReturnsRows = len(stmt.GetReturningList()) > 0

	if stmt == nil || stmt.Relation == nil {
		routed.Fallback = true
		routed.FallbackReason = "postgres delete relation is empty"
		return routed, nil
	}
	if stmt.WithClause != nil || len(stmt.UsingClause) > 0 {
		routed.Fallback = true
		routed.FallbackReason = "postgres delete using/with uses fallback routing"
		return routed, nil
	}

	routed.Table = RouteTable{
		Schema: stmt.Relation.GetSchemaname(),
		Name:   stmt.Relation.GetRelname(),
	}
	if stmt.Relation.GetAlias() != nil {
		routed.Table.Alias = stmt.Relation.GetAlias().GetAliasname()
	}
	routed.Condition = postgresRouteCondition(stmt.WhereClause)
	return routed, nil
}

func parsePostgresTruncateRouteStatement(routed *RouteStatement, stmt *pg_query.TruncateStmt) (*RouteStatement, error) {
	routed.Kind = RouteStatementTruncate
	if stmt == nil || len(stmt.Relations) != 1 {
		routed.Fallback = true
		routed.FallbackReason = "postgres truncate subset only supports one table"
		return routed, nil
	}
	table, ok := postgresRouteTable(stmt.Relations[0])
	if !ok {
		routed.Fallback = true
		routed.FallbackReason = "postgres truncate target is not a simple table"
		return routed, nil
	}
	routed.Table = table
	return routed, nil
}

func postgresRouteTable(node *pg_query.Node) (RouteTable, bool) {
	rangeVar := node.GetRangeVar()
	if rangeVar == nil {
		return RouteTable{}, false
	}
	table := RouteTable{
		Schema: rangeVar.GetSchemaname(),
		Name:   rangeVar.GetRelname(),
	}
	if rangeVar.GetAlias() != nil {
		table.Alias = rangeVar.GetAlias().GetAliasname()
	}
	if table.Name == "" {
		return RouteTable{}, false
	}
	return table, true
}

func postgresRouteCondition(node *pg_query.Node) RouteCondition {
	if node == nil {
		return nil
	}
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		args := boolExpr.GetArgs()
		switch boolExpr.GetBoolop() {
		case pg_query.BoolExprType_AND_EXPR:
			return postgresReduceBooleanConditions("and", args)
		case pg_query.BoolExprType_OR_EXPR:
			return postgresReduceBooleanConditions("or", args)
		default:
			return &RouteUnknownCondition{}
		}
	}
	if expr := node.GetAExpr(); expr != nil {
		switch expr.GetKind() {
		case pg_query.A_Expr_Kind_AEXPR_OP:
			return &RouteComparisonCondition{
				Operator: strings.ToLower(postgresOperator(expr.GetName())),
				Left:     postgresRouteExpr(expr.GetLexpr()),
				Right:    postgresRouteExpr(expr.GetRexpr()),
			}
		case pg_query.A_Expr_Kind_AEXPR_IN:
			operator := strings.ToLower(postgresOperator(expr.GetName()))
			if operator == "" {
				operator = "="
			}
			if operator == "=" {
				operator = "in"
			} else {
				operator = "not in"
			}
			return &RouteComparisonCondition{
				Operator: operator,
				Left:     postgresRouteExpr(expr.GetLexpr()),
				Right:    postgresRouteExpr(expr.GetRexpr()),
			}
		case pg_query.A_Expr_Kind_AEXPR_BETWEEN, pg_query.A_Expr_Kind_AEXPR_BETWEEN_SYM:
			return postgresRouteRangeCondition("between", expr)
		case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN, pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
			return postgresRouteRangeCondition("not between", expr)
		default:
			return &RouteUnknownCondition{}
		}
	}
	return &RouteUnknownCondition{}
}

func postgresReduceBooleanConditions(operator string, args []*pg_query.Node) RouteCondition {
	if len(args) == 0 {
		return &RouteUnknownCondition{}
	}
	current := postgresRouteCondition(args[0])
	for i := 1; i < len(args); i++ {
		current = &RouteBooleanCondition{
			Operator: operator,
			Left:     current,
			Right:    postgresRouteCondition(args[i]),
		}
	}
	return current
}

func postgresRouteRangeCondition(operator string, expr *pg_query.A_Expr) RouteCondition {
	values := expr.GetRexpr().GetList()
	if values == nil || len(values.GetItems()) != 2 {
		return &RouteUnknownCondition{}
	}
	return &RouteRangeCondition{
		Operator: operator,
		Left:     postgresRouteExpr(expr.GetLexpr()),
		From:     postgresRouteExpr(values.GetItems()[0]),
		To:       postgresRouteExpr(values.GetItems()[1]),
	}
}

func postgresRouteExpr(node *pg_query.Node) RouteExpr {
	if node == nil {
		return nil
	}
	if typeCast := node.GetTypeCast(); typeCast != nil {
		return &RouteTypeCastExpr{Expr: postgresRouteExpr(typeCast.GetArg())}
	}
	if column := node.GetColumnRef(); column != nil {
		return postgresRouteColumnExpr(column)
	}
	if param := node.GetParamRef(); param != nil {
		return &RouteValueExpr{Value: RouteValue{Kind: RouteValueParam, ParamIndex: int(param.GetNumber())}}
	}
	if literal, ok := postgresRouteValue(node); ok {
		return &RouteValueExpr{Value: literal}
	}
	if list := node.GetList(); list != nil {
		items := make([]RouteExpr, 0, len(list.GetItems()))
		for _, item := range list.GetItems() {
			items = append(items, postgresRouteExpr(item))
		}
		return &RouteListExpr{Items: items}
	}
	return nil
}

func postgresRouteColumnExpr(column *pg_query.ColumnRef) RouteExpr {
	if column == nil || len(column.GetFields()) == 0 {
		return nil
	}
	fields := column.GetFields()
	if len(fields) == 1 {
		name := postgresNodeString(fields[0])
		if name == "" {
			return nil
		}
		return &RouteColumnExpr{Name: name}
	}

	name := postgresNodeString(fields[len(fields)-1])
	qualifier := postgresNodeString(fields[len(fields)-2])
	if name == "" {
		return nil
	}
	return &RouteColumnExpr{
		Qualifier: qualifier,
		Name:      name,
	}
}

func postgresRouteValue(node *pg_query.Node) (RouteValue, bool) {
	if node == nil {
		return RouteValue{}, false
	}
	constant := node.GetAConst()
	if constant == nil {
		return RouteValue{}, false
	}
	if constant.GetIsnull() {
		return RouteValue{Kind: RouteValueLiteral, Literal: nil}, true
	}
	if value := constant.GetIval(); value != nil {
		return RouteValue{Kind: RouteValueLiteral, Literal: int64(value.GetIval())}, true
	}
	if value := constant.GetFval(); value != nil {
		if parsed, err := strconv.ParseFloat(value.GetFval(), 64); err == nil {
			return RouteValue{Kind: RouteValueLiteral, Literal: parsed}, true
		}
		return RouteValue{Kind: RouteValueLiteral, Literal: value.GetFval()}, true
	}
	if value := constant.GetSval(); value != nil {
		return RouteValue{Kind: RouteValueLiteral, Literal: value.GetSval()}, true
	}
	if value := constant.GetBoolval(); value != nil {
		return RouteValue{Kind: RouteValueLiteral, Literal: value.GetBoolval()}, true
	}
	return RouteValue{}, false
}

func postgresOperator(nodes []*pg_query.Node) string {
	if len(nodes) == 0 {
		return ""
	}
	return postgresNodeString(nodes[0])
}

func postgresNodeString(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if value := node.GetString_(); value != nil {
		return value.GetSval()
	}
	return ""
}

func postgresSelectNeedsFallbackForQualifier(stmt *pg_query.SelectStmt, table RouteTable) bool {
	for _, target := range stmt.GetTargetList() {
		if postgresNodeReferencesLogicalTable(target, table) {
			return true
		}
	}
	return postgresNodeReferencesLogicalTable(stmt.GetWhereClause(), table)
}

func postgresUpdateNeedsFallbackForQualifier(stmt *pg_query.UpdateStmt, table RouteTable) bool {
	if postgresNodeReferencesLogicalTable(stmt.GetWhereClause(), table) {
		return true
	}
	for _, target := range stmt.GetTargetList() {
		if postgresNodeReferencesLogicalTable(target, table) {
			return true
		}
	}
	return false
}

func postgresNodeReferencesLogicalTable(node *pg_query.Node, table RouteTable) bool {
	if node == nil || table.Alias != "" {
		return false
	}
	if column := node.GetColumnRef(); column != nil {
		fields := column.GetFields()
		if len(fields) >= 2 && postgresNodeString(fields[len(fields)-2]) == table.Name {
			return true
		}
	}
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.GetArgs() {
			if postgresNodeReferencesLogicalTable(arg, table) {
				return true
			}
		}
	}
	if expr := node.GetAExpr(); expr != nil {
		return postgresNodeReferencesLogicalTable(expr.GetLexpr(), table) || postgresNodeReferencesLogicalTable(expr.GetRexpr(), table)
	}
	if list := node.GetList(); list != nil {
		for _, item := range list.GetItems() {
			if postgresNodeReferencesLogicalTable(item, table) {
				return true
			}
		}
	}
	if typeCast := node.GetTypeCast(); typeCast != nil {
		return postgresNodeReferencesLogicalTable(typeCast.GetArg(), table)
	}
	if target := node.GetResTarget(); target != nil {
		return postgresNodeReferencesLogicalTable(target.GetVal(), table)
	}
	return false
}
