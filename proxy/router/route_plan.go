package router

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/sqlparser"
)

func (r *Router) BuildPlanSQL(db, sql, dialect string, args []interface{}) (*Plan, *RouteStatement, error) {
	stmt, err := ParseRouteStatement(sql, dialect)
	if err != nil {
		return nil, nil, err
	}

	plan, err := r.BuildPlanForStatement(db, stmt, args)
	if err != nil {
		return nil, stmt, err
	}
	return plan, stmt, nil
}

func (r *Router) BuildPlanForStatement(db string, stmt *RouteStatement, args []interface{}) (*Plan, error) {
	if stmt == nil {
		return nil, errors.ErrNoPlan
	}

	if stmt.Fallback {
		return r.buildFallbackPlan(db, stmt), nil
	}

	if stmt.Dialect == RouteDialectMySQL && stmt.MySQLStatement() != nil {
		return r.BuildPlan(db, stmt.MySQLStatement())
	}

	switch stmt.Kind {
	case RouteStatementSelect:
		return r.buildRouteSelectPlan(db, stmt, args)
	case RouteStatementInsert:
		return r.buildRouteInsertPlan(db, stmt, args)
	case RouteStatementUpdate:
		return r.buildRouteUpdatePlan(db, stmt, args)
	case RouteStatementDelete:
		return r.buildRouteDeletePlan(db, stmt, args)
	case RouteStatementTruncate:
		return r.buildRouteTruncatePlan(db, stmt)
	default:
		return r.buildFallbackPlan(db, stmt), nil
	}
}

func (r *Router) buildFallbackPlan(db string, stmt *RouteStatement) *Plan {
	rule := r.GetFallbackRule(db)
	plan := &Plan{
		Rule:            rule,
		RouteNodeIndexs: []int{0},
		RewrittenSqls: map[string][]string{
			rule.Nodes[0]: {routeOriginalSQL(stmt)},
		},
	}
	return plan
}

func (r *Router) buildRouteSelectPlan(db string, stmt *RouteStatement, args []interface{}) (*Plan, error) {
	plan := &Plan{Rule: r.GetRule(db, stmt.Table.LookupName(db))}
	if plan.Rule.Type == DefaultRuleType {
		return r.buildPassthroughPlan(plan.Rule, stmt), nil
	}

	indexes, err := routeTableIndexes(plan.Rule, stmt.Table, stmt.Condition, args)
	if err != nil {
		return nil, err
	}
	if stmt.RequiresSingleShard && len(indexes) > 1 {
		return r.buildFallbackPlan(db, stmt), nil
	}

	plan.RouteTableIndexs = indexes
	plan.RouteNodeIndexs = routeNodeIndexes(plan.Rule, indexes)
	if len(plan.RouteTableIndexs) == 0 {
		return nil, errors.ErrNoCriteria
	}
	if err := r.rewriteRoutePlan(plan, stmt); err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildRouteInsertPlan(db string, stmt *RouteStatement, args []interface{}) (*Plan, error) {
	plan := &Plan{Rule: r.GetRule(db, stmt.Table.LookupName(db))}
	if plan.Rule.Type == DefaultRuleType {
		return r.buildPassthroughPlan(plan.Rule, stmt), nil
	}

	keyIndex := routeInsertKeyIndex(plan.Rule, stmt.InsertColumns)
	if keyIndex < 0 {
		return nil, errors.ErrIRNoShardingKey
	}

	indexes, rowGroups, err := routeInsertIndexes(plan.Rule, stmt.InsertRows, keyIndex, args)
	if err != nil {
		return nil, err
	}
	plan.RouteTableIndexs = indexes
	plan.RouteNodeIndexs = routeNodeIndexes(plan.Rule, indexes)
	plan.RouteRowIndexGroups = rowGroups
	if len(plan.RouteTableIndexs) == 0 {
		return nil, errors.ErrNoCriteria
	}
	if err := r.rewriteRoutePlan(plan, stmt); err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildRouteUpdatePlan(db string, stmt *RouteStatement, args []interface{}) (*Plan, error) {
	plan := &Plan{Rule: r.GetRule(db, stmt.Table.LookupName(db))}
	if plan.Rule.Type == DefaultRuleType {
		return r.buildPassthroughPlan(plan.Rule, stmt), nil
	}

	if routeTouchesShardKey(plan.Rule, stmt.UpdateColumns) {
		return nil, errors.ErrUpdateKey
	}

	indexes, err := routeTableIndexes(plan.Rule, stmt.Table, stmt.Condition, args)
	if err != nil {
		return nil, err
	}
	if stmt.RequiresSingleShard && len(indexes) > 1 {
		return r.buildFallbackPlan(db, stmt), nil
	}

	plan.RouteTableIndexs = indexes
	plan.RouteNodeIndexs = routeNodeIndexes(plan.Rule, indexes)
	if len(plan.RouteTableIndexs) == 0 {
		return nil, errors.ErrNoCriteria
	}
	if err := r.rewriteRoutePlan(plan, stmt); err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildRouteDeletePlan(db string, stmt *RouteStatement, args []interface{}) (*Plan, error) {
	plan := &Plan{Rule: r.GetRule(db, stmt.Table.LookupName(db))}
	if plan.Rule.Type == DefaultRuleType {
		return r.buildPassthroughPlan(plan.Rule, stmt), nil
	}

	indexes, err := routeTableIndexes(plan.Rule, stmt.Table, stmt.Condition, args)
	if err != nil {
		return nil, err
	}
	if stmt.RequiresSingleShard && len(indexes) > 1 {
		return r.buildFallbackPlan(db, stmt), nil
	}

	plan.RouteTableIndexs = indexes
	plan.RouteNodeIndexs = routeNodeIndexes(plan.Rule, indexes)
	if len(plan.RouteTableIndexs) == 0 {
		return nil, errors.ErrNoCriteria
	}
	if err := r.rewriteRoutePlan(plan, stmt); err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildRouteTruncatePlan(db string, stmt *RouteStatement) (*Plan, error) {
	plan := &Plan{Rule: r.GetRule(db, stmt.Table.LookupName(db))}
	if plan.Rule.Type == DefaultRuleType {
		return r.buildPassthroughPlan(plan.Rule, stmt), nil
	}

	plan.RouteTableIndexs = append([]int(nil), plan.Rule.SubTableIndexs...)
	plan.RouteNodeIndexs = routeNodeIndexes(plan.Rule, plan.RouteTableIndexs)
	if err := r.rewriteRoutePlan(plan, stmt); err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildPassthroughPlan(rule *Rule, stmt *RouteStatement) *Plan {
	return &Plan{
		Rule:            rule,
		RouteNodeIndexs: []int{0},
		RewrittenSqls: map[string][]string{
			rule.Nodes[0]: {routeOriginalSQL(stmt)},
		},
	}
}

func routeOriginalSQL(stmt *RouteStatement) string {
	if stmt == nil {
		return ""
	}
	if stmt.OriginalSQL != "" {
		return stmt.OriginalSQL
	}
	if stmt.MySQLStatement() == nil {
		return ""
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.MySQLStatement().Format(buf)
	return buf.String()
}

func routeInsertKeyIndex(rule *Rule, columns []string) int {
	for i, column := range columns {
		if strings.EqualFold(column, rule.Key) {
			return i
		}
	}
	return -1
}

func routeTouchesShardKey(rule *Rule, columns []string) bool {
	if rule.Type == DefaultRuleType || len(rule.Nodes) == 1 {
		return false
	}
	for _, column := range columns {
		if strings.EqualFold(column, rule.Key) {
			return true
		}
	}
	return false
}

func routeTableIndexes(rule *Rule, table RouteTable, condition RouteCondition, args []interface{}) ([]int, error) {
	if rule.Type == DefaultRuleType {
		return nil, nil
	}
	if condition == nil {
		return append([]int(nil), rule.SubTableIndexs...), nil
	}
	return routeIndexesByCondition(rule, table, condition, args)
}

func routeIndexesByCondition(rule *Rule, table RouteTable, condition RouteCondition, args []interface{}) ([]int, error) {
	switch typed := condition.(type) {
	case *RouteBooleanCondition:
		left, err := routeIndexesByCondition(rule, table, typed.Left, args)
		if err != nil {
			return nil, err
		}
		right, err := routeIndexesByCondition(rule, table, typed.Right, args)
		if err != nil {
			return nil, err
		}
		switch typed.Operator {
		case "and":
			return interList(left, right), nil
		case "or":
			return unionList(left, right), nil
		default:
			return append([]int(nil), rule.SubTableIndexs...), nil
		}
	case *RouteComparisonCondition:
		return routeIndexesByComparison(rule, table, typed, args)
	case *RouteRangeCondition:
		return routeIndexesByRange(rule, table, typed, args)
	default:
		return append([]int(nil), rule.SubTableIndexs...), nil
	}
}

func routeIndexesByComparison(rule *Rule, table RouteTable, comparison *RouteComparisonCondition, args []interface{}) ([]int, error) {
	if comparison == nil {
		return append([]int(nil), rule.SubTableIndexs...), nil
	}

	switch strings.ToLower(comparison.Operator) {
	case "=", "<=>":
		if routeExprKind(rule, table, comparison.Left, args) == EID_NODE && routeExprKind(rule, table, comparison.Right, args) == VALUE_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		}
		if routeExprKind(rule, table, comparison.Left, args) == VALUE_NODE && routeExprKind(rule, table, comparison.Right, args) == EID_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Left, args)
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		}
	case "<", "<=", ">", ">=":
		return routeIndexesByOrderedComparison(rule, table, comparison, args)
	case "in", "not in":
		leftKind := routeExprKind(rule, table, comparison.Left, args)
		rightKind := routeExprKind(rule, table, comparison.Right, args)
		if leftKind == EID_NODE && rightKind == LIST_NODE {
			indexes, err := routeIndexesByTuple(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			if strings.EqualFold(comparison.Operator, "not in") && (rule.Type == DateDayRuleType || rule.Type == DateMonthRuleType || rule.Type == DateYearRuleType) {
				return differentList(rule.SubTableIndexs, indexes), nil
			}
			if strings.EqualFold(comparison.Operator, "not in") {
				return append([]int(nil), rule.SubTableIndexs...), nil
			}
			return indexes, nil
		}
	}

	return append([]int(nil), rule.SubTableIndexs...), nil
}

func routeIndexesByOrderedComparison(rule *Rule, table RouteTable, comparison *RouteComparisonCondition, args []interface{}) ([]int, error) {
	leftKind := routeExprKind(rule, table, comparison.Left, args)
	rightKind := routeExprKind(rule, table, comparison.Right, args)

	switch rule.Type {
	case HashRuleType:
		return append([]int(nil), rule.SubTableIndexs...), nil
	case RangeRuleType:
		return routeRangeShardByComparison(rule, leftKind, rightKind, comparison, args)
	case DateDayRuleType, DateMonthRuleType, DateYearRuleType:
		return routeDateShardByComparison(rule, leftKind, rightKind, comparison, args)
	default:
		return append([]int(nil), rule.SubTableIndexs...), nil
	}
}

func routeIndexesByRange(rule *Rule, table RouteTable, rangeCond *RouteRangeCondition, args []interface{}) ([]int, error) {
	leftKind := routeExprKind(rule, table, rangeCond.Left, args)
	fromKind := routeExprKind(rule, table, rangeCond.From, args)
	toKind := routeExprKind(rule, table, rangeCond.To, args)
	if leftKind != EID_NODE || fromKind != VALUE_NODE || toKind != VALUE_NODE {
		return append([]int(nil), rule.SubTableIndexs...), nil
	}

	switch rule.Type {
	case HashRuleType:
		return append([]int(nil), rule.SubTableIndexs...), nil
	case RangeRuleType:
		start, err := routeTableIndexByExpr(rule, rangeCond.From, args)
		if err != nil {
			return nil, err
		}
		last, err := routeTableIndexByExpr(rule, rangeCond.To, args)
		if err != nil {
			return nil, err
		}
		if last < start {
			start, last = last, start
		}
		if strings.EqualFold(rangeCond.Operator, "between") {
			return makeList(start, last+1), nil
		}
		start = adjustRangeShardIndex(rule, rangeCond.From, start, args)
		return unionList(makeList(0, start+1), makeList(last, len(rule.SubTableIndexs))), nil
	case DateDayRuleType, DateMonthRuleType, DateYearRuleType:
		start, err := routeTableIndexByExpr(rule, rangeCond.From, args)
		if err != nil {
			return nil, err
		}
		last, err := routeTableIndexByExpr(rule, rangeCond.To, args)
		if err != nil {
			return nil, err
		}
		if last < start {
			start, last = last, start
		}
		if strings.EqualFold(rangeCond.Operator, "between") {
			return makeBetweenList(start, last, rule.SubTableIndexs), nil
		}
		return differentList(rule.SubTableIndexs, makeBetweenList(start, last, rule.SubTableIndexs)), nil
	default:
		return append([]int(nil), rule.SubTableIndexs...), nil
	}
}

func routeRangeShardByComparison(rule *Rule, leftKind, rightKind int, comparison *RouteComparisonCondition, args []interface{}) ([]int, error) {
	switch comparison.Operator {
	case "<", "<=":
		if leftKind == EID_NODE && rightKind == VALUE_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			if comparison.Operator == "<" {
				index = adjustRangeShardIndex(rule, comparison.Right, index, args)
			}
			return makeList(0, index+1), nil
		}
		if leftKind == VALUE_NODE && rightKind == EID_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Left, args)
			if err != nil {
				return nil, err
			}
			return makeList(index, len(rule.SubTableIndexs)), nil
		}
	case ">", ">=":
		if leftKind == EID_NODE && rightKind == VALUE_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			return makeList(index, len(rule.SubTableIndexs)), nil
		}
		if leftKind == VALUE_NODE && rightKind == EID_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Left, args)
			if err != nil {
				return nil, err
			}
			if comparison.Operator == ">" {
				index = adjustRangeShardIndex(rule, comparison.Left, index, args)
			}
			return makeList(0, index+1), nil
		}
	}
	return append([]int(nil), rule.SubTableIndexs...), nil
}

func routeDateShardByComparison(rule *Rule, leftKind, rightKind int, comparison *RouteComparisonCondition, args []interface{}) ([]int, error) {
	switch comparison.Operator {
	case "<", "<=":
		if leftKind == EID_NODE && rightKind == VALUE_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			return makeLeList(index, rule.SubTableIndexs), nil
		}
		if leftKind == VALUE_NODE && rightKind == EID_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Left, args)
			if err != nil {
				return nil, err
			}
			return makeGeList(index, rule.SubTableIndexs), nil
		}
	case ">", ">=":
		if leftKind == EID_NODE && rightKind == VALUE_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Right, args)
			if err != nil {
				return nil, err
			}
			return makeGeList(index, rule.SubTableIndexs), nil
		}
		if leftKind == VALUE_NODE && rightKind == EID_NODE {
			index, err := routeTableIndexByExpr(rule, comparison.Left, args)
			if err != nil {
				return nil, err
			}
			return makeLeList(index, rule.SubTableIndexs), nil
		}
	}
	return append([]int(nil), rule.SubTableIndexs...), nil
}

func routeExprKind(rule *Rule, table RouteTable, expr RouteExpr, args []interface{}) int {
	switch typed := routeUnwrapExpr(expr).(type) {
	case *RouteColumnExpr:
		if typed != nil && strings.EqualFold(typed.Name, rule.Key) && table.MatchesQualifier(typed.Qualifier) {
			return EID_NODE
		}
	case *RouteValueExpr:
		if typed != nil {
			if _, ok := typed.Value.Resolve(args); ok {
				return VALUE_NODE
			}
		}
	case *RouteListExpr:
		if typed == nil {
			return OTHER_NODE
		}
		for _, item := range typed.Items {
			if routeExprKind(rule, table, item, args) != VALUE_NODE {
				return OTHER_NODE
			}
		}
		return LIST_NODE
	}
	return OTHER_NODE
}

func routeUnwrapExpr(expr RouteExpr) RouteExpr {
	for {
		typeCast, ok := expr.(*RouteTypeCastExpr)
		if !ok || typeCast == nil {
			return expr
		}
		expr = typeCast.Expr
	}
}

func routeTableIndexByExpr(rule *Rule, expr RouteExpr, args []interface{}) (int, error) {
	valueExpr, ok := routeUnwrapExpr(expr).(*RouteValueExpr)
	if !ok || valueExpr == nil {
		return 0, fmt.Errorf("route expression %T is not a value", expr)
	}
	value, ok := valueExpr.Value.Resolve(args)
	if !ok {
		return 0, fmt.Errorf("route parameter %d is not bound", valueExpr.Value.ParamIndex)
	}
	return rule.FindTableIndex(value)
}

func routeIndexesByTuple(rule *Rule, expr RouteExpr, args []interface{}) ([]int, error) {
	listExpr, ok := routeUnwrapExpr(expr).(*RouteListExpr)
	if !ok || listExpr == nil {
		return nil, fmt.Errorf("route expression %T is not a value list", expr)
	}

	indexes := make([]int, 0, len(listExpr.Items))
	for _, item := range listExpr.Items {
		index, err := routeTableIndexByExpr(rule, item, args)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	indexes = cleanList(indexes)
	sort.Ints(indexes)
	return indexes, nil
}

func routeInsertIndexes(rule *Rule, rows []RouteInsertRow, keyIndex int, args []interface{}) ([]int, map[int][]int, error) {
	indexes := make([]int, 0, len(rows))
	rowGroups := make(map[int][]int)
	for i, row := range rows {
		if keyIndex >= len(row.Values) {
			return nil, nil, errors.ErrColsLenNotMatch
		}
		value, ok := row.Values[keyIndex].Resolve(args)
		if !ok {
			return nil, nil, fmt.Errorf("route parameter %d is not bound", row.Values[keyIndex].ParamIndex)
		}
		index, err := rule.FindTableIndex(value)
		if err != nil {
			return nil, nil, err
		}
		indexes = append(indexes, index)
		rowGroups[index] = append(rowGroups[index], i)
	}
	indexes = cleanList(indexes)
	sort.Ints(indexes)
	return indexes, rowGroups, nil
}

func routeNodeIndexes(rule *Rule, tableIndexes []int) []int {
	if len(tableIndexes) == 0 {
		return nil
	}
	nodes := make([]int, 0, len(tableIndexes))
	for _, tableIndex := range tableIndexes {
		nodes = append(nodes, rule.TableToNode[tableIndex])
	}
	nodes = cleanList(nodes)
	sort.Ints(nodes)
	return nodes
}

func adjustRangeShardIndex(rule *Rule, expr RouteExpr, index int, args []interface{}) int {
	valueExpr, ok := routeUnwrapExpr(expr).(*RouteValueExpr)
	if !ok || valueExpr == nil {
		return index
	}
	value, ok := valueExpr.Value.Resolve(args)
	if !ok {
		return index
	}
	shard, ok := rule.Shard.(RangeShard)
	if !ok {
		return index
	}
	if shard.EqualStart(value, index) {
		index--
		if index < 0 {
			index = 0
		}
	}
	return index
}

func (r *Router) rewriteRoutePlan(plan *Plan, stmt *RouteStatement) error {
	if stmt == nil {
		return errors.ErrNoPlan
	}
	if stmt.Dialect != RouteDialectPostgres {
		return nil
	}

	sqls := make(map[string][]string)
	if len(plan.RouteTableIndexs) == 0 {
		sqls[plan.Rule.Nodes[0]] = []string{routeOriginalSQL(stmt)}
		plan.RewrittenSqls = sqls
		return nil
	}

	for _, tableIndex := range plan.RouteTableIndexs {
		sql, err := rewritePostgresSQL(stmt, plan.Rule, tableIndex, plan.RouteRowIndexGroups[tableIndex])
		if err != nil {
			return err
		}
		nodeName := plan.Rule.Nodes[plan.Rule.TableToNode[tableIndex]]
		sqls[nodeName] = append(sqls[nodeName], sql)
	}
	plan.RewrittenSqls = sqls
	return nil
}

func rewritePostgresSQL(stmt *RouteStatement, rule *Rule, tableIndex int, rowIndexes []int) (string, error) {
	if stmt == nil || stmt.PostgresTree() == nil || len(stmt.PostgresTree().Stmts) == 0 {
		return "", fmt.Errorf("postgres route statement has no parse tree")
	}

	cloned, ok := proto.Clone(stmt.PostgresTree()).(*pg_query.ParseResult)
	if !ok || cloned == nil || len(cloned.Stmts) == 0 || cloned.Stmts[0] == nil || cloned.Stmts[0].Stmt == nil {
		return "", fmt.Errorf("failed to clone postgres parse tree")
	}
	raw := cloned.Stmts[0].Stmt
	physicalName := fmt.Sprintf("%s_%04d", rule.Table, tableIndex)

	switch stmt.Kind {
	case RouteStatementSelect:
		raw.GetSelectStmt().GetFromClause()[0].GetRangeVar().Relname = physicalName
	case RouteStatementInsert:
		insert := raw.GetInsertStmt()
		insert.GetRelation().Relname = physicalName
		valuesStmt := insert.GetSelectStmt().GetSelectStmt()
		if len(rowIndexes) == 0 {
			return "", fmt.Errorf("postgres insert rewrite for table %d has no row group", tableIndex)
		}
		filtered := make([]*pg_query.Node, 0, len(rowIndexes))
		for _, rowIndex := range rowIndexes {
			if rowIndex < 0 || rowIndex >= len(valuesStmt.ValuesLists) {
				return "", fmt.Errorf("postgres insert row index %d is out of range", rowIndex)
			}
			filtered = append(filtered, valuesStmt.ValuesLists[rowIndex])
		}
		valuesStmt.ValuesLists = filtered
	case RouteStatementUpdate:
		raw.GetUpdateStmt().GetRelation().Relname = physicalName
	case RouteStatementDelete:
		raw.GetDeleteStmt().GetRelation().Relname = physicalName
	case RouteStatementTruncate:
		raw.GetTruncateStmt().GetRelations()[0].GetRangeVar().Relname = physicalName
	default:
		return routeOriginalSQL(stmt), nil
	}

	return deparsePostgresQuery(cloned)
}
