package server

import (
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/proxy/router"
)

func (c *ClientConn) handleRoutedQuery(sql, dialect string, args []interface{}) error {
	plan, routed, err := c.schema.rule.BuildPlanSQL(c.db, sql, dialect, args)
	if err != nil {
		return err
	}

	fromSlave := routed.Kind == router.RouteStatementSelect
	conns, err := c.getShardConns(fromSlave, plan)
	defer c.closeShardConns(conns, err != nil)
	if err != nil {
		golog.Error("ClientConn", "handleRoutedQuery", err.Error(), c.connectionId)
		return err
	}
	if conns == nil {
		if routed.ReturnsRows || routed.Kind == router.RouteStatementSelect {
			return c.writeResultset(c.status, &mysql.Resultset{})
		}
		return c.writeOK(nil)
	}

	rs, err := c.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	if err != nil {
		golog.Error("ClientConn", "handleRoutedQuery", err.Error(), c.connectionId)
		return err
	}

	if routed.Kind == router.RouteStatementSelect || routed.ReturnsRows {
		return c.mergeRoutedResultset(rs)
	}
	return c.mergeExecResult(rs)
}

func (c *ClientConn) mergeRoutedResultset(rs []*mysql.Result) error {
	var merged *mysql.Resultset
	status := c.status

	for _, result := range rs {
		if result == nil {
			continue
		}
		status |= result.Status
		if result.Resultset == nil {
			continue
		}
		if merged == nil {
			merged = result.Resultset
			continue
		}
		merged.Values = append(merged.Values, result.Resultset.Values...)
		merged.RowDatas = append(merged.RowDatas, result.Resultset.RowDatas...)
	}

	if merged == nil {
		return c.writeOK(nil)
	}
	return c.writeResultset(status, merged)
}
