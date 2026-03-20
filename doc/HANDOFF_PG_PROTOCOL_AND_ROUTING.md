# PostgreSQL Proxy Handoff

Last updated: 2026-03-20
Branch: `codex/kingshard-pg`

## Scope

This handoff covers the ongoing work to extend `kingshard` from a MySQL-only proxy into a proxy that can also speak the PostgreSQL frontend protocol and, in phases, support PostgreSQL backend connections plus sharding/routing.

The current focus has been:

1. Decouple frontend/backend/protocol concerns from the old MySQL-only path.
2. Add a PostgreSQL frontend protocol skeleton and working execution path.
3. Add a PostgreSQL backend driver.
4. Preserve exact PostgreSQL native OID metadata for `Describe`/prepared statements.
5. Start decoupling the router from the old MySQL AST and add a PostgreSQL route adapter with fallback-to-default-node behavior.

## Current status

### 1. Frontend/backend decoupling is in place

- Backend driver abstraction exists and supports `backend_type`.
- Frontend protocol abstraction exists and supports `frontend_type`.
- Session execution logic has been pulled out of the old MySQL connection lifecycle.

Relevant files:

- `backend/driver.go`
- `backend/node.go`
- `backend/backend_conn.go`
- `proxy/server/frontend.go`
- `proxy/server/frontend_mysql.go`
- `proxy/server/session.go`

### 2. PostgreSQL protocol path exists

The proxy can now speak PostgreSQL protocol on the frontend side.

Implemented:

- startup/auth handshake
- simple query path
- parse/bind/describe/execute/sync skeleton and working minimum flow
- PostgreSQL message encoding for row descriptions, data rows, command complete, ready-for-query

Relevant files:

- `proxy/server/frontend_postgres.go`
- `proxy/server/frontend_postgres_test.go`
- `proxy/server/frontend_postgres_integration_test.go`

Important caveats:

- frontend auth is still cleartext-password style, not SCRAM/MD5/SSL
- not production-ready
- many PostgreSQL dialect features still intentionally unsupported

### 3. PostgreSQL backend driver exists

There is now a real PostgreSQL backend driver path instead of only a stub.

Implemented:

- connect/open
- execute/query
- transactions
- database switch via reconnect
- metadata/prepare support

Relevant files:

- `backend/postgres_driver.go`
- `backend/postgres_conn.go`
- `backend/postgres_driver_test.go`
- `backend/postgres_integration_test.go`

### 4. Native PostgreSQL OID metadata is preserved

Prepared statement metadata and row descriptions now prefer native server metadata.

Implemented:

- exact `ParamOIDs`
- exact `FieldDescription`
- preservation of table OID / attribute number / type OID / type size / modifier / format

Relevant files:

- `backend/postgres_conn.go`
- `mysql/field.go`
- `proxy/server/frontend_postgres.go`

Important caveat:

- native describe currently uses a dedicated metadata connection, so temp tables and some session-local state still form a boundary

### 5. Router work has started

This is the newest part of the work.

Implemented:

- a unified route AST independent from the old MySQL parser AST
- a MySQL route adapter
- a PostgreSQL route adapter based on `pg_query_go`
- schema-level fallback node support for unsupported PostgreSQL SQL
- an initial PostgreSQL route planner/rewriter for a small SQL subset
- server-side hook so PostgreSQL queries can route through the new planner when the old MySQL parser path does not apply

Relevant files:

- `proxy/router/route_ast.go`
- `proxy/router/route_parser.go`
- `proxy/router/route_plan.go`
- `proxy/router/postgres_parser_cgo.go`
- `proxy/router/postgres_parser_nocgo.go`
- `proxy/server/conn_route.go`
- `proxy/server/conn_query.go`
- `proxy/server/frontend_postgres.go`
- `config/config.go`

## PostgreSQL routing subset currently intended

The intended routing subset is deliberately narrow.

Supported shape:

- single-table `SELECT`
- single-table `INSERT ... VALUES`
- single-table `UPDATE`
- single-table `DELETE`
- single-table `TRUNCATE`
- shard key predicates using:
  - `=`
  - `IN`
  - `BETWEEN`
  - `< <= > >=`
- bind parameters like `$1`, `$2`

Fallback behavior:

- unsupported PostgreSQL SQL should go to schema `fallback`
- if `fallback` is not configured, it falls back to schema `default`

Examples that should fallback instead of shard-routing:

- `WITH`
- `JOIN`
- `ON CONFLICT`
- `UPDATE ... FROM`
- `DELETE ... USING`
- set operations
- complex subqueries

## Current environment limitation

The PostgreSQL route adapter is written against `github.com/pganalyze/pg_query_go/v6`, but this local machine currently has:

- `CGO_ENABLED=0`
- no `gcc` toolchain available

Because of that:

- the non-cgo build compiles cleanly
- the non-cgo route path returns a clear error that PostgreSQL route parsing requires cgo
- cgo-specific tests were added, but they cannot run on this machine until a cgo-capable toolchain is available

This is intentional so the repo still builds in the current environment without breaking other work.

## Validation already run

Commands that passed during this handoff stage:

```powershell
go test ./proxy/router
go test ./proxy/server -run ^$
go test ./backend -run ^$
```

Additional targeted tests for previous PostgreSQL protocol/backend work were also run earlier in this branch, including:

- PostgreSQL frontend protocol unit tests
- PostgreSQL backend unit tests
- integration tests that auto-skip when no PG DSN is available

## New config behavior

Schema config now supports:

```yaml
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
```

Meaning:

- `default` is the normal unsharded/default rule node
- `fallback` is where unsupported PostgreSQL SQL should be sent
- if `fallback` is omitted, it uses `default`

## Files changed in this latest routing stage

Modified:

- `config/config.go`
- `go.mod`
- `go.sum`
- `proxy/router/list_test.go`
- `proxy/router/planbuilder.go`
- `proxy/router/router.go`
- `proxy/server/conn_query.go`
- `proxy/server/frontend_postgres.go`

Added:

- `proxy/router/route_ast.go`
- `proxy/router/route_parser.go`
- `proxy/router/route_plan.go`
- `proxy/router/postgres_parser_cgo.go`
- `proxy/router/postgres_parser_nocgo.go`
- `proxy/router/route_plan_test.go`
- `proxy/router/route_postgres_cgo_test.go`
- `proxy/router/route_postgres_nocgo_test.go`
- `proxy/server/conn_route.go`

There are also earlier branch changes outside this latest routing step in:

- `backend/*`
- `proxy/server/frontend*.go`
- `proxy/server/session.go`
- `mysql/field.go`

## Recommended next steps

### Immediate next step

Get a cgo-capable environment and run the PostgreSQL route parser path for real.

Checklist:

1. Install a working C toolchain on the next machine.
2. Ensure `CGO_ENABLED=1`.
3. Run:

```powershell
go test ./proxy/router
```

4. Specifically verify:

```powershell
go test ./proxy/router -run Postgres
```

### After cgo is available

The next engineering steps should be:

1. Verify `BuildPlanSQL(..., RouteDialectPostgres, ...)` on real PostgreSQL subset queries.
2. Expand PostgreSQL SQL rewrite coverage for:
   - quoted identifiers
   - qualified column references
   - `RETURNING`
3. Route PostgreSQL prepared statements with bind args through the new planner end-to-end and verify shard selection.
4. Add integration tests for:
   - `psql` or `pgconn` -> proxy -> real PG backend
   - routed single-shard queries
   - fallback queries
5. Decide whether PostgreSQL aggregated multi-shard `SELECT` should reuse old MySQL result-merging logic or get a PostgreSQL-specific merge policy.

### Important design constraint

Do not let PostgreSQL raw parse trees leak into the planner core.

Keep this layering:

1. `pg_query_go` gives syntax truth.
2. PostgreSQL adapter maps only the supported subset into route AST.
3. Unsupported constructs mark the statement as fallback.
4. Planner operates on route AST, not on PostgreSQL parse-tree internals.

## Things to watch closely

- `proxy/server/conn_route.go` currently merges routed resultsets in a simple append-only way.
  This is fine for the current narrow PG subset, but not enough for all future multi-shard semantics.
- `proxy/server/frontend_postgres.go` now has a mixed path:
  - old MySQL parser path when parse succeeds
  - new PostgreSQL route path when the old parse path does not apply
  This should eventually be made more explicit and less transitional.
- fallback routing is a deliberate product behavior here, not a parser failure.
  Keep that distinction clear in tests and logs.

## Suggested resume point

If resuming on another machine, start here:

1. Open `proxy/router/route_parser.go`
2. Open `proxy/router/route_plan.go`
3. Open `proxy/server/conn_route.go`
4. Open `proxy/server/frontend_postgres.go`
5. Run `go test ./proxy/router`
6. Then enable cgo and verify the PostgreSQL parser path

## Short summary

The repository now has:

- PostgreSQL frontend/backend protocol foundations
- exact native OID metadata support
- the first real route-AST decoupling step
- a PostgreSQL route adapter/fallback mechanism

What is not finished yet is the cgo-enabled execution of the PostgreSQL parser path and broader PostgreSQL SQL rewrite/routing coverage.
