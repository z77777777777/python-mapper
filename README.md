# python-mapper

[English](README.md) · [简体中文](README.zh-CN.md)

PostgreSQL-first XML mapper runtime backed directly by asyncpg. The package is
framework-neutral: it does not import FastAPI, a host application's settings,
logging formatter, schema, or business models.

## Installation

```bash
pip install "git+https://github.com/z77777777777/python-mapper.git@v0.3.0"
```

Requires Python 3.12+. The only runtime dependencies are `asyncpg` and `Jinja2`.

## Package layout

- `database.py`: asyncpg pool configuration, startup, shutdown, and connection checkout.
- `extension.py`: application bootstrap, mapper-package scanning and pool lifespan.
- `base.py`: implicit connection and transaction propagation.
- `compiler.py`: safe `:name` to `$1` compilation and collection expansion.
- `plugins.py`: generic around-execution plugin contract.
- `pagination.py`: opt-in list pagination and stable page result models.
- `observability.py`: parameter-safe structured SQL logging plugin.
- `mapping.py`: `resultType`/`resultMap`, model validation, row materialization, and
  strict 0..1 cardinality.
- `errors.py`: public framework exception hierarchy.
- `runtime.py`: current public facade plus XML loading, mapper binding, SQL rendering,
  and execution.

`runtime.py` can later be separated into `builder`, `binding`, and `executor`, but only
after its shared dictionaries and load lock are owned by a `MapperRegistry`/`Configuration`
object. Splitting those functions first would replace one cohesive module with circular
imports and shared global state spread across several files.

## Application setup

Wire everything once through the extension:

```python
from pathlib import Path

from python_mapper import PyMapperExtension, SqlLoggingPlugin

pymapper = PyMapperExtension(
    database_url="postgresql://user:password@localhost/database",
    mapper_paths=[Path(__file__).resolve().parent / "mapper"],
    mapper_packages=["app.repositories"],
    plugins=[SqlLoggingPlugin(slow_query_threshold_ms=500)],
)

# Embed this lifespan in FastAPI, Starlette, a CLI worker, or your own host.
async def run_application() -> None:
    async with pymapper.lifespan() as state:
        print(state.statement_count)
        await serve_application()
```

The extension imports every module under `mapper_packages`, loads the XML, runs
startup-time contract validation, opens the pool, and closes it on exit. Lower-level
wiring remains available through `configure()`, `load_all_mappers()` and
`open_database()`. Use `reset_state()` for test isolation instead of clearing the
registry by hand.

## Mapper declaration

```python
from python_mapper import amapper, transactional

@amapper()
class OrdersMapper:
    async def find(*, order_id: int | None = None) -> list: ...
    async def update(*, order_id: int | None = None) -> int: ...
    async def add_log(*, order_id: int | None = None) -> int: ...

@transactional()
async def update_order(order_id: int) -> None:
    await OrdersMapper.update(order_id=order_id)
    await OrdersMapper.add_log(order_id=order_id)
```

Mapper methods do not accept a connection. A direct mapper call borrows one connection
without opening an explicit transaction; PostgreSQL commits that statement as its own
transaction. Calls inside `@transactional()` reuse the task-local connection and commit or
roll back together. `REQUIRED` and `REQUIRES_NEW` propagation are supported; a `REQUIRED`
participant inherits the outer transaction's isolation level, matching Spring/MyBatis
semantics.

Jinja blocks may control SQL structure, but values must use named binds
such as `:order_id`. `{{ value }}` interpolation is rejected while loading XML.
Only `if/elif/else/endif` Jinja tags are accepted; output, include, macro, loop,
assignment and filter tags fail during mapper loading. Native asyncpg `$n` binds
are also rejected because XML statements use one binding contract: `:name`.

## Result mapping

- `resultType="module.Row"` maps by matching names: only fields the model declares as
  constructor arguments are passed in. Extra columns returned by the SQL are ignored, and
  optional fields the query did not return fall back to the model's defaults.
- `resultMap="rowMap"` declares `column -> property` explicitly. If an explicit property
  does not exist on the model, `load_all_mappers()` fails during startup validation.
- Every `resultType` path is validated after all XML has loaded, so a typo in a dotted
  path surfaces at startup rather than on the first query.
- `single="true"` is a strict 0..1 row contract: zero rows return `None`, one row returns
  the object, and more than one raises `TooManyResultsError` — it never silently takes the
  first row.

Framework exceptions live in `python_mapper.errors` and are re-exported from the package
root:

```python
from python_mapper import PyMapperError, TooManyResultsError
```

## Optional pagination

Pagination is explicit and opt-in. A plain mapper call never rewrites your SQL:

```xml
<select id="list_orders" countRef="count_orders" resultType="app.types.OrderRow">
    SELECT id, order_no, status
    FROM orders
    WHERE status = :status
    ORDER BY created_at DESC, id DESC
</select>

<select id="count_orders" expose="false">
    SELECT COUNT(*) FROM orders WHERE status = :status
</select>
```

```python
from python_mapper import Page

class OrdersMapper:
    async def list_orders(
        *, status: str | None = None, page: int = 1, page_size: int = 30,
    ) -> Page[OrderRow]: ...

page_result = await OrdersMapper.list_orders(
    page=2,
    status="active",
)
```

When the return annotation is `Page[T]`, the mapper call returns a finished, web-framework
agnostic page object whose fields are fixed as `items / total / page / page_size / pages`.
The framework puts the `list[T]` returned by the SQL into the `Page`; callers never touch
`PaginationOptions`, `QueryResult` or `PageMetadata`. Methods that do not paginate keep
declaring and returning `list[T]`.

`query(..., pagination=PaginationOptions(...))` remains as a low-level entry point, for
dynamically disabling pagination or for slice/`has_next` cases with `include_total=False`.

- `page_size` defaults to 30 and is capped at 200.
- With `enabled=False` no pagination clause is added and no count runs; if the XML
  declares `<page/>`, only that internal marker is removed before executing the
  unpaginated SQL.
- A stable top-level `ORDER BY` is required whenever pagination is enabled.
- `include_total=True` requires an explicit `countRef`. With `False`, `has_next` is
  determined by fetching one extra row and no count is executed.
- Without `<page/>` the pagination clause is appended at the end of the SQL. Use `<page/>`
  when the insertion point matters, for example with `FOR UPDATE`. The marker must sit
  after a complete top-level `ORDER BY` and before any `FOR` locking clause — never inside
  select columns, a `WHERE`, a subquery, a string, or a comment.
- A hand-written top-level `LIMIT/OFFSET/FETCH` conflicts with enabled framework
  pagination; with framework pagination disabled, hand-written pagination is preserved
  exactly. Declaring `<page/>` together with hand-written pagination fails at XML load
  time.
- This feature provides page-number `LIMIT/OFFSET` pagination. For deep paging over
  millions of rows, disable it and implement keyset (cursor) pagination explicitly in your
  own mapper — the framework will not guess your business cursor.

## SQL execution plugins and logging

Hosts configure execution plugins through `plugins=[...]`. A plugin depends only on
`StatementContext`, `StatementResult` and `StatementPlugin`, so it can serve metrics,
tracing, auditing or read-only protection without depending on any web framework.

`SqlLoggingPlugin` emits a structured `LogRecord.pymapper` by default: statement id, final
asyncpg SQL, SQL fingerprint, elapsed time, connection wait, row count, parameter names
and parameter types. It never logs parameter values, and exceptions go through
`logger.exception` so the traceback is preserved. A host can lift `record.pymapper` into
its own JSON formatter payload.

Plugin instances are process-level configuration and may be reused across concurrent
requests. Custom plugins must be stateless, or guard their own mutable state.

## Known pitfalls (read before writing code)

- **An empty expanding bind silently matches zero rows.** Passing an empty list to
  `IN :ids` renders an "empty set" expression — no error, no matching rows. `NOT IN :ids`
  with an empty list behaves the same way in reverse: nothing is excluded, so everything
  passes. When a collection can legitimately be empty, branch in the caller or give the
  parameter a non-optional default.
- **Result sets are fully materialized.** A mapper call pulls the whole result set into
  memory before mapping rows. Always paginate or `LIMIT` large sets; streaming is
  deliberately not offered, so that a cursor's lifetime is never leaked to the caller.
- **Never call a mapper from `asyncio.create_task` inside a transaction.** The child task
  would either use the same connection concurrently (asyncpg raises) or use a connection
  that has already been released. See the `transactional` docstring.
- **`:name::type` is rejected at load time.** Write `CAST(:name AS type)` instead.

## Tests

```bash
pip install -e ".[test]"
pytest
```

The package's test fixtures snapshot and restore global state, so they can run alongside a
host project's own suite without polluting the host's mapper registry.

CI runs the suite on Windows and Linux across Python 3.12 / 3.13 / 3.14. No host
application, business database or web framework takes part in that matrix.
