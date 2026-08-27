# cyt-pymapper

PostgreSQL-first XML mapper runtime backed directly by asyncpg. The package is
framework-neutral: it does not import FastAPI, a host application's settings,
logging formatter, schema, or business models.

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

推荐使用扩展完成一次性接线：

```python
from pathlib import Path

from cyt_pymapper import PyMapperExtension, SqlLoggingPlugin

pymapper = PyMapperExtension(
    database_url="postgresql://user:password@localhost/database",
    mapper_paths=[Path(__file__).resolve().parent / "mapper"],
    mapper_packages=["app.repositories"],
    plugins=[SqlLoggingPlugin(slow_query_threshold_ms=500)],
)

# 把这个生命周期嵌入 FastAPI、Starlette、CLI worker 或自有宿主。
async def run_application() -> None:
    async with pymapper.lifespan() as state:
        print(state.statement_count)
        await serve_application()
```

扩展会自动导入 `mapper_packages` 下的全部模块、加载 XML、执行启动期契约校验、
打开连接池并在退出时关闭。需要底层接线时仍可使用 `configure()`、
`load_all_mappers()` 和 `open_database()`。测试隔离用 `reset_state()`，不要手动清注册表。

## Mapper declaration

```python
from cyt_pymapper import amapper, transactional

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
roll back together. `REQUIRED` and `REQUIRES_NEW` propagation are supported;
`REQUIRED` 加入即继承外层事务的隔离级别(与 Spring/MyBatis 同语义)。

Jinja blocks may control SQL structure, but values must use named binds
such as `:order_id`. `{{ value }}` interpolation is rejected while loading XML.
Only `if/elif/else/endif` Jinja tags are accepted; output, include, macro, loop,
assignment and filter tags fail during mapper loading. Native asyncpg `$n` binds
are also rejected because XML statements use one binding contract: `:name`.

## Result mapping

- `resultType="module.Row"` 使用同名自动映射：只传入模型声明过的构造字段，SQL
  多返回的列直接忽略，未返回的可选字段使用模型默认值。
- `resultMap="rowMap"` 允许显式声明 `column -> property`；显式 property 在模型中
  不存在时，`load_all_mappers()` 启动校验直接失败。
- 所有 `resultType` 路径在全量 XML 加载完成后统一验证，路径拼错不会拖到首次查询。
- `single="true"` 是严格的 0..1 行契约：0 行返回 `None`，1 行返回单对象，
  多于 1 行抛出 `TooManyResultsError`，不会静默取第一行。

框架异常定义在 `cyt_pymapper.errors`，业务代码可统一从包根导入：

```python
from cyt_pymapper import PyMapperError, TooManyResultsError
```

## Optional pagination

分页是显式、可插拔能力。普通 Mapper 调用永远不自动加工 SQL：

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
from cyt_pymapper import PaginationOptions, query

page_result = await query(
    OrdersMapper.list_orders,
    pagination=PaginationOptions(enabled=True, page_number=2),
    status="active",
)
```

- 不配置 `page_size` 时默认 30，最大 200。
- `enabled=False` 时不添加分页子句、不 count；若 XML 声明了 `<page/>`，只移除
  这个内部插入标记后执行未分页 SQL。
- 开启分页时必须有稳定的顶层 `ORDER BY`。
- `include_total=True` 需要显式 `countRef`；设为 `False` 时通过多取一行判断
  `has_next`，不执行 count。
- 不写 `<page/>` 时分页子句追加到 SQL 末尾；`FOR UPDATE` 等需要指定插入位置时
  使用 `<page/>`。标记必须位于完整的顶层 `ORDER BY` 子句之后、可选的 `FOR`
  锁定子句之前，不能放进 SELECT 列、WHERE、子查询、字符串或注释。
- 手写顶层 `LIMIT/OFFSET/FETCH` 与开启的框架分页冲突；关闭框架分页时手写分页
  完全保留。`<page/>` 与手写分页同时出现会在 XML 加载期失败。
- 该能力提供页码式 `LIMIT/OFFSET` 分页；百万级深翻页应关闭插件，在业务 Mapper
  中显式实现基于稳定排序键的游标分页，框架不会猜测宿主的业务游标。

## SQL execution plugins and logging

宿主通过 `plugins=[...]` 配置执行插件。插件只依赖 `StatementContext`、
`StatementResult` 和 `StatementPlugin`，可用于指标、追踪、审计或只读保护，不依赖
具体 Web 框架。

`SqlLoggingPlugin` 默认输出结构化 `LogRecord.pymapper`：statement id、最终 asyncpg
SQL、SQL fingerprint、耗时、连接等待、行数、参数名称和参数类型。它不记录参数值；
异常使用 `logger.exception` 保留 traceback。宿主可在自己的 JSON formatter 中把
`record.pymapper` 放进日志载荷。

插件实例是进程级配置，可能被并发请求复用。自定义插件必须保持无状态，或自行保护
可变状态。

## 已知坑(写代码前读一遍)

- **空集合 expanding 绑参静默匹配零行**: `IN :ids` 传空 list 时, pymapper 渲染成
  "空集"表达式 —— 不报错、匹配不到任何行。`NOT IN :ids` 传空同理会**排除不了任何行
  之外的东西**(即全部通过)。集合可能为空时, 调用方自己分支或给参数设不可省略的默认。
- **结果集全量物化**: mapper 调用把整个结果集一次拉进内存再做行映射。大集合一律
  LIMIT/分页, 不要指望流式 —— 框架刻意不提供(避免把游标生命周期泄给调用方)。
- **事务范围内禁止 `asyncio.create_task` 调 mapper**: 子任务会并发使用同一个连接
  (asyncpg 直接报错)或使用已释放的连接。见 `transactional` docstring。
- **`:name::type` 写法加载期即拒**: 改写 `CAST(:name AS type)`。

## Tests

```bash
pip install -e ".[test]"
pytest packages/cyt-pymapper/tests
```

包测试的 fixture 是快照/还原式, 可以混在宿主项目的 CI 里跑, 不污染宿主 mapper 状态。
仓库 CI 会在 Windows/Linux 与 Python 3.12/3.13/3.14 上独立运行包测试；
任何宿主业务库或 Web 框架都不参与这个矩阵。
