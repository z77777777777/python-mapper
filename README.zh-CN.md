# python-mapper

[English](https://github.com/z77777777777/python-mapper/blob/main/README.md) ·
[简体中文](https://github.com/z77777777777/python-mapper/blob/main/README.zh-CN.md)

一款轻量的 Python 异步 XML 映射工具，把 SQL 从业务代码里解离出来——查询写在 XML 里，
Python 侧只留签名，于是一个 service 方法读起来就是业务本身，而不是绕着游标拼字符串。

面向 PostgreSQL，直接跑在 asyncpg 上。包本身与框架无关：不 import FastAPI，
也不碰宿主的配置、日志 formatter、schema 或业务模型。

## 安装

```bash
pip install python-mapper
```

需要 Python 3.12+。运行时依赖只有 `asyncpg` 和 `Jinja2`。

## 只支持异步

所有执行路径都是异步的：mapper 调用、`query()`、`scalar()` 返回的都是协程，
`transactional()` 装同步函数会在**装饰期**直接 `TypeError`。这不是没做，而是底层驱动
asyncpg 本身就没有同步 API。

要在同步框架里用（Flask、传统 Django view、脚本），起一个常驻事件循环放在后台线程，
把调用提交过去：

```python
import asyncio
import threading

loop = asyncio.new_event_loop()
threading.Thread(target=loop.run_forever, daemon=True).start()

# 连接池只在这个 loop 上开一次
asyncio.run_coroutine_threadsafe(open_database(...), loop).result()

# 之后在同步代码里这样调
rows = asyncio.run_coroutine_threadsafe(
    OrdersMapper.find(order_id=1), loop,
).result()
```

**不要**给每次调用套 `asyncio.run()`。那样每次都新建一个事件循环，而 asyncpg 的连接池
绑死在创建它的那个 loop 上——结果要么报 "attached to a different loop"，要么每个请求
重建一次连接池。

## 模块划分

- `database.py`：asyncpg 连接池的配置、启动、关闭与连接借出。
- `extension.py`：应用启动接线、mapper 包扫描、连接池生命周期。
- `base.py`：隐式连接与事务传播。
- `compiler.py`：`:name` 到 `$1` 的安全编译与集合展开。
- `plugins.py`：通用的执行环绕插件契约。
- `pagination.py`：可选的列表分页与稳定的分页结果模型。
- `observability.py`：不记录参数值的结构化 SQL 日志插件。
- `mapping.py`：`resultType`/`resultMap`、模型校验、行物化、严格的 0..1 基数。
- `errors.py`：对外的框架异常层次。
- `runtime.py`：当前的公开门面，外加 XML 加载、mapper 绑定、SQL 渲染与执行。

`runtime.py` 以后可以拆成 `builder`、`binding`、`executor`，但前提是它那些共享字典和
加载锁先归一个 `MapperRegistry`/`Configuration` 对象所有。先拆函数只会把一个内聚模块
换成循环 import 加散落在多个文件里的共享全局状态。

## 应用接线

推荐用扩展完成一次性接线：

```python
from pathlib import Path

from python_mapper import PyMapperExtension, SqlLoggingPlugin

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

## Mapper 声明

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

Mapper 方法不接收连接。直接调用会借一条连接、不开显式事务，PostgreSQL 把那条语句
作为独立事务提交。`@transactional()` 内部的调用复用 task 本地的那条连接，一起提交或
一起回滚。支持 `REQUIRED` 与 `REQUIRES_NEW` 两种传播；`REQUIRED` 加入即继承外层事务的
隔离级别（与 Spring/MyBatis 同语义）。

Jinja 块可以控制 SQL 结构，但值必须走 `:order_id` 这样的具名绑参。`{{ value }}` 插值在
XML 加载期就被拒。Jinja 标签只接受 `if/elif/else/endif`；output、include、macro、循环、
赋值和 filter 标签会在 mapper 加载时失败。asyncpg 原生的 `$n` 绑参同样被拒 —— XML 语句
只有一套绑定契约：`:name`。

## 结果映射

- `resultType="module.Row"` 使用同名自动映射：只传入模型声明过的构造字段，SQL
  多返回的列直接忽略，未返回的可选字段使用模型默认值。
- `resultMap="rowMap"` 允许显式声明 `column -> property`；显式 property 在模型中
  不存在时，`load_all_mappers()` 启动校验直接失败。
- 所有 `resultType` 路径在全量 XML 加载完成后统一验证，路径拼错不会拖到首次查询。
- `single="true"` 是严格的 0..1 行契约：0 行返回 `None`，1 行返回单对象，
  多于 1 行抛出 `TooManyResultsError`，不会静默取第一行。

框架异常定义在 `python_mapper.errors`，业务代码可统一从包根导入：

```python
from python_mapper import PyMapperError, TooManyResultsError
```

## 可选分页

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

返回注解为 `Page[T]` 时，Mapper 调用直接返回与 Web 框架无关的成品分页对象，
字段固定为 `items / total / page / page_size / pages`。框架把 SQL 返回的
`list[T]` 放入 `Page`，调用方不接触 `PaginationOptions`、`QueryResult` 或
`PageMetadata`。不分页的方法继续声明并返回 `list[T]`。

`query(..., pagination=PaginationOptions(...))` 仅保留为低层入口，用于动态关闭
分页或 `include_total=False` 的 slice/`has_next` 场景。

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

## SQL 执行插件与日志

宿主通过 `plugins=[...]` 配置执行插件。插件只依赖 `StatementContext`、
`StatementResult` 和 `StatementPlugin`，可用于指标、追踪、审计或只读保护，不依赖
具体 Web 框架。

`SqlLoggingPlugin` 默认输出结构化 `LogRecord.pymapper`：statement id、最终 asyncpg
SQL、SQL fingerprint、耗时、连接等待、行数、参数名称和参数类型。它不记录参数值；
异常使用 `logger.exception` 保留 traceback。宿主可在自己的 JSON formatter 中把
`record.pymapper` 放进日志载荷。

插件实例是进程级配置，可能被并发请求复用。自定义插件必须保持无状态，或自行保护
可变状态。

## 已知坑（写代码前读一遍）

- **空集合 expanding 绑参静默匹配零行**：`IN :ids` 传空 list 时，pymapper 渲染成
  "空集"表达式 —— 不报错、匹配不到任何行。`NOT IN :ids` 传空同理会**排除不了任何行**
  （即全部通过）。集合可能为空时，调用方自己分支或给参数设不可省略的默认。
- **结果集全量物化**：mapper 调用把整个结果集一次拉进内存再做行映射。大集合一律
  LIMIT/分页，不要指望流式 —— 框架刻意不提供（避免把游标生命周期泄给调用方）。
- **事务范围内禁止 `asyncio.create_task` 调 mapper**：子任务会并发使用同一个连接
  （asyncpg 直接报错）或使用已释放的连接。见 `transactional` docstring。
- **`:name::type` 写法加载期即拒**：改写 `CAST(:name AS type)`。

## 测试

```bash
pip install -e ".[test]"
pytest
```

包测试的 fixture 是快照/还原式，可以混在宿主项目的 CI 里跑，不污染宿主 mapper 状态。

CI 在 Windows/Linux 与 Python 3.12 / 3.13 / 3.14 上跑这套测试，任何宿主业务库或
Web 框架都不参与这个矩阵。
