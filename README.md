# cyt-pymapper

Internal PostgreSQL-first XML mapper runtime backed directly by asyncpg.

## Package layout

- `database.py`: asyncpg pool configuration, startup, shutdown, and connection checkout.
- `extension.py`: application bootstrap, mapper-package scanning and pool lifespan.
- `base.py`: implicit connection and transaction propagation.
- `compiler.py`: safe `:name` to `$1` compilation and collection expansion.
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
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from cyt_pymapper import PyMapperExtension

pymapper = PyMapperExtension(
    database_url="postgresql://user:password@localhost/database",
    mapper_paths=[Path(__file__).resolve().parent / "mapper"],
    mapper_packages=["app.repositories"],
)

# 包本身不依赖 FastAPI，同一个 lifespan 也可嵌入其他 ASGI 框架。
@asynccontextmanager
async def lifespan(app):
    async with pymapper.lifespan() as state:
        print(state.statement_count)
        yield

app = FastAPI(lifespan=lifespan)
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
