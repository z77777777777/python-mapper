"""框架自带回归: 事务语义 + 加载期契约(注入/绑参/XML 校验)。

这批测试是包的免疫系统, 必须随包走(而不是留在某个消费项目里):
- 事务边界: 直调自管短事务 / REQUIRED 共享回滚 / REQUIRES_NEW 独立提交
- 加载期契约: {{ 值 }} 注入拦截 / :name::type 拦截 / 绑参契约 / XML id 校验
- asyncpg 位置参数编译与 SQL 词法边界，确保字符串、注释和 PostgreSQL cast 不被误判。
"""
from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass

import pytest
from cyt_pymapper import (
    PaginationConflictError,
    PaginationError,
    PaginationOptions,
    PyMapperExtension,
    QueryResult,
    SqlLoggingPlugin,
    TooManyResultsError,
    amapper,
    bind_connection,
    configure,
    current_connection,
    database,
    load_all_mappers,
    load_mapper,
    query,
    reset_state,
    runtime,
    transactional,
)
from cyt_pymapper import mapping as result_mapping
from cyt_pymapper.compiler import (
    compile_query,
    contains_top_level_keyword,
    named_parameter_names,
    positional_parameter_numbers,
    sql_token_parenthesis_depths,
)
from cyt_pymapper.errors import TooManyResultsError as ErrorsModuleTooManyResultsError
from cyt_pymapper.runtime import _reject_interpolation

# 快照/还原要覆盖的全部注册表 —— 与 reset_state 清的范围一致。
# fixture 用"快照→reset→跑→reset→还原"而不是清空了事: 这样本测试文件可以混在
# 消费项目的 CI 里跑(例如把 packages/*/tests 加进 testpaths), 跑完宿主的
# mapper 状态原样回来, 不会把后面的业务测试炸成"未配置"。
_RUNTIME_REGISTRY_NAMES = (
    "_SQL_CONTAINER", "_NS_FILE", "_JINJA_VARS", "_NS_METHODS", "_BIND_VARS",
    "_METHOD_PARAMS", "_STATEMENT_KINDS", "_PAGINATION_SPECS",
)
_MAPPING_REGISTRY_NAMES = (
    "_RESULT_SPEC", "_RESULT_MAP_DEFS", "_TYPE_CACHE",
)


@dataclass(frozen=True, slots=True)
class AutoMappedRow:
    row_id: int
    name: str
    optional_label: str = "default-label"


@pytest.fixture(autouse=True)
def isolated_framework_state():
    saved_runtime_registries = {
        name: dict(getattr(runtime, name)) for name in _RUNTIME_REGISTRY_NAMES
    }
    saved_mapping_registries = {
        name: dict(getattr(result_mapping, name)) for name in _MAPPING_REGISTRY_NAMES
    }
    saved_paths = runtime._MAPPER_PATHS
    saved_internal_ids = set(runtime._INTERNAL_IDS)
    saved_plugins = runtime._PLUGINS
    saved_fully_loaded = runtime._FULLY_LOADED
    saved_database_state = (
        database._DATABASE_CONFIG,
        database._POOL,
        database._POOL_OWNED,
        database._POOL_FACTORY,
    )
    # Package tests may run inside a host application's test suite while its real
    # pool is open. Detach that state without closing it, then restore it verbatim.
    database._DATABASE_CONFIG = None
    database._POOL = None
    database._POOL_OWNED = False
    database._POOL_FACTORY = database.asyncpg.create_pool
    reset_state()
    yield
    reset_state()
    database.clear_database_configuration()
    for name, snapshot in saved_runtime_registries.items():
        getattr(runtime, name).update(snapshot)
    for name, snapshot in saved_mapping_registries.items():
        getattr(result_mapping, name).update(snapshot)
    runtime._MAPPER_PATHS = saved_paths
    runtime._INTERNAL_IDS.update(saved_internal_ids)
    runtime._PLUGINS = saved_plugins
    runtime._FULLY_LOADED = saved_fully_loaded
    (
        database._DATABASE_CONFIG,
        database._POOL,
        database._POOL_OWNED,
        database._POOL_FACTORY,
    ) = saved_database_state


class FakeTransaction:
    def __init__(self, owner):
        self.owner = owner

    async def __aenter__(self):
        self.owner.begins += 1

    async def __aexit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.owner.commits += 1
        else:
            self.owner.rollbacks += 1


class FakeConnection:
    def __init__(self):
        self.begins = 0
        self.commits = 0
        self.rollbacks = 0
        self.executions: list[tuple[str, tuple]] = []

    def transaction(self, **options):
        return FakeTransaction(self)

    async def execute(self, statement, *args, timeout=None):
        self.executions.append((statement, args))
        return "UPDATE 1"

    async def fetch(self, statement, *args, timeout=None):
        self.executions.append((statement, args))
        return []

    async def fetchval(self, statement, *args, column=0, timeout=None):
        self.executions.append((statement, args))
        return 1


class PaginationConnection(FakeConnection):
    """Return deterministic rows for count and page statements."""

    async def fetch(self, statement, *args, timeout=None):
        self.executions.append((statement, args))
        if "COUNT(*)" in statement:
            return [{"count": 61}]
        return [{"row_id": 31, "name": "page-two"}]


class FakeAcquire:
    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    async def __aenter__(self):
        self.connection = FakeConnection()
        self.pool.connections.append(self.connection)
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class FakePool:
    def __init__(self):
        self.connections: list[FakeConnection] = []

    def acquire(self):
        return FakeAcquire(self)

    async def close(self):
        return None


class FakeCodecConnection:
    def __init__(self):
        self.codecs: dict[str, dict] = {}

    async def set_type_codec(self, type_name, **options):
        self.codecs[type_name] = options


def write_mapper(tmp_path, *, namespace: str, sql: str = "UPDATE demo SET value = :value",
                 sql_id: str = "update"):
    path = tmp_path / f"{namespace.rsplit('.', 1)[-1]}.xml"
    path.write_text(
        f'<mapper namespace="{namespace}"><update id="{sql_id}">{sql}</update></mapper>',
        encoding="utf-8",
    )
    return path


@pytest.mark.asyncio
async def test_json_codecs_return_native_values_without_double_encoding_strings():
    connection = FakeCodecConnection()

    await database._initialize_connection(connection)

    assert set(connection.codecs) == {"json", "jsonb"}
    for codec in connection.codecs.values():
        assert codec["format"] == "text"
        assert codec["decoder"]('{"items":[1,2]}') == {"items": [1, 2]}
        assert json.loads(codec["encoder"]({"items": [1, 2]})) == {"items": [1, 2]}
        assert codec["encoder"]('{"already":"serialized"}') == '{"already":"serialized"}'


@pytest.mark.asyncio
async def test_extension_scans_mapper_packages_and_starts_ready(tmp_path, monkeypatch):
    package_dir = tmp_path / "extension_probe"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "orders_mapper.py").write_text(
        "from cyt_pymapper import amapper\n"
        "@amapper()\n"
        "class OrdersMapper:\n"
        "    async def update(*, value: str | None = None) -> int: ...\n",
        encoding="utf-8",
    )
    mapper_file = tmp_path / "OrdersMapper.xml"
    mapper_file.write_text(
        '<mapper namespace="extension_probe.orders_mapper.OrdersMapper">'
        '<update id="update">UPDATE demo SET value = :value</update>'
        "</mapper>",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    pool = FakePool()
    extension = PyMapperExtension(
        pool=pool,
        mapper_paths=[mapper_file],
        mapper_packages=["extension_probe"],
    )

    state = await extension.startup()
    module = importlib.import_module("extension_probe.orders_mapper")

    assert state.statement_count == 1
    assert state.database_ready is True
    assert "extension_probe.orders_mapper" in extension.imported_modules
    assert await module.OrdersMapper.update(value="ready") == 1
    await extension.shutdown()


# ============================================================ 可插拔执行能力
@pytest.mark.asyncio
async def test_pagination_disabled_executes_original_sql_without_count_or_rewrite(tmp_path):
    namespace = "tests.probe.RawPageMapper"

    @amapper(namespace=namespace)
    class RawPageMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

        async def count_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "RawPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        'SELECT id AS row_id, name FROM demo WHERE status = :status ORDER BY id'
        '</select>'
        '<select id="count_rows">SELECT COUNT(*) FROM demo WHERE status = :status</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        result = await query(
            RawPageMapper.list_rows,
            pagination=PaginationOptions(enabled=False, page_number=2, page_size=15),
            status="open",
        )

    assert isinstance(result, QueryResult)
    assert result.pagination is None
    assert result.items == [{"row_id": 31, "name": "page-two"}]
    assert len(connection.executions) == 1
    statement, args = connection.executions[0]
    assert "COUNT(*)" not in statement
    assert "LIMIT" not in statement and "OFFSET" not in statement
    assert args == ("open",)


@pytest.mark.asyncio
async def test_pagination_enabled_uses_default_size_and_explicit_count_ref(tmp_path):
    namespace = "tests.probe.AutoPageMapper"

    @amapper(namespace=namespace)
    class AutoPageMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

        async def count_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "AutoPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        'SELECT id AS row_id, name FROM demo WHERE status = :status ORDER BY id'
        '</select>'
        '<select id="count_rows">SELECT COUNT(*) FROM demo WHERE status = :status</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])
    pagination = PaginationOptions(enabled=True, page_number=2)

    assert pagination.page_size == 30

    async with bind_connection(connection):
        result = await query(
            AutoPageMapper.list_rows,
            pagination=pagination,
            status="open",
        )

    assert result.items == [{"row_id": 31, "name": "page-two"}]
    assert result.pagination is not None
    assert result.pagination.page_number == 2
    assert result.pagination.page_size == 30
    assert result.pagination.total == 61
    assert result.pagination.total_pages == 3
    assert result.pagination.has_next is True
    assert len(connection.executions) == 2
    count_statement, count_args = connection.executions[0]
    page_statement, page_args = connection.executions[1]
    assert "COUNT(*)" in count_statement
    assert count_args == ("open",)
    assert "LIMIT $2 OFFSET $3" in page_statement
    assert page_args == ("open", 30, 30)


@pytest.mark.asyncio
async def test_enabled_pagination_requires_stable_top_level_order(tmp_path):
    namespace = "tests.probe.UnorderedPageMapper"

    @amapper(namespace=namespace)
    class UnorderedPageMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "UnorderedPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo"
        "</select></mapper>",
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(PaginationError, match="top-level ORDER BY"):
        await query(
            UnorderedPageMapper.list_rows,
            pagination=PaginationOptions(enabled=True, include_total=False),
        )


@pytest.mark.asyncio
async def test_enabled_pagination_rejects_non_select_mapper(tmp_path):
    namespace = "tests.probe.UpdatePageMapper"

    @amapper(namespace=namespace)
    class UpdatePageMapper:
        async def update_rows() -> int: ...

    mapper_file = tmp_path / "UpdatePageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><update id="update_rows">'
        "UPDATE demo SET active = TRUE"
        "</update></mapper>",
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(PaginationError, match="only supports <select>"):
        await query(
            UpdatePageMapper.update_rows,
            pagination=PaginationOptions(enabled=True, include_total=False),
        )


@pytest.mark.asyncio
async def test_pagination_count_statement_requires_exactly_one_column(tmp_path):
    namespace = "tests.probe.MultiColumnCountMapper"

    @amapper(namespace=namespace)
    class MultiColumnCountMapper:
        async def list_rows() -> list[dict]: ...

    class MultiColumnCountConnection(PaginationConnection):
        async def fetch(self, statement, *args, timeout=None):
            self.executions.append((statement, args))
            if "COUNT(*)" in statement:
                return [{"count": 61, "status": "open"}]
            return [{"row_id": 1}]

    mapper_file = tmp_path / "MultiColumnCountMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select>"
        '<select id="count_rows" expose="false">'
        "SELECT COUNT(*), status FROM demo GROUP BY status"
        "</select>"
        "</mapper>",
        encoding="utf-8",
    )
    connection = MultiColumnCountConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        with pytest.raises(PaginationError, match="exactly one column"):
            await query(
                MultiColumnCountMapper.list_rows,
                pagination=PaginationOptions(enabled=True),
            )


@pytest.mark.parametrize(("count_rows", "message"), [
    ([], "returned no rows"),
    ([{"count": "not-an-integer"}], "non-integer value"),
])
@pytest.mark.asyncio
async def test_pagination_rejects_invalid_count_results(tmp_path, count_rows, message):
    namespace = "tests.probe.InvalidCountResultMapper"

    @amapper(namespace=namespace)
    class InvalidCountResultMapper:
        async def list_rows() -> list[dict]: ...

    class InvalidCountConnection(PaginationConnection):
        async def fetch(self, statement, *args, timeout=None):
            self.executions.append((statement, args))
            if "COUNT(*)" in statement:
                return count_rows
            return [{"row_id": 1}]

    mapper_file = tmp_path / "InvalidCountResultMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select>"
        '<select id="count_rows" expose="false">'
        "SELECT COUNT(*) FROM demo"
        "</select>"
        "</mapper>",
        encoding="utf-8",
    )
    connection = InvalidCountConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        with pytest.raises(PaginationError, match=message):
            await query(
                InvalidCountResultMapper.list_rows,
                pagination=PaginationOptions(enabled=True),
            )


@pytest.mark.parametrize(("pagination", "message"), [
    (
        PaginationOptions(enabled=True, page_size=0, include_total=False),
        "page_size must be between 1 and 200",
    ),
    (
        PaginationOptions(enabled=True, page_size=201, include_total=False),
        "page_size must be between 1 and 200",
    ),
    (
        PaginationOptions(enabled=True, page_number=0, include_total=False),
        "page_number must be greater than or equal to 1",
    ),
])
@pytest.mark.asyncio
async def test_pagination_rejects_invalid_page_boundaries(tmp_path, pagination, message):
    namespace = "tests.probe.OversizedPageMapper"

    @amapper(namespace=namespace)
    class OversizedPageMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "OversizedPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select></mapper>",
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        with pytest.raises(PaginationError, match=message):
            await query(
                OversizedPageMapper.list_rows,
                pagination=pagination,
            )

    assert connection.executions == []


@pytest.mark.asyncio
async def test_last_partially_filled_page_has_no_next_page(tmp_path):
    namespace = "tests.probe.LastPageMapper"

    @amapper(namespace=namespace)
    class LastPageMapper:
        async def list_rows() -> list[dict]: ...

    class LastPageConnection(PaginationConnection):
        async def fetch(self, statement, *args, timeout=None):
            self.executions.append((statement, args))
            if "COUNT(*)" in statement:
                return [{"count": 31}]
            return [{"row_id": 31}]

    mapper_file = tmp_path / "LastPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select>"
        '<select id="count_rows" expose="false">'
        "SELECT COUNT(*) FROM demo"
        "</select>"
        "</mapper>",
        encoding="utf-8",
    )
    connection = LastPageConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        result = await query(
            LastPageMapper.list_rows,
            pagination=PaginationOptions(enabled=True, page_number=2),
        )

    assert result.pagination is not None
    assert result.pagination.total_pages == 2
    assert result.pagination.has_next is False


@pytest.mark.asyncio
async def test_pagination_without_count_fetches_one_extra_row_for_has_next(tmp_path):
    namespace = "tests.probe.NoCountPageMapper"

    @amapper(namespace=namespace)
    class NoCountPageMapper:
        async def list_rows() -> list[dict]: ...

    class ExtraRowConnection(PaginationConnection):
        async def fetch(self, statement, *args, timeout=None):
            self.executions.append((statement, args))
            return [{"row_id": 1}, {"row_id": 2}, {"row_id": 3}]

    mapper_file = tmp_path / "NoCountPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select></mapper>",
        encoding="utf-8",
    )
    connection = ExtraRowConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        result = await query(
            NoCountPageMapper.list_rows,
            pagination=PaginationOptions(
                enabled=True,
                page_size=2,
                include_total=False,
            ),
        )

    assert [row["row_id"] for row in result.items] == [1, 2]
    assert result.pagination is not None
    assert result.pagination.total is None
    assert result.pagination.has_next is True
    assert connection.executions[0][1] == (3, 0)


@pytest.mark.asyncio
async def test_pagination_with_total_requires_explicit_count_ref(tmp_path):
    namespace = "tests.probe.MissingCountPageMapper"

    @amapper(namespace=namespace)
    class MissingCountPageMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "MissingCountPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select></mapper>",
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(PaginationError, match="requires countRef"):
        await query(
            MissingCountPageMapper.list_rows,
            pagination=PaginationOptions(enabled=True, include_total=True),
        )


@pytest.mark.asyncio
async def test_page_marker_controls_clause_position(tmp_path):
    namespace = "tests.probe.MarkerPageMapper"

    @amapper(namespace=namespace)
    class MarkerPageMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "MarkerPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">'
        'SELECT id FROM demo WHERE status = :status ORDER BY id <page/> FOR UPDATE'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        await query(
            MarkerPageMapper.list_rows,
            pagination=PaginationOptions(
                enabled=True,
                page_number=1,
                page_size=10,
                include_total=False,
            ),
            status="open",
        )

    statement, _ = connection.executions[0]
    assert statement.index("LIMIT") < statement.index("FOR UPDATE")


@pytest.mark.asyncio
async def test_unpaged_calls_remove_internal_page_marker_without_paginating(tmp_path):
    namespace = "tests.probe.DirectMarkerMapper"

    @amapper(namespace=namespace)
    class DirectMarkerMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "DirectMarkerMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id <page/> FOR UPDATE"
        "</select></mapper>",
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        await DirectMarkerMapper.list_rows()
        disabled_result = await query(
            DirectMarkerMapper.list_rows,
            pagination=PaginationOptions(enabled=False),
        )

    assert disabled_result.pagination is None
    assert len(connection.executions) == 2
    for statement, _ in connection.executions:
        assert "CYT_PYMAPPER_PAGE" not in statement
        assert "LIMIT" not in statement and "OFFSET" not in statement
        assert statement.rstrip().endswith("FOR UPDATE")


def test_page_marker_inside_subquery_is_rejected_during_load(tmp_path):
    namespace = "tests.probe.NestedMarkerMapper"

    @amapper(namespace=namespace)
    class NestedMarkerMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "NestedMarkerMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT nested.id FROM (SELECT id FROM demo ORDER BY id <page/>) nested "
        "ORDER BY nested.id"
        "</select></mapper>",
        encoding="utf-8",
    )

    with pytest.raises(PaginationConflictError, match=r"<page/>.*顶层"):
        load_mapper(mapper_file)


@pytest.mark.parametrize("sql", [
    "SELECT id, <page/> alias_id FROM demo ORDER BY id",
    "SELECT id FROM demo <page/> ORDER BY id",
    "SELECT id FROM demo WHERE active = :active <page/> ORDER BY id",
])
def test_page_marker_must_follow_the_complete_top_level_order_by(tmp_path, sql):
    namespace = f"tests.probe.MisplacedMarkerMapper{abs(hash(sql))}"

    @amapper(namespace=namespace)
    class MisplacedMarkerMapper:
        async def list_rows(*, active: bool | None = None) -> list[dict]: ...

    mapper_file = tmp_path / f"misplaced-page-{abs(hash(sql))}.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        f"{sql}"
        "</select></mapper>",
        encoding="utf-8",
    )

    with pytest.raises(PaginationConflictError, match=r"<page/>.*ORDER BY.*之后"):
        load_mapper(mapper_file)


@pytest.mark.parametrize(("body", "message"), [
    (
        '<select id="list_rows">SELECT id FROM demo ORDER BY id <page/><page/></select>',
        "more than one <page/>",
    ),
    (
        '<update id="update_rows">UPDATE demo SET active = TRUE <page/></update>',
        "uses <page/> outside <select>",
    ),
])
def test_invalid_page_marker_usage_fails_during_load(tmp_path, body, message):
    mapper_file = tmp_path / f"bad-page-{abs(hash(message))}.xml"
    mapper_file.write_text(
        f'<mapper namespace="probe.invalid.page.{abs(hash(message))}">{body}</mapper>',
        encoding="utf-8",
    )

    with pytest.raises(PaginationConflictError, match=message):
        load_mapper(mapper_file)


def test_page_marker_and_manual_limit_conflict_fails_during_load(tmp_path):
    namespace = "tests.probe.ConflictingPageMapper"

    @amapper(namespace=namespace)
    class ConflictingPageMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "ConflictingPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">'
        'SELECT id FROM demo WHERE status = :status ORDER BY id <page/> LIMIT 10'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(PaginationConflictError, match="page.*LIMIT|LIMIT.*page"):
        load_all_mappers()


@pytest.mark.asyncio
async def test_disabled_pagination_preserves_manual_limit(tmp_path):
    namespace = "tests.probe.ManualLimitMapper"

    @amapper(namespace=namespace)
    class ManualLimitMapper:
        async def list_rows(*, limit: int = 7) -> list[dict]: ...

    mapper_file = tmp_path / "ManualLimitMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">SELECT id FROM demo ORDER BY id LIMIT :limit</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        result = await query(
            ManualLimitMapper.list_rows,
            pagination=PaginationOptions(enabled=False),
            limit=7,
        )

    assert result.pagination is None
    assert len(connection.executions) == 1
    assert "LIMIT $1" in connection.executions[0][0]
    assert connection.executions[0][1] == (7,)


@pytest.mark.asyncio
async def test_enabled_pagination_rejects_manual_limit_before_database_call(tmp_path):
    namespace = "tests.probe.EnabledManualLimitMapper"

    @amapper(namespace=namespace)
    class EnabledManualLimitMapper:
        async def list_rows(*, limit: int = 10) -> list[dict]: ...

    mapper_file = tmp_path / "EnabledManualLimitMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">'
        'SELECT id FROM demo ORDER BY id LIMIT :limit'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        with pytest.raises(PaginationConflictError, match="already contains top-level"):
            await query(
                EnabledManualLimitMapper.list_rows,
                pagination=PaginationOptions(enabled=True, include_total=False),
                limit=10,
            )

    assert connection.executions == []


@pytest.mark.asyncio
async def test_for_update_requires_page_marker_when_pagination_is_enabled(tmp_path):
    namespace = "tests.probe.ImplicitLockingPageMapper"

    @amapper(namespace=namespace)
    class ImplicitLockingPageMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "ImplicitLockingPageMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id FOR UPDATE"
        "</select></mapper>",
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        with pytest.raises(PaginationError, match=r"FOR UPDATE.*<page/>"):
            await query(
                ImplicitLockingPageMapper.list_rows,
                pagination=PaginationOptions(enabled=True, include_total=False),
            )

    assert connection.executions == []


@pytest.mark.asyncio
async def test_subquery_limit_does_not_conflict_with_outer_pagination(tmp_path):
    namespace = "tests.probe.SubqueryLimitMapper"

    @amapper(namespace=namespace)
    class SubqueryLimitMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "SubqueryLimitMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">'
        'SELECT limited.id FROM (SELECT id FROM demo ORDER BY id LIMIT 100) limited '
        'ORDER BY limited.id'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    async with bind_connection(connection):
        await query(
            SubqueryLimitMapper.list_rows,
            pagination=PaginationOptions(
                enabled=True,
                page_size=10,
                include_total=False,
            ),
        )

    statement, args = connection.executions[0]
    assert statement.count("LIMIT") == 2
    assert statement.rstrip().endswith("LIMIT $1 OFFSET $2")
    assert args == (11, 0)


def test_count_ref_must_not_reference_the_paginated_statement(tmp_path):
    namespace = "tests.probe.SelfCountMapper"

    @amapper(namespace=namespace)
    class SelfCountMapper:
        async def list_rows() -> list[dict]: ...

    mapper_file = tmp_path / "SelfCountMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="list_rows">SELECT id FROM demo ORDER BY id</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(ValueError, match="countRef must not reference itself"):
        load_all_mappers()


def test_count_ref_is_only_valid_on_select_statements(tmp_path):
    namespace = "tests.probe.UpdateCountMapper"
    mapper_file = tmp_path / "UpdateCountMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<update id="update_rows" countRef="count_rows">'
        "UPDATE demo SET active = TRUE"
        "</update>"
        '<select id="count_rows">SELECT COUNT(*) FROM demo</select>'
        "</mapper>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"countRef.*only valid on <select>"):
        load_mapper(mapper_file)


@pytest.mark.parametrize(("body", "message"), [
    (
        '<select id="list_rows" countRef="missing">SELECT id FROM demo ORDER BY id</select>',
        "points to missing statement",
    ),
    (
        '<select id="list_rows" countRef="count_rows">SELECT id FROM demo ORDER BY id</select>'
        '<update id="count_rows">UPDATE demo SET active = TRUE</update>',
        "must point to <select>",
    ),
    (
        '<select id="list_rows" countRef=" ">SELECT id FROM demo ORDER BY id</select>',
        "countRef must not be empty",
    ),
    (
        '<select id="list_rows" countRef="other.count_rows">'
        "SELECT id FROM demo ORDER BY id</select>",
        "countRef must be a local statement id",
    ),
])
def test_invalid_count_ref_contract_fails_during_load(tmp_path, body, message):
    mapper_file = tmp_path / f"bad-count-ref-{abs(hash(message))}.xml"
    mapper_file.write_text(
        f'<mapper namespace="probe.invalid.count.{abs(hash(message))}">{body}</mapper>',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_mapper(mapper_file)


def test_hidden_count_ref_uses_paginated_mapper_signature(tmp_path):
    namespace = "tests.probe.HiddenCountMapper"

    @amapper(namespace=namespace)
    class HiddenCountMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "HiddenCountMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        'SELECT id FROM demo WHERE status = :status ORDER BY id'
        '</select>'
        '<select id="count_rows" expose="false">'
        'SELECT COUNT(*) FROM demo WHERE status = :status'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    assert load_all_mappers() == 2


def test_hidden_count_ref_parameter_typo_fails_during_load(tmp_path):
    namespace = "tests.probe.HiddenCountTypoMapper"

    @amapper(namespace=namespace)
    class HiddenCountTypoMapper:
        async def list_rows(*, status: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "HiddenCountTypoMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows" countRef="count_rows">'
        'SELECT id FROM demo WHERE status = :status ORDER BY id'
        '</select>'
        '<select id="count_rows" expose="false">'
        'SELECT COUNT(*) FROM demo WHERE status = :statsu'
        '</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(ValueError, match=r"countRef.*statsu"):
        load_all_mappers()


@pytest.mark.asyncio
async def test_statement_plugins_wrap_execution_in_configuration_order(tmp_path):
    namespace = "tests.probe.PluginOrderMapper"
    events: list[str] = []

    @amapper(namespace=namespace)
    class PluginOrderMapper:
        async def list_rows() -> list[dict]: ...

    class RecordingPlugin:
        def __init__(self, name: str) -> None:
            self.name = name

        async def execute(self, context, call_next):
            events.append(f"{self.name}.before")
            result = await call_next(context)
            events.append(f"{self.name}.after")
            return result

    mapper_file = tmp_path / "PluginOrderMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="list_rows">'
        "SELECT id FROM demo ORDER BY id"
        "</select></mapper>",
        encoding="utf-8",
    )
    configure(
        pool=FakePool(),
        mapper_paths=[mapper_file],
        plugins=[RecordingPlugin("first"), RecordingPlugin("second")],
    )

    await PluginOrderMapper.list_rows()

    assert events == [
        "first.before",
        "second.before",
        "second.after",
        "first.after",
    ]


@pytest.mark.asyncio
async def test_plugin_error_after_execution_propagates_without_reexecuting_sql(tmp_path):
    namespace = "tests.probe.PluginErrorMapper"

    @amapper(namespace=namespace)
    class PluginErrorMapper:
        async def update_rows() -> int: ...

    class FailingAfterPlugin:
        async def execute(self, context, call_next):
            await call_next(context)
            raise RuntimeError("plugin failed after execution")

    mapper_file = tmp_path / "PluginErrorMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><update id="update_rows">'
        "UPDATE demo SET active = TRUE"
        "</update></mapper>",
        encoding="utf-8",
    )
    pool = FakePool()
    configure(
        pool=pool,
        mapper_paths=[mapper_file],
        plugins=[FailingAfterPlugin()],
    )

    with pytest.raises(RuntimeError, match="plugin failed after execution"):
        await PluginErrorMapper.update_rows()

    assert len(pool.connections) == 1
    assert len(pool.connections[0].executions) == 1


@pytest.mark.asyncio
async def test_sql_logging_plugin_records_shape_without_parameter_values(tmp_path, caplog):
    namespace = "tests.probe.LoggingMapper"

    @amapper(namespace=namespace)
    class LoggingMapper:
        async def list_rows(*, secret: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "LoggingMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">SELECT id FROM demo WHERE secret = :secret ORDER BY id</select>'
        '</mapper>',
        encoding="utf-8",
    )
    connection = PaginationConnection()
    configure(
        pool=FakePool(),
        mapper_paths=[mapper_file],
        plugins=[SqlLoggingPlugin(slow_query_threshold_ms=0)],
    )

    with caplog.at_level(logging.WARNING, logger="cyt_pymapper.query"):
        async with bind_connection(connection):
            await LoggingMapper.list_rows(secret="do-not-log-this")

    record = next(record for record in caplog.records if record.name == "cyt_pymapper.query")
    event = record.pymapper
    assert event["statement_id"] == f"{namespace}.list_rows"
    assert event["parameter_names"] == ["secret"]
    assert event["parameter_types"] == ["str"]
    assert event["row_count"] == 1
    assert event["slow"] is True
    assert "$1" in event["sql"]
    assert "do-not-log-this" not in record.getMessage()
    assert "do-not-log-this" not in json.dumps(event)


@pytest.mark.asyncio
async def test_sql_logging_plugin_keeps_traceback_and_error_type(tmp_path, caplog):
    namespace = "tests.probe.FailingLoggingMapper"

    @amapper(namespace=namespace)
    class FailingLoggingMapper:
        async def list_rows(*, secret: str | None = None) -> list[dict]: ...

    class FailingConnection(FakeConnection):
        async def fetch(self, statement, *args, timeout=None):
            raise RuntimeError("database unavailable")

    mapper_file = tmp_path / "FailingLoggingMapper.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="list_rows">SELECT id FROM demo WHERE secret = :secret ORDER BY id</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(
        pool=FakePool(),
        mapper_paths=[mapper_file],
        plugins=[SqlLoggingPlugin()],
    )

    with caplog.at_level(logging.ERROR, logger="cyt_pymapper.query"):
        with pytest.raises(RuntimeError, match="database unavailable"):
            async with bind_connection(FailingConnection()):
                await FailingLoggingMapper.list_rows(secret="do-not-log-this")

    record = next(record for record in caplog.records if record.name == "cyt_pymapper.query")
    assert record.exc_info is not None
    assert record.pymapper["error_type"] == "RuntimeError"
    assert "do-not-log-this" not in json.dumps(record.pymapper)


# ============================================================ 事务语义
@pytest.mark.asyncio
async def test_direct_mapper_call_owns_one_short_unit_of_work(tmp_path):
    namespace = "tests.probe.DirectMapper"

    @amapper(namespace=namespace)
    class DirectMapper:
        async def update(*, value: str | None = None) -> int: ...

    pool = FakePool()
    configure(
        pool=pool,
        mapper_paths=[write_mapper(tmp_path, namespace=namespace)],
    )

    assert await DirectMapper.update(value="one") == 1
    assert current_connection() is None
    assert len(pool.connections) == 1
    assert pool.connections[0].begins == 0


@pytest.mark.asyncio
async def test_transactional_calls_share_session_and_rollback_together(tmp_path):
    namespace = "tests.probe.TransactionalMapper"

    @amapper(namespace=namespace)
    class TransactionalMapper:
        async def update(*, value: str | None = None) -> int: ...

    pool = FakePool()
    configure(
        pool=pool,
        mapper_paths=[write_mapper(tmp_path, namespace=namespace)],
    )

    @transactional()
    async def update_twice_then_fail():
        await TransactionalMapper.update(value="one")
        await TransactionalMapper.update(value="two")
        raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        await update_twice_then_fail()

    assert len(pool.connections) == 1
    assert len(pool.connections[0].executions) == 2
    assert pool.connections[0].commits == 0
    assert pool.connections[0].rollbacks == 1


@pytest.mark.asyncio
async def test_requires_new_uses_independent_session_and_commit(tmp_path):
    namespace = "tests.probe.RequiresNewMapper"

    @amapper(namespace=namespace)
    class RequiresNewMapper:
        async def update(*, value: str | None = None) -> int: ...

    pool = FakePool()
    configure(
        pool=pool,
        mapper_paths=[write_mapper(tmp_path, namespace=namespace)],
    )

    @transactional(propagation="REQUIRES_NEW")
    async def write_inner_transaction():
        await RequiresNewMapper.update(value="inner")

    @transactional()
    async def write_outer_then_fail():
        await RequiresNewMapper.update(value="outer")
        await write_inner_transaction()
        raise RuntimeError("outer rollback")

    with pytest.raises(RuntimeError, match="outer rollback"):
        await write_outer_then_fail()

    outer_connection, inner_connection = pool.connections
    assert outer_connection.rollbacks == 1
    assert outer_connection.commits == 0
    assert inner_connection.commits == 1
    assert inner_connection.rollbacks == 0


@pytest.mark.asyncio
async def test_omitted_mapper_parameter_is_filled_with_none(tmp_path):
    """契约只追到 Mapper; 调用方省略参数时统一按 None 传给 SQL。

    这是防"jinja Undefined is not none 为 True"那坑的行为锚点: 不补全的话,
    省略参数仍会渲染出该子句但缺绑参。
    """
    namespace = "tests.probe.OmittedMapper"

    @amapper(namespace=namespace)
    class OmittedMapper:
        async def update(*, value: str | None = None, po_id: int | None = None) -> int: ...

    pool = FakePool()
    configure(
        pool=pool,
        mapper_paths=[write_mapper(
            tmp_path, namespace=namespace,
            sql="UPDATE demo SET value = :value WHERE :po_id IS NULL OR id = :po_id")],
    )

    await OmittedMapper.update(value="only-this")
    sql, args = pool.connections[0].executions[0]
    assert "$1" in sql and "$2" in sql
    assert args == ("only-this", None)


@pytest.mark.asyncio
async def test_mapper_parameter_names_cannot_collide_with_execution_metadata(tmp_path):
    """Mapper parameters are data, even when named like framework metadata fields."""
    namespace = "tests.probe.MetadataNameMapper"

    @amapper(namespace=namespace)
    class MetadataNameMapper:
        async def update(
            *, operation: str | None = None, plugins: str | None = None,
            connection_wait_ms: int | None = None,
        ) -> int: ...

    mapper_file = write_mapper(
        tmp_path,
        namespace=namespace,
        sql=(
            "UPDATE demo SET operation = :operation, plugins = :plugins "
            "WHERE wait_ms = :connection_wait_ms"
        ),
    )
    pool = FakePool()
    configure(pool=pool, mapper_paths=[mapper_file])

    await MetadataNameMapper.update(
        operation="create",
        plugins="business-value",
        connection_wait_ms=25,
    )

    assert pool.connections[0].executions[0][1] == (
        "create",
        "business-value",
        25,
    )


# ============================================================ 配置生命周期
def test_framework_requires_explicit_configuration():
    with pytest.raises(RuntimeError, match="not configured"):
        load_all_mappers()


def test_reconfigure_clears_loaded_sql_and_loads_new_paths(tmp_path):
    """configure(A)→load→configure(B)→load 必须真的加载 B —— 旧实现返回 A 的旧计数。"""
    ns_a, ns_b = "tests.probe.ReloadA", "tests.probe.ReloadB"
    pool = FakePool()

    # write_mapper 按 namespace 尾段命名文件(ReloadA.xml / ReloadB.xml), 同目录不冲突
    configure(pool=pool,
              mapper_paths=[write_mapper(tmp_path, namespace=ns_a)])
    load_all_mappers()
    assert f"{ns_a}.update" in runtime._SQL_CONTAINER

    configure(pool=pool,
              mapper_paths=[write_mapper(tmp_path, namespace=ns_b)])
    assert not runtime._SQL_CONTAINER, "reconfigure 后已加载 SQL 必须被清空"
    load_all_mappers()
    assert f"{ns_b}.update" in runtime._SQL_CONTAINER
    assert f"{ns_a}.update" not in runtime._SQL_CONTAINER


# ============================================================ 加载期契约
@pytest.mark.parametrize("bad_sql", [
    "SELECT 1 FROM t WHERE po_no = '{{ q }}'",       # 裸值 —— 与片段引用文法同形, 必须一起拒
    "SELECT {{ a|upper }} FROM t",                    # 带过滤器
    "SELECT * FROM t ORDER BY {{ sort_col }}",        # 动态列名(也走不了绑参, 需白名单映射)
    "SELECT * FROM t\nWHERE x = {{\n  q\n}}",         # 跨行
])
def test_jinja_value_interpolation_rejected_at_load(tmp_path, bad_sql):
    """任何 {{ }} 都在加载期被拒: {{ q }} 与 {{ where_common }} 文法无法区分, 不留白名单。"""
    from pathlib import Path
    with pytest.raises(ValueError, match="jinja 值插值"):
        _reject_interpolation("probe.bad", bad_sql, Path("probe.xml"))


def test_structure_only_jinja_is_allowed():
    """{% if %} 控结构 + :name 绑参 = 合法写法, 不被拦。"""
    from pathlib import Path
    _reject_interpolation(
        "probe.ok",
        "SELECT 1 FROM t WHERE 1=1 {% if q %} AND po_no ILIKE :q {% endif %}",
        Path("probe.xml"))
    _reject_interpolation(
        "probe.ok_plus_whitespace_control",
        "SELECT 1 FROM t WHERE 1=1 "
        "{%+ if q +%} AND po_no ILIKE :q "
        "{%+ elif fallback +%} AND active = TRUE "
        "{%+ else +%} AND active = FALSE {%+ endif +%}",
        Path("probe.xml"),
    )


@pytest.mark.parametrize("unsafe_sql", [
    "SELECT id FROM demo WHERE name = '{% print value %}'",
    "SELECT id FROM demo WHERE name = '{%- print value -%}'",
    "SELECT id FROM demo WHERE name = '{%+ print value +%}'",
    "SELECT id FROM demo {% filter upper %}WHERE name = :value{% endfilter %}",
    "{%+ set alias = value +%} SELECT id FROM demo",
    'SELECT id FROM demo {% include "other.sql" %}',
    "{% set alias = value %} SELECT id FROM demo",
    "{% for item in items %} SELECT :item {% endfor %}",
    "{% macro predicate() %} TRUE {% endmacro %} SELECT id FROM demo",
])
def test_non_structural_jinja_statement_is_rejected_during_mapper_load(
    tmp_path,
    unsafe_sql,
):
    namespace = "probe.jinja_print.BadMapper"

    @amapper(namespace=namespace)
    class BadMapper:
        async def fetch(*, value: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "bad-jinja-print.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="fetch">'
        f"{unsafe_sql}"
        "</select></mapper>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="jinja.*只允许"):
        load_mapper(mapper_file)


@pytest.mark.parametrize(("body", "message"), [
    ('<sql>SELECT 1</sql>', "片段缺 id"),
    ('<sql id="same">SELECT 1</sql><sql id="same">SELECT 2</sql>', "片段 id 重复"),
    ('<select>SELECT 1</select>', "mapper 条目缺 id"),
    ('<select id="same">SELECT 1</select><select id="same">SELECT 2</select>', "sql_id 重复"),
    ('<slect id="pick">SELECT 1</slect>', "不支持的 mapper 标签"),
])
def test_invalid_mapper_ids_always_raise_value_error(tmp_path, body, message):
    """XML 配置错误必须用生产环境始终生效的异常暴露, 不能依赖 assert。"""
    mapper_file = tmp_path / f"bad-{abs(hash(message))}.xml"
    namespace = f"probe.invalid.{abs(hash(message))}"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">{body}</mapper>', encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_mapper(mapper_file)


def test_xml_bind_parameter_must_be_declared_by_mapper(tmp_path):
    """XML 的 :name 拼写错误必须在 Mapper 加载边界暴露。"""
    namespace = "probe.bind_contract.BadMapper"

    @amapper(namespace=namespace)
    class BadMapper:
        async def fetch(*, po_id=None): ...

    mapper_file = tmp_path / "bad-bind.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="fetch">'
        'SELECT :po_di</select></mapper>',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"XML 绑定参数未在 Mapper 方法签名声明.*po_di"):
        load_mapper(mapper_file)


def test_native_asyncpg_positional_parameter_is_rejected_during_load(tmp_path):
    namespace = "probe.native_position.BadMapper"

    @amapper(namespace=namespace)
    class BadMapper:
        async def fetch(*, order_no: str | None = None) -> list[dict]: ...

    mapper_file = tmp_path / "bad-native-position.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="fetch">'
        "SELECT id FROM orders WHERE tenant_id = $1 AND order_no = :order_no"
        "</select></mapper>",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"原生位置参数.*\$1"):
        load_mapper(mapper_file)


@pytest.mark.parametrize("sql", [
    "SELECT '$1' AS literal_value",
    'SELECT "$1" AS quoted_identifier',
    "SELECT 1 -- $1\n",
    "SELECT 1 /* $1 */",
    "SELECT $$ $1 $$ AS dollar_body",
    "SELECT $tag$ $1 $tag$ AS tagged_dollar_body",
])
def test_native_position_text_in_non_executable_regions_is_allowed(tmp_path, sql):
    namespace = f"probe.native_position.SafeMapper{abs(hash(sql))}"
    mapper_file = tmp_path / f"safe-native-position-{abs(hash(sql))}.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="fetch">{sql}</select></mapper>',
        encoding="utf-8",
    )

    load_mapper(mapper_file)


@pytest.mark.parametrize(("sql", "expected"), [
    ("SELECT E'x\\'$1' FROM demo", set()),
    ("SELECT a$b$c FROM demo WHERE id = $1", {1}),
    ("SELECT a$b$c, d$b$e FROM demo WHERE id = $1", {1}),
])
def test_native_position_scanner_handles_postgresql_lexical_boundaries(sql, expected):
    assert positional_parameter_numbers(sql) == expected


@pytest.mark.parametrize("sql", [
    "SELECT E'x\\')' AS value FROM demo ORDER BY value __PAGE_MARKER__",
    "SELECT a$b$c FROM demo ORDER BY 1 __PAGE_MARKER__",
])
def test_page_depth_scanner_handles_postgresql_lexical_boundaries(sql):
    assert sql_token_parenthesis_depths(sql, "__PAGE_MARKER__") == (0,)


def test_dollar_identifier_does_not_create_false_top_level_keyword():
    sql = "SELECT price$limit FROM demo ORDER BY id"

    assert contains_top_level_keyword(sql, "LIMIT") is False


@pytest.mark.parametrize(("sql", "expected"), [
    ("SELECT * FROM t WHERE id = :po_id", {"po_id"}),
    ("SELECT to_char(created_at, 'HH24:MI') AS hm FROM t WHERE id = :po_id", {"po_id"}),
    ("SELECT raw::text FROM t WHERE id = :po_id", {"po_id"}),
    ("SELECT * FROM t WHERE id IN :ids AND po_no ILIKE :keyword", {"ids", "keyword"}),
    ("SELECT 'https://a.b/c' AS u, '12:30' AS hm FROM t WHERE id=:po_id", {"po_id"}),
    ("UPDATE t SET a = :a, b = :b WHERE id = :po_id RETURNING id", {"a", "b", "po_id"}),
    ("SELECT ':ignored' /* :comment */ -- :line\n, :kept", {"kept"}),
    ("SELECT E'x\\':ignored' AS escaped FROM t WHERE id = :kept", {"kept"}),
])
def test_named_parameter_scanner_ignores_non_executable_sql(sql, expected):
    assert named_parameter_names(sql) == expected


def test_compile_query_uses_positional_parameters_and_expands_collections():
    compiled = compile_query(
        "SELECT * FROM t WHERE id IN :ids AND status=:status OR backup=:status",
        {"ids": [7, 9], "status": "open"},
    )

    assert compiled.sql == (
        "SELECT * FROM t WHERE id IN ($1, $2) AND status=$3 OR backup=$3"
    )
    assert compiled.args == (7, 9, "open")


def _write_probe_mapper(tmp_path, tag: str, sql: str) -> tuple[object, str]:
    """造一个只有 fetch 一条条目的探针 mapper(namespace 按 tag 唯一)。"""
    namespace = f"probe.colon.{tag}"

    @amapper(namespace=namespace)
    class Probe:
        async def fetch(*, po_id: int | None = None) -> list: ...

    mapper_file = tmp_path / f"{tag}.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}"><select id="fetch">{sql}</select></mapper>',
        encoding="utf-8")
    return mapper_file, namespace


def test_colon_inside_string_literal_is_not_treated_as_bind(tmp_path):
    """SQL 字符串字面量里的冒号不能被绑参契约误拒(to_char 时间格式串是最常见的一种)。"""
    mapper_file, namespace = _write_probe_mapper(
        tmp_path, "time_format",
        "SELECT to_char(created_at, 'HH24:MI') AS hm FROM demo WHERE id = :po_id")

    load_mapper(mapper_file)          # 不抛错即通过

    assert runtime._BIND_VARS[f"{namespace}.fetch"] == {"po_id"}


def test_cast_suffix_bind_is_rejected_at_load(tmp_path):
    """`:name::type` SQLAlchemy 不认, 必须加载期拦掉而不是等 PG 报语法错。"""
    mapper_file, _ = _write_probe_mapper(
        tmp_path, "cast_suffix",
        "SELECT * FROM demo WHERE id = ANY(:po_id::int[])")

    with pytest.raises(ValueError, match="CAST"):
        load_mapper(mapper_file)


def test_postgres_cast_and_time_literal_not_treated_as_param(tmp_path):
    """::cast 与 '12:30' 这类冒号不能被 render_sql 误当绑参。"""
    from jinja2 import Template
    # 先真加载一轮(render_sql 的惰性入口按 _FULLY_LOADED 判定, 直接注入不算已加载)
    configure(pool=FakePool(),
              mapper_paths=[write_mapper(tmp_path, namespace="tests.probe.ColonHost")])
    load_all_mappers()
    runtime._SQL_CONTAINER["probe.colon"] = Template(
        "SELECT '12:30' AS t, 1::text AS c")
    sql, params = runtime.render_sql("probe.colon")
    assert params == {}


def test_amapper_enhancer_inherits_mapper_base():
    from cyt_pymapper import AMapper, MapperBase
    assert issubclass(AMapper, MapperBase)
    assert amapper is AMapper


# ============================================================ 2026-08-17 外评修复的回归锚点
def test_jinja_var_typo_is_caught_at_load_not_first_call(tmp_path):
    """{% if 拼写错 %} 必须在**加载期**炸, 不是首次调用:

    拼写错的变量渲染成 Undefined(假值), 它守着的 WHERE 被静默吞掉 ——
    `UPDATE ... {% if valeu %}WHERE ...{% endif %}` 直接退化成全表更新。
    """
    namespace = "tests.probe.JinjaTypoMapper"

    @amapper(namespace=namespace)
    class JinjaTypoMapper:
        async def update(*, value: str | None = None) -> int: ...

    mapper_file = write_mapper(
        tmp_path, namespace=namespace,
        sql="UPDATE demo SET flag = 1 {% if valeu %}WHERE value = :value{% endif %}")

    with pytest.raises(ValueError, match=r"引用了签名没有的变量.*valeu"):
        load_mapper(mapper_file)


def test_jinja_var_typo_is_caught_at_decoration_when_xml_loaded_first(tmp_path):
    """反过来的 import 顺序同样要抓: XML 先加载, 装饰类时比对。"""
    namespace = "tests.probe.JinjaTypoLateMapper"
    mapper_file = write_mapper(
        tmp_path, namespace=namespace,
        sql="UPDATE demo SET flag = 1 {% if valeu %}WHERE value = :value{% endif %}")
    load_mapper(mapper_file)   # 类未装饰, 单边加载不报错

    with pytest.raises(ValueError, match=r"引用了签名没有的变量.*valeu"):
        @amapper(namespace=namespace)
        class JinjaTypoLateMapper:
            async def update(*, value: str | None = None) -> int: ...


def test_partial_load_does_not_block_full_load(tmp_path):
    """load_mapper(单文件) 之后 load_all_mappers 必须仍然全量加载(先清再重建),
    而不是看容器非空就当已加载 —— 旧实现会让配置目录里其余 XML 永远缺席。"""
    ns_cfg, ns_extra = "tests.probe.ConfiguredNs", "tests.probe.ExtraNs"
    configure(pool=FakePool(),
              mapper_paths=[write_mapper(tmp_path, namespace=ns_cfg)])

    load_mapper(write_mapper(tmp_path, namespace=ns_extra))   # 局部加载在先
    assert f"{ns_extra}.update" in runtime._SQL_CONTAINER

    count = load_all_mappers()
    assert f"{ns_cfg}.update" in runtime._SQL_CONTAINER, "配置路径的条目必须被加载"
    # 重建语义: 不在 mapper_paths 里的局部条目被丢弃 —— 所有 XML 都该进 mapper_paths
    assert f"{ns_extra}.update" not in runtime._SQL_CONTAINER
    assert count == len(runtime._SQL_CONTAINER)


def test_transactional_rejects_sync_function_at_decoration():
    """同步函数装饰期即拒 —— 否则 `await func()` 要等调用时才炸, 冷路径拖到生产。"""
    with pytest.raises(TypeError, match="只能装饰 async 函数"):
        @transactional()
        def sync_update() -> int:
            return 1


# ============================================================ MyBatis 风格结果映射
def test_result_type_ignores_unmodelled_columns_and_uses_defaults():
    """resultType 自动映射只填模型字段；SQL 多余列忽略，缺失可选字段走默认值。"""
    full_id = "tests.probe.auto_mapping"
    result_mapping._RESULT_SPEC[full_id] = result_mapping.ResultSpec(
        f"{__name__}.AutoMappedRow", None, False,
    )

    rows = result_mapping.shape_rows(full_id, [{
        "row_id": 7,
        "name": "mapped",
        "joined_only_column": "ignored",
    }])

    assert rows == [AutoMappedRow(row_id=7, name="mapped")]


@pytest.mark.parametrize(
    ("database_rows", "expected"),
    [([], None), ([{"row_id": 7, "name": "only"}], AutoMappedRow(row_id=7, name="only"))],
)
def test_single_result_accepts_zero_or_one_row(database_rows, expected):
    """single=true 的合法基数是 0..1：空结果返回 None，单行返回对象。"""
    full_id = "tests.probe.single_result"
    result_mapping._RESULT_SPEC[full_id] = result_mapping.ResultSpec(
        f"{__name__}.AutoMappedRow", None, True,
    )

    assert result_mapping.shape_rows(full_id, database_rows) == expected


def test_single_result_rejects_multiple_rows():
    """single=true 不得只取第一行，否则会静默掩盖唯一性约束或 SQL 错误。"""
    full_id = "tests.probe.too_many_results"
    result_mapping._RESULT_SPEC[full_id] = result_mapping.ResultSpec("", None, True)

    with pytest.raises(
        TooManyResultsError,
        match=r"tests\.probe\.too_many_results.*single=true.*2 行",
    ):
        result_mapping.shape_rows(full_id, [{"row_id": 1}, {"row_id": 2}])

    assert TooManyResultsError is ErrorsModuleTooManyResultsError


def test_result_map_maps_declared_property_and_ignores_other_columns():
    """resultMap 强制列名转换，但未声明且模型不接收的 SQL 列仍按自动映射规则忽略。"""
    full_id = "tests.probe.explicit_mapping"
    result_mapping._RESULT_SPEC[full_id] = result_mapping.ResultSpec(
        f"{__name__}.AutoMappedRow", {"database_name": "name"}, False,
    )

    rows = result_mapping.shape_rows(full_id, [{
        "row_id": 8,
        "database_name": "renamed",
        "joined_only_column": "ignored",
    }])

    assert rows == [AutoMappedRow(row_id=8, name="renamed")]


def test_result_map_rejects_explicit_unknown_model_property_at_startup(tmp_path):
    """XML 明确指定的 property 不存在时必须在全量加载阶段失败。"""
    namespace = "tests.probe.BadResultMap"
    mapper_file = tmp_path / "bad-result-map.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        f'<resultMap id="row" type="{__name__}.AutoMappedRow">'
        '<result column="database_name" property="missing_property"/>'
        '</resultMap>'
        '<select id="find" resultMap="row">SELECT 1 AS row_id</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(ValueError, match=r"显式映射了模型.*不存在的属性.*missing_property"):
        load_all_mappers()


def test_invalid_result_type_path_fails_during_full_load(tmp_path):
    """resultType 路径错误不能拖到接口首次返回数据时才暴露。"""
    namespace = "tests.probe.BadResultType"
    mapper_file = tmp_path / "bad-result-type.xml"
    mapper_file.write_text(
        f'<mapper namespace="{namespace}">'
        '<select id="find" resultType="missing.module.Row">SELECT 1</select>'
        '</mapper>',
        encoding="utf-8",
    )
    configure(pool=FakePool(), mapper_paths=[mapper_file])

    with pytest.raises(ValueError, match=r"resultType.*无法导入"):
        load_all_mappers()
