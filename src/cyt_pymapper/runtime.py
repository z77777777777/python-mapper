"""XML mapper execution runtime backed directly by asyncpg.

对外 API 从 `cyt_pymapper` 导入(本模块是实现)。

XML keeps readable ``:name`` parameters. They are compiled to asyncpg ``$1``
parameters after Jinja has selected SQL structure. Collection parameters retain
the established ``IN :names`` expansion behavior.
"""
from __future__ import annotations

import functools
import inspect
import re
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar, cast

from jinja2 import Environment, Template, meta

from cyt_pymapper import mapping as result_mapping
from cyt_pymapper.base import MapperBase
from cyt_pymapper.compiler import compile_query, contains_sql_keyword, named_parameter_names
from cyt_pymapper.database import ConnectionLike

validate_result_types = result_mapping.validate_result_types

_SQL_CONTAINER: dict[str, Template] = {}
_NS_FILE: dict[str, Path] = {}  # namespace → 所属 XML 文件(1 XML ↔ 1 mapper, 不交叉)
_JINJA_VARS: dict[str, set[str]] = {}  # full_id → 模板 {% if %} 引用的变量名(签名比对用)
_JINJA_ENV = Environment(autoescape=False)  # noqa: S701 - 产物是 SQL 不是 HTML, 且值一律走绑参

# namespace → (Mapper 类 qualname, 已绑定的方法名集合) —— 与 XML 条目双向对齐校验用
_NS_METHODS: dict[str, tuple[str, set[str]]] = {}
# full_id → XML 命名绑参 / Mapper 声明参数。契约只校验到 Mapper, 不追踪上层调用方。
_BIND_VARS: dict[str, set[str]] = {}
_METHOD_PARAMS: dict[str, set[str]] = {}
_STATEMENT_KINDS: dict[str, str] = {}
_DecoratedT = TypeVar("_DecoratedT")
_MAPPER_PATHS: tuple[Path, ...] = ()
# "配置的全部路径已完整加载"的显式标志。⚠ 不能拿 _SQL_CONTAINER 非空当这个判据:
# load_mapper(单文件) 是公开原语, 局部加载后容器非空, 旧实现会让 load_all_mappers
# 变 no-op —— 配置目录里其余 XML 永远不加载, 报出来的却是"条目不存在"。
_FULLY_LOADED = False


# Keep casts explicit so named-parameter parsing remains unambiguous.
_CAST_SUFFIX_PARAM = re.compile(r"(?<![:\w\\]):(\w+)::")

# jinja2 值插值 {{ ... }} —— 实测确认是 SQL 注入口(值被直接拼进 SQL, 完全绕过绑参),
# 加载期一律硬拦截: 结构用 {% if %}, 值一律 :name。
# ⚠ 不设"纯标识符放行"白名单: {{ q }}(值) 与 {{ where_common }}(片段) 文法上无法区分,
#   放行等于留下注入口。片段复用走下面的 <<sql:id>> 机制(加载期展开, 不经 jinja)。
_JINJA_INTERP = re.compile(r"\{\{(.*?)\}\}", re.S)

# SQL 片段: <sql id="x">…</sql> 声明, <include refid="x"/> 引用(与 MyBatis 同语法)。
# 用途: list 与 count 共用同一套 WHERE, 改一处两边生效。
# 安全性: 只在**加载期**按 refid 从本 namespace 片段表取文本拼接, 运行期不接受任何输入,
# refid 必须已声明 —— 无法通过参数影响, 与注入无关。
_MAX_FRAGMENT_DEPTH = 5  # 片段可嵌套引用片段, 限深防环


# 加载的 check-then-act 不是原子的。本框架的执行面是纯 asyncio(加载函数无 await, 任务间
# 不可打断), 但消费方可能从线程里触发惰性加载(sync 端点跑在线程池等) —— RLock 三行钱,
# 把"两个线程同时看见空容器、各自加载一遍"这种半加载态直接买断。RLock 而非 Lock:
# _ensure_loaded 持锁调 load_all_mappers, 普通锁会自锁死。
_LOAD_LOCK = threading.RLock()


def configure_mapper_paths(paths: list[str | Path] | tuple[str | Path, ...]) -> None:
    """Register application-owned mapper XML roots without importing application code.

    重新配置会**清掉已加载的 SQL**: 否则 configure(A)→load→configure(B) 后,
    load_all_mappers 见容器非空直接返回旧计数, B 的 XML 永远不加载 ——
    factory 是新的、SQL 是旧的, 这种不一致还无声。清了, 下次加载走新路径。
    """
    normalized = tuple(Path(path).resolve() for path in paths)
    if not normalized:
        raise ValueError("mapper_paths must contain at least one file or directory")
    with _LOAD_LOCK:
        global _MAPPER_PATHS
        _MAPPER_PATHS = normalized
        _clear_loaded_sql()


def load_all_mappers() -> int:
    """Load every configured mapper XML root and return executable statement count.

    幂等键是 _FULLY_LOADED 标志, 不是容器非空; 未完整加载时**先清再整体重建** ——
    局部 load_mapper 留下的条目一律丢弃, 加载结果只由 mapper_paths 决定
    (锁内重建, 对线程等效于"编译成功后原子替换")。所有 XML 都应放进 mapper_paths;
    load_mapper 只是加载原语, 不参与"已加载"的判定。
    """
    with _LOAD_LOCK:
        global _FULLY_LOADED
        if _FULLY_LOADED:
            return len(_SQL_CONTAINER)
        if not _MAPPER_PATHS:
            raise RuntimeError(
                "cyt-pymapper is not configured: call configure(database_url=..., mapper_paths=...)"
            )
        _clear_loaded_sql()
        try:
            for path in _MAPPER_PATHS:
                if not path.exists():
                    raise FileNotFoundError(f"mapper path does not exist: {path}")
                load_mapper(path)
        except Exception:
            _clear_loaded_sql()
            raise
        result_mapping.validate_result_types()
        _FULLY_LOADED = True
        return len(_SQL_CONTAINER)


def reset_state() -> None:
    """Reset mapper registries and configured XML paths.

    数据库池拥有独立的异步生命周期，不能由同步的 mapper 重置函数关闭或清空；调用方
    必须先 await close_database()，确有需要时再清数据库配置。
    ⚠ 已被 @amapper 装饰的类不受影响: wrapper 闭包与其"首次调用已校验"标记都还在。
      reset 后若 XML 契约变了, 需要重新 import(重新装饰)mapper 类才能重跑校验。
    """
    with _LOAD_LOCK:
        _clear_loaded_sql()
        _NS_METHODS.clear()
        _METHOD_PARAMS.clear()
        global _MAPPER_PATHS
        _MAPPER_PATHS = ()


def _clear_loaded_sql() -> None:
    """Clear XML-derived state after a failed load so the next attempt reports the root cause."""
    global _FULLY_LOADED
    _FULLY_LOADED = False
    _SQL_CONTAINER.clear()
    _NS_FILE.clear()
    _JINJA_VARS.clear()
    result_mapping.clear_result_mappings()
    _BIND_VARS.clear()
    _STATEMENT_KINDS.clear()


def load_mapper(path: str | Path) -> None:
    """加载 mapper XML(文件或目录)。格式与 batisx 相同:
    <mapper namespace="..."><select id="...">SQL</select>...</mapper>
    """
    path = Path(path)
    files = [path] if path.is_file() else sorted(path.rglob("*.xml"))
    for file in files:
        root = ET.parse(file).getroot()
        namespace = root.attrib.get("namespace", "")
        # 与 MyBatis 同约定: 一个 XML 映射一个 mapper(namespace), 不交叉。
        # 同一 namespace 出现在第二个文件 = 条目散落, 直接拒绝加载。
        if namespace in _NS_FILE and _NS_FILE[namespace] != file:
            raise ValueError(
                f"namespace '{namespace}' 已属于 {_NS_FILE[namespace]}, "
                f"不允许再出现在 {file} (1 XML ↔ 1 mapper, 不交叉)")
        _NS_FILE[namespace] = file

        # 第一轮: 收声明块 —— <sql> 复用片段 与 <resultMap> 结果映射(都不是可执行条目)
        frag_els: dict[str, ET.Element] = {}
        for child in root:
            if child.tag == "sql":
                frag_id = child.attrib.get("id")
                if not frag_id:
                    raise ValueError(f"<sql> 片段缺 id: {file}")
                if frag_id in frag_els:
                    raise ValueError(f"<sql> 片段 id 重复: {namespace}.{frag_id}")
                frag_els[frag_id] = child
            elif child.tag == "resultMap":
                result_mapping.load_result_map(namespace, child, file)

        # 第二轮: 可执行条目, 展开 <include> 后编译
        for child in root:
            if child.tag in ("sql", "resultMap"):
                continue
            sql_id = child.attrib.get("id")
            if not sql_id:
                raise ValueError(f"mapper 条目缺 id: {file}")
            full_id = f"{namespace}.{sql_id}"
            if full_id in _SQL_CONTAINER:
                raise ValueError(f"sql_id 重复: {full_id}")
            sql = _element_sql(child, frag_els, full_id, file)
            _reject_interpolation(full_id, sql, file)
            _reject_cast_suffix(full_id, sql, file)
            _SQL_CONTAINER[full_id] = Template(sql)
            _STATEMENT_KINDS[full_id] = child.tag.lower()
            result_mapping.load_result_spec(namespace, full_id, child, file)
            # 模板里 {% if xxx %} 引用的变量名(不含 :name 绑参) —— 首次调用时与方法签名比对,
            # 抓"XML 写了签名没声明的名字"这类拼写错。
            # ⚠ 不用 StrictUndefined 实现: 它会让 render_sql() 这个调试/自省入口无法只传部分参数,
            #   而且拦不住 `x is not none`(identity 测试不触发 Undefined 报错)。
            _JINJA_VARS[full_id] = meta.find_undeclared_variables(_JINJA_ENV.parse(sql))
            _BIND_VARS[full_id] = named_parameter_names(sql)
            _verify_bind_contract(full_id)

        # 该 namespace 的 Mapper 类若已 import, 立刻做双向对齐(见 _verify_namespace)
        _verify_namespace(namespace)


def _element_sql(elem: ET.Element, frag_els: dict[str, ET.Element], full_id: str,
                 file: Path, depth: int = 0) -> str:
    """把一个条目/片段元素的内容拼成 SQL 文本, 就地展开 <include refid="x"/>。

    按文档顺序取 text 与各子元素的 tail, 所以 `SQL <include/> SQL` 前后文本都不丢。
    片段可嵌套引用片段(限深 _MAX_FRAGMENT_DEPTH 防环)。加载期一次性完成, 运行期零开销。
    """
    if depth > _MAX_FRAGMENT_DEPTH:
        raise ValueError(
            f"mapper 条目 '{full_id}' ({file.name}) <include> 嵌套超过 "
            f"{_MAX_FRAGMENT_DEPTH} 层, 疑似循环引用")
    parts = [elem.text or ""]
    for sub in elem:
        if sub.tag != "include":
            raise ValueError(
                f"mapper 条目 '{full_id}' ({file.name}) 含不支持的子元素 <{sub.tag}> "
                "(本 runtime 只支持 <include refid=\"…\"/>; 条件判断请用 jinja2 {% if %})")
        refid = sub.attrib.get("refid")
        if not refid or refid not in frag_els:
            raise ValueError(
                f"mapper 条目 '{full_id}' ({file.name}) 的 <include refid=\"{refid}\"/> "
                f"未声明 (可用片段: {sorted(frag_els)})")
        parts.append(_element_sql(frag_els[refid], frag_els, full_id, file, depth + 1))
        parts.append(sub.tail or "")
    return "".join(parts).strip()


def _reject_interpolation(full_id: str, sql: str, file: Path) -> None:
    """加载期拦截 {{ 值 }} 插值 —— 它会把值直接拼进 SQL(实测可注入), 必须用 :name 绑参。"""
    for interpolation_match in _JINJA_INTERP.finditer(sql):
        expression = interpolation_match.group(1)
        raise ValueError(
            f"mapper 条目 '{full_id}' ({file.name}) 含 jinja 值插值 '{{{{{expression}}}}}' —— "
            "这会把值拼进 SQL 造成注入。值请改用命名绑参 :name; jinja 只用 {% if %} 控结构。")


def _reject_cast_suffix(full_id: str, sql: str, file: Path) -> None:
    """Require ``CAST(:name AS type)`` instead of ambiguous suffix casts."""
    hits = sorted(set(_CAST_SUFFIX_PARAM.findall(sql)))
    if not hits:
        return
    raise ValueError(
        f"mapper 条目 '{full_id}' ({file.name}) 写了 {[f':{name}::' for name in hits]} —— "
        "命名参数后直接接 PostgreSQL cast 容易产生解析歧义，改写成 `CAST(:name AS type)`。")


def render_sql(full_id: str, **kwargs: Any) -> tuple[str, dict[str, Any]]:
    """渲染动态块 → 抽取渲染后仍存在的 :name → 只保留命中的参数。"""
    _ensure_loaded()
    if full_id not in _SQL_CONTAINER:
        raise KeyError(
            f"mapper 条目 '{full_id}' 不存在 —— 检查 XML 的 namespace/id 是否与 "
            f"@amapper 类的 模块路径.类名.方法名 一致 (已加载 {len(_SQL_CONTAINER)} 条)")
    sql = _SQL_CONTAINER[full_id].render(**kwargs)
    sql = "\n".join(line for line in sql.splitlines() if line.strip())
    names = named_parameter_names(sql)
    params = {
        parameter_name: parameter_value
        for parameter_name, parameter_value in kwargs.items()
        if parameter_name in names
    }
    return sql, params


def _ensure_loaded() -> None:
    """惰性确保 mapper XML 已加载。

    生产走 main.py lifespan 显式加载; 本函数覆盖不走 lifespan 的入口(pytest 的
    ASGITransport、脚本、REPL) —— 让 mapper 声明文件不必自己调 load_all_mappers,
    保持"只声明接口"。幂等且只在未完整加载时扫盘, 正常调用零开销。
    check-then-act 交给 load_all_mappers 里的 _LOAD_LOCK 保原子, 这里的快路径只是免锁。
    """
    if _FULLY_LOADED:
        return
    try:
        load_all_mappers()
    except Exception:
        # 加载中途抛错(XML 语法/绑参契约不符)会留下半加载的容器, 而本函数只在容器为空时
        # 才重试 —— 后续调用会拿着不完整的条目表报"条目不存在", 把真正的加载错误盖掉。
        # 这里失败即清空: 每次调用都重新加载、重新报同一个真实错误。
        _clear_loaded_sql()
        raise


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    rows: list[Any] | None
    rowcount: int

    @property
    def returns_rows(self) -> bool:
        return self.rows is not None


def _command_rowcount(status: str) -> int:
    """Extract affected rows from asyncpg command tags such as ``UPDATE 3``."""
    for token in reversed(status.split()):
        if token.isdigit():
            return int(token)
    return 0


async def _execute(
    connection: ConnectionLike,
    full_id: str,
    **kwargs: Any,
) -> ExecutionResult:
    """Render, compile and execute one statement on an asyncpg connection."""
    _ensure_loaded()
    sql, params = render_sql(full_id, **kwargs)
    compiled = compile_query(sql, params)
    statement_kind = _STATEMENT_KINDS.get(full_id)
    # XML entries carry an explicit statement kind. Directly registered templates
    # (tests/extensions) do not, so infer only from the first executable keyword;
    # searching for SELECT anywhere would misclassify INSERT ... SELECT statements.
    first_keyword = re.match(r"\s*([A-Za-z]+)", sql)
    returns_rows = (
        statement_kind == "select"
        or (
            statement_kind is None
            and first_keyword is not None
            and first_keyword.group(1).upper() in {"SELECT", "SHOW", "VALUES", "EXPLAIN"}
        )
        or contains_sql_keyword(sql, "RETURNING")
    )
    if returns_rows:
        rows = list(await connection.fetch(compiled.sql, *compiled.args))
        return ExecutionResult(rows=rows, rowcount=len(rows))
    status = await connection.execute(compiled.sql, *compiled.args)
    return ExecutionResult(rows=None, rowcount=_command_rowcount(status))


def _declared_defaults(func) -> dict[str, Any]:
    """返回声明参数的默认值快照；调用方省略的参数统一按 None 处理。"""
    declared: dict[str, Any] = {}
    for name, param in inspect.signature(func).parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        declared[name] = None if param.default is inspect.Parameter.empty else param.default
    return declared


def _make_wrapper(full_id: str, func, owner: MapperBase | type[MapperBase]):
    # 签名快照在装饰期取一次(运行期零反射开销)。
    # XML 变量名 vs 签名的比对**不在这里做**: 它在 _verify_bind_contract(加载期+装饰期
    # 各触发一次), wrapper 只管每次调用的 kwargs 合法性 —— 旧版的首调闭包标记在
    # reset/重载后失效, 已废弃。
    defaults = _declared_defaults(func)

    @functools.wraps(func)
    async def wrapper(**kwargs: Any):
        unknown = sorted(set(kwargs) - set(defaults))
        if unknown:
            raise TypeError(
                f"mapper '{full_id}': 传入了签名未声明的参数 {unknown} "
                f"(可用: {sorted(defaults)})")
        # ⚠ 必须按签名补全: jinja2 里未传的名字是 Undefined, 而 `Undefined is not none`
        # 求值为 True —— 不补全会导致"省略参数"仍渲染出该 SET/WHERE 子句, 但绑参缺失而报错
        # (甚至在 `{% if x %}` 写法下静默生成错 SQL)。补全后 jinja 永远看到显式值。
        merged = {**defaults, **kwargs}
        async with owner.acquire_connection() as connection:
            result = await _execute(connection, full_id, **merged)
            if result.returns_rows:
                return result_mapping.shape_rows(full_id, result.rows or [])
            return result.rowcount
    return wrapper


def _verify_bind_contract(full_id: str) -> None:
    """校验 XML 只引用 Mapper 已声明的名字(不追踪 Mapper 的调用方), 两条腿都查:

    - :name 绑定参数 —— 拼写错会在运行期缺绑参
    - {% if %} 引用的 jinja 变量 —— 这条**必须在加载期查**而不是首次调用查:
      拼写错的变量渲染成 Undefined(假值), 它守着的 WHERE 子句被静默吞掉,
      `UPDATE ... {% if valeu %}WHERE ...{% endif %}` 直接退化成全表更新。
      旧实现放在 wrapper 首调、闭包标记只查一次 —— reset_state()/重载 XML 后
      标记还是 True, 新 XML 的拼写错就再也没人查了(2026-08-17 外评抓出)。
    加载期与装饰期各调一次本函数(哪边后到哪边查), 两种 import 顺序都覆盖。
    """
    method_params = _METHOD_PARAMS.get(full_id)
    if method_params is None:
        return
    bind_vars = _BIND_VARS.get(full_id)
    if bind_vars is not None:
        undeclared = sorted(bind_vars - method_params)
        if undeclared:
            raise ValueError(
                f"mapper '{full_id}': XML 绑定参数未在 Mapper 方法签名声明 {undeclared} "
                f"(签名: {sorted(method_params)})")
    jinja_vars = _JINJA_VARS.get(full_id)
    if jinja_vars is not None:
        undeclared = sorted(jinja_vars - method_params)
        if undeclared:
            raise ValueError(
                f"mapper '{full_id}': XML 的 {{% if %}} 引用了签名没有的变量 {undeclared} "
                f"—— 拼写错或签名漏了参数 (签名: {sorted(method_params)})")


def _xml_ids(namespace: str) -> set[str]:
    """该 namespace 下已加载的可执行条目 id(不含 <sql> 片段与 <resultMap>, 它们不进容器)。"""
    prefix = f"{namespace}."
    return {key[len(prefix):] for key in _SQL_CONTAINER if key.startswith(prefix)}


def _verify_namespace(namespace: str) -> None:
    """XML 条目 ↔ Mapper 方法 双向对齐。两边都已知时才跑, 否则直接返回。

    不校验的后果: 方法漏了 XML 条目, 要等它**第一次被真调用**才 KeyError ——
    冷路径方法(如 retry_outbox)可能上线数周后才炸一个 500。这里改成启动期就抓:
    load_mapper 里 XML 落地后调一次, amapper 装饰类时再调一次(覆盖两种 import 顺序)。
    """
    registered = _NS_METHODS.get(namespace)
    if registered is None or namespace not in _NS_FILE:
        return
    owner, methods = registered
    xml_ids = _xml_ids(namespace)
    missing_sql = sorted(methods - xml_ids)
    missing_method = sorted(xml_ids - methods)
    if not missing_sql and not missing_method:
        return
    raise ValueError(
        f"mapper 绑定不对齐: {owner} ↔ {_NS_FILE[namespace].name}\n"
        f"  方法有、XML 缺条目: {missing_sql or '无'}\n"
        f"  XML 有、类缺方法  : {missing_method or '无'}")


def _check_signature(full_id: str, func) -> None:
    """装饰期校验声明签名: Mapper 不暴露 session, 参数必须 keyword-only。"""
    params = list(inspect.signature(func).parameters.values())
    for param in params:
        if param.kind is not param.KEYWORD_ONLY:
            raise TypeError(
                f"mapper '{full_id}': 参数 '{param.name}' 是 {param.kind.description}, "
                "必须改成 keyword-only —— 参数前加一个 `*`")


def _check_namespace(namespace: str | None, owner: str) -> str:
    if not namespace or namespace == "__main__":
        raise RuntimeError(
            f"amapper: {owner} 定义在直接运行的脚本里, 拿不到模块名 —— "
            "把它移进可 import 的模块, 或显式传 namespace")
    return namespace


class AMapper(MapperBase):
    """Async mapper enhancer backed by implicit asyncpg connections.

    挂函数: namespace 缺省 = 模块路径(func.__module__), fullId = 模块.函数名。
    挂类(仿 MyBatis Mapper 接口): namespace 缺省 = 模块路径.类名, 类里所有公开方法
    自动绑定(方法名 = XML id), 不用逐个写装饰器或 @staticmethod；包装结果固定为类级方法，
    直接 `OrdersRepo.list_xxx(...)` 类级调用, 不需要实例化, 方法签名不写 self/session。
    XML 对应 <mapper namespace="app.repositories.orders_mapper.OrdersMapper">。

    AMapper 继承 MapperBase；被装饰的业务 Mapper 只声明接口，不持有连接。
    SELECT 返回 list[RowMapping](dict 风格行); 其他语句返回受影响行数。
    ⚠ 直接运行的脚本里 __module__ 是 "__main__", mapper 必须定义在可 import 的模块里。

    装饰期硬校验(都属于"不查就要等首次调用才炸"的类型):
    - @classmethod / @property 直接拒(Mapper 只允许普通 async def 声明)
    - 不暴露 session, 全部参数必须 keyword-only(见 _check_signature)
    - 方法集合与 XML 条目双向对齐(见 _verify_namespace)
    """

    def __init__(self, namespace: str | None = None, sql_id: str | None = None):
        self.namespace = namespace
        self.sql_id = sql_id

    def __call__(self, target: _DecoratedT) -> _DecoratedT:
        """Preserve the decorated class/function type for Pyright and IDE callers."""
        return cast(_DecoratedT, self._decorate(target))

    def _decorate(self, target):
        if inspect.isclass(target):
            if self.sql_id is not None:
                raise ValueError("amapper 挂类时不接受 sql_id(方法名即 id)")
            namespace = _check_namespace(
                self.namespace or f"{target.__module__}.{target.__qualname__}",
                f"类 {target.__qualname__}",
            )
            bound: set[str] = set()
            for name, attr in list(vars(target).items()):
                if name.startswith("_"):
                    continue
                # @classmethod / @property 不是普通接口声明，直接拒绝，避免方法体 `...`
                # 被静默保留后调用返回 None。
                if isinstance(attr, (classmethod, property)):
                    raise TypeError(
                        f"amapper: {target.__qualname__}.{name} 声明成 "
                        f"@{type(attr).__name__} —— mapper 方法只写普通 async def，"
                        "不使用方法级装饰器")
                # 声明层只写普通 async def；兼容已有 staticmethod，但最终都统一替换为
                # staticmethod 包装结果，避免实例绑定并保证 Mapper 无状态。
                declared_func = attr.__func__ if isinstance(attr, staticmethod) else attr
                if not inspect.isfunction(declared_func):
                    continue
                full_id = f"{namespace}.{name}"
                _check_signature(full_id, declared_func)
                _METHOD_PARAMS[full_id] = set(_declared_defaults(declared_func))
                _verify_bind_contract(full_id)
                setattr(target, name, staticmethod(_make_wrapper(full_id, declared_func, self)))
                bound.add(name)
            _NS_METHODS[namespace] = (target.__qualname__, bound)
            _verify_namespace(namespace)  # XML 已加载则立刻对齐; 未加载则由 load_mapper 侧补跑
            return target

        namespace = _check_namespace(
            self.namespace or target.__module__,
            f"函数 {target.__name__}",
        )
        full_id = f"{namespace}.{self.sql_id or target.__name__}"
        _check_signature(full_id, target)
        _METHOD_PARAMS[full_id] = set(_declared_defaults(target))
        _verify_bind_contract(full_id)
        return _make_wrapper(full_id, target, self)


# 保留既有小写装饰器用法：@amapper()；该符号本身现在就是 MapperBase 子类。
amapper = AMapper


async def scalar(namespace_sql_id: str, **kwargs: Any) -> Any:
    """便捷标量查询(如 count): 返回首行首列。"""
    async with MapperBase.acquire_connection() as connection:
        result = await _execute(connection, namespace_sql_id, **kwargs)
        if not result.rows:
            raise LookupError(f"mapper 标量查询 '{namespace_sql_id}' 没有返回行")
        return next(iter(dict(result.rows[0]).values()))
