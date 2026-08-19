"""MyBatis-style result declarations, validation, and row materialization."""
from __future__ import annotations

import importlib
import inspect
import xml.etree.ElementTree as ET
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, NamedTuple

from cyt_pymapper.errors import TooManyResultsError


class ResultSpec(NamedTuple):
    dotted: str
    column_map: dict[str, str] | None
    single: bool


# XML result declarations and imported model types are owned by this module.
_RESULT_SPEC: dict[str, ResultSpec] = {}
_RESULT_MAP_DEFS: dict[str, tuple[str, dict[str, str]]] = {}
_TYPE_CACHE: dict[str, type] = {}


def clear_result_mappings() -> None:
    """Clear all XML-derived result declarations and resolved model classes."""
    _RESULT_SPEC.clear()
    _RESULT_MAP_DEFS.clear()
    _TYPE_CACHE.clear()


def load_result_map(namespace: str, elem: ET.Element, file: Path) -> None:
    """Register one explicit ``resultMap`` declaration from mapper XML."""
    map_id = elem.attrib.get("id")
    dotted = elem.attrib.get("type")
    if not map_id or not dotted:
        raise ValueError(f"<resultMap> 必须带 id 与 type: {file}")
    key = f"{namespace}.{map_id}"
    if key in _RESULT_MAP_DEFS:
        raise ValueError(f"<resultMap> id 重复: {key}")
    column_map: dict[str, str] = {}
    for sub in elem:
        if sub.tag not in ("result", "id"):
            raise ValueError(
                f"<resultMap {map_id}> 含不支持的子元素 <{sub.tag}> "
                f"({file.name}; 只支持 <result>/<id>)"
            )
        column, property_name = sub.attrib.get("column"), sub.attrib.get("property")
        if not column or not property_name:
            raise ValueError(
                f"<resultMap {map_id}> 的 <{sub.tag}> 必须带 column 与 property "
                f"({file.name})"
            )
        column_map[column] = property_name
    _RESULT_MAP_DEFS[key] = (dotted, column_map)


def load_result_spec(
    namespace: str,
    full_id: str,
    elem: ET.Element,
    file: Path,
) -> None:
    """Register ``resultType``/``resultMap`` and strict single-row semantics."""
    result_map = elem.attrib.get("resultMap")
    result_type = elem.attrib.get("resultType")
    single = str(elem.attrib.get("single", "")).lower() in ("1", "true", "yes")
    if result_map and result_type:
        raise ValueError(
            f"条目 '{full_id}' ({file.name}) 不能同时给 resultMap 与 resultType"
        )
    if result_map:
        key = f"{namespace}.{result_map}"
        if key not in _RESULT_MAP_DEFS:
            available = sorted(
                result_key.split(".")[-1]
                for result_key in _RESULT_MAP_DEFS
                if result_key.startswith(namespace + ".")
            )
            raise ValueError(
                f"条目 '{full_id}' ({file.name}) 的 resultMap=\"{result_map}\" 未声明 "
                f"(可用: {available})"
            )
        dotted, column_map = _RESULT_MAP_DEFS[key]
        _RESULT_SPEC[full_id] = ResultSpec(dotted, column_map, single)
    elif result_type:
        _RESULT_SPEC[full_id] = ResultSpec(result_type, None, single)
    elif single:
        _RESULT_SPEC[full_id] = ResultSpec("", None, True)


def _resolve_type(dotted: str, full_id: str) -> type:
    cached = _TYPE_CACHE.get(dotted)
    if cached is not None:
        return cached
    module_path, _, class_name = dotted.rpartition(".")
    if not module_path:
        raise ValueError(
            f"条目 '{full_id}' 的 resultType '{dotted}' 不是合法点路径(要 模块.类名)"
        )
    try:
        model_class = getattr(importlib.import_module(module_path), class_name)
    except (ImportError, AttributeError) as exc:
        raise ValueError(
            f"条目 '{full_id}' 的 resultType '{dotted}' 无法导入: {exc}"
        ) from exc
    if not inspect.isclass(model_class):
        raise ValueError(
            f"条目 '{full_id}' 的 resultType '{dotted}' 指向的不是类: "
            f"{type(model_class).__name__}"
        )
    _TYPE_CACHE[dotted] = model_class
    return model_class


def _model_init_fields(model_class: type) -> frozenset[str] | None:
    """Return constructor fields; ``None`` means arbitrary kwargs are accepted."""
    if is_dataclass(model_class):
        return frozenset(field.name for field in fields(model_class) if field.init)

    pydantic_fields = getattr(model_class, "model_fields", None)
    if isinstance(pydantic_fields, dict):
        return frozenset(pydantic_fields)

    annotations: dict[str, Any] = {}
    for base_class in reversed(model_class.__mro__):
        annotations.update(getattr(base_class, "__annotations__", {}))
    if annotations:
        return frozenset(annotations)

    try:
        parameters = inspect.signature(model_class).parameters.values()
    except (TypeError, ValueError):
        return frozenset()
    if any(parameter.kind is parameter.VAR_KEYWORD for parameter in parameters):
        return None
    return frozenset(
        parameter.name
        for parameter in parameters
        if parameter.kind
        in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY)
    )


def _validate_result_spec(full_id: str, spec: ResultSpec) -> None:
    """Validate type paths and properties explicitly forced by ``resultMap``."""
    if not spec.dotted:
        return
    model_class = _resolve_type(spec.dotted, full_id)
    if spec.column_map is None:
        return
    model_fields = _model_init_fields(model_class)
    if model_fields is None:
        return
    unknown_properties = sorted(set(spec.column_map.values()) - model_fields)
    if unknown_properties:
        raise ValueError(
            f"条目 '{full_id}' 的 resultMap 显式映射了模型 {spec.dotted} 不存在的属性 "
            f"{unknown_properties} (可用: {sorted(model_fields)})"
        )


def validate_result_types() -> None:
    """Validate every result type after all mapper XML files have loaded."""
    for full_id, spec in list(_RESULT_SPEC.items()):
        _validate_result_spec(full_id, spec)


def shape_rows(full_id: str, rows: list[Any]) -> Any:
    """Auto-map known fields, ignore extra columns, and enforce result cardinality."""
    spec = _RESULT_SPEC.get(full_id)
    if spec is None:
        return rows
    if spec.dotted:
        model_class = _resolve_type(spec.dotted, full_id)
        _validate_result_spec(full_id, spec)
        model_fields = _model_init_fields(model_class)
        built = []
        for row in rows:
            row_data: dict[str, Any] = dict(row)
            if spec.column_map is not None:
                row_data = {
                    spec.column_map.get(column, column): value
                    for column, value in row_data.items()
                }
            if model_fields is not None:
                row_data = {
                    field_name: value
                    for field_name, value in row_data.items()
                    if field_name in model_fields
                }
            try:
                built.append(model_class(**row_data))
            except TypeError as exc:
                raise TypeError(
                    f"条目 '{full_id}' 结果映射到 {spec.dotted} 失败: {exc}; "
                    f"实际填充字段={sorted(row_data)}"
                ) from exc
        rows = built
    if spec.single:
        if len(rows) > 1:
            raise TooManyResultsError(
                f"条目 '{full_id}' 声明 single=true，但查询返回了 {len(rows)} 行"
            )
        return rows[0] if rows else None
    return rows


__all__ = ["ResultSpec", "clear_result_mappings", "load_result_map", "load_result_spec", "shape_rows", "validate_result_types"]
