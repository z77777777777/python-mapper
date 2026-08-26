"""Compile named XML parameters into asyncpg positional parameters."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CompiledQuery:
    sql: str
    args: tuple[Any, ...]


def _is_identifier_start(character: str) -> bool:
    return character == "_" or character.isalpha()


def _is_identifier_part(character: str) -> bool:
    return character == "_" or character.isalnum()


def _is_collection_parameter(value: Any) -> bool:
    return isinstance(value, (list, tuple, set, frozenset))


def _dollar_quote_delimiter(sql: str, offset: int) -> str | None:
    if sql[offset] != "$":
        return None
    end = sql.find("$", offset + 1)
    if end < 0:
        return None
    tag = sql[offset + 1:end]
    if tag and (not _is_identifier_start(tag[0]) or not all(_is_identifier_part(c) for c in tag)):
        return None
    return sql[offset:end + 1]


def _scan_named_parameters(sql: str) -> list[tuple[int, int, str]]:
    """Find ``:name`` outside SQL strings, identifiers and comments."""
    found: list[tuple[int, int, str]] = []
    index = 0
    length = len(sql)
    while index < length:
        character = sql[index]
        if character == "'":
            index += 1
            while index < length:
                if sql[index] == "'":
                    if index + 1 < length and sql[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if character == '"':
            index += 1
            while index < length:
                if sql[index] == '"':
                    if index + 1 < length and sql[index + 1] == '"':
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            depth = 1
            index += 2
            while index < length and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        delimiter = _dollar_quote_delimiter(sql, index) if character == "$" else None
        if delimiter is not None:
            end = sql.find(delimiter, index + len(delimiter))
            index = length if end < 0 else end + len(delimiter)
            continue
        if (
            character == ":"
            and not sql.startswith("::", index)
            and (index == 0 or sql[index - 1] != ":")
        ):
            name_start = index + 1
            if name_start < length and _is_identifier_start(sql[name_start]):
                name_end = name_start + 1
                while name_end < length and _is_identifier_part(sql[name_end]):
                    name_end += 1
                found.append((index, name_end, sql[name_start:name_end]))
                index = name_end
                continue
        index += 1
    return found


def named_parameter_names(sql: str) -> set[str]:
    return {name for _, _, name in _scan_named_parameters(sql)}


def compile_query(sql: str, parameters: dict[str, Any]) -> CompiledQuery:
    """Compile ``:name`` parameters and SQLAlchemy-style collection expansion."""
    occurrences = _scan_named_parameters(sql)
    missing = sorted({name for _, _, name in occurrences if name not in parameters})
    if missing:
        raise ValueError(f"SQL 缺少绑定参数: {missing}")

    args: list[Any] = []
    placeholders: dict[str, str] = {}
    output: list[str] = []
    cursor = 0
    for start, end, name in occurrences:
        output.append(sql[cursor:start])
        placeholder = placeholders.get(name)
        if placeholder is None:
            value = parameters[name]
            if _is_collection_parameter(value):
                values = list(value)
                if values:
                    positions = []
                    for item in values:
                        args.append(item)
                        positions.append(f"${len(args)}")
                    placeholder = f"({', '.join(positions)})"
                else:
                    placeholder = "(SELECT NULL WHERE FALSE)"
            else:
                args.append(value)
                placeholder = f"${len(args)}"
            placeholders[name] = placeholder
        output.append(placeholder)
        cursor = end
    output.append(sql[cursor:])
    return CompiledQuery("".join(output), tuple(args))


def contains_sql_keyword(sql: str, keyword: str) -> bool:
    """Check a keyword outside SQL literals and comments."""
    import re

    executable = []
    cursor = 0
    for start, end, _ in _scan_named_parameters(sql):
        executable.append(sql[cursor:start])
        executable.append(" ")
        cursor = end
    executable.append(sql[cursor:])
    text = "".join(executable)
    text = re.sub(r"'(?:''|[^'])*'", " ", text)
    text = re.sub(r'"(?:""|[^"])*"', " ", text)
    text = re.sub(r"--[^\n]*", " ", text)
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.search(rf"\b{re.escape(keyword)}\b", text, re.IGNORECASE) is not None


__all__ = ["CompiledQuery", "compile_query", "contains_sql_keyword", "named_parameter_names"]
