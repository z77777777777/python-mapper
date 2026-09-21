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


def _is_postgresql_identifier_part(character: str) -> bool:
    """PostgreSQL permits dollar signs after the first identifier character."""
    return character == "$" or _is_identifier_part(character)


def _is_collection_parameter(value: Any) -> bool:
    return isinstance(value, (list, tuple, set, frozenset))


def _dollar_quote_delimiter(sql: str, offset: int) -> str | None:
    if sql[offset] != "$":
        return None
    if offset > 0 and _is_postgresql_identifier_part(sql[offset - 1]):
        return None
    end = sql.find("$", offset + 1)
    if end < 0:
        return None
    tag = sql[offset + 1:end]
    if tag and (not _is_identifier_start(tag[0]) or not all(_is_identifier_part(c) for c in tag)):
        return None
    return sql[offset:end + 1]


def _skip_single_quoted_string(sql: str, quote_offset: int) -> int:
    """Return the first offset after a regular or PostgreSQL E-string."""
    prefix_offset = quote_offset - 1
    uses_backslash_escapes = (
        prefix_offset >= 0
        and sql[prefix_offset] in {"E", "e"}
        and (
            prefix_offset == 0
            or not _is_postgresql_identifier_part(sql[prefix_offset - 1])
        )
    )
    index = quote_offset + 1
    length = len(sql)
    while index < length:
        if uses_backslash_escapes and sql[index] == "\\":
            index = min(length, index + 2)
            continue
        if sql[index] == "'":
            if index + 1 < length and sql[index + 1] == "'":
                index += 2
                continue
            return index + 1
        index += 1
    return length


def _scan_parameters(
    sql: str,
) -> tuple[list[tuple[int, int, str]], list[tuple[int, int, int]]]:
    """Find named and native positional binds outside non-executable regions."""
    named: list[tuple[int, int, str]] = []
    positional: list[tuple[int, int, int]] = []
    index = 0
    length = len(sql)
    while index < length:
        character = sql[index]
        if character == "'":
            index = _skip_single_quoted_string(sql, index)
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
        if character == "$" and index + 1 < length and sql[index + 1].isdigit():
            position_end = index + 2
            while position_end < length and sql[position_end].isdigit():
                position_end += 1
            positional.append((index, position_end, int(sql[index + 1:position_end])))
            index = position_end
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
                named.append((index, name_end, sql[name_start:name_end]))
                index = name_end
                continue
        index += 1
    return named, positional


def _scan_named_parameters(sql: str) -> list[tuple[int, int, str]]:
    return _scan_parameters(sql)[0]


def named_parameter_names(sql: str) -> set[str]:
    return {name for _, _, name in _scan_named_parameters(sql)}


def positional_parameter_numbers(sql: str) -> set[int]:
    """Return native asyncpg ``$n`` binds outside literals and comments."""
    return {number for _, _, number in _scan_parameters(sql)[1]}


def sql_token_parenthesis_depths(sql: str, token: str) -> tuple[int, ...]:
    """Return parenthesis depths for an exact token in executable SQL regions."""
    if not token:
        raise ValueError("token must not be empty")
    depths: list[int] = []
    index = 0
    depth = 0
    length = len(sql)
    while index < length:
        if sql.startswith(token, index):
            depths.append(depth)
            index += len(token)
            continue
        character = sql[index]
        if character == "'":
            index = _skip_single_quoted_string(sql, index)
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
            comment_depth = 1
            index += 2
            while index < length and comment_depth:
                if sql.startswith("/*", index):
                    comment_depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    comment_depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        delimiter = _dollar_quote_delimiter(sql, index) if character == "$" else None
        if delimiter is not None:
            end = sql.find(delimiter, index + len(delimiter))
            index = length if end < 0 else end + len(delimiter)
            continue
        if character == "(":
            depth += 1
        elif character == ")" and depth:
            depth -= 1
        index += 1
    return tuple(depths)


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


def top_level_sql_word_positions(sql: str) -> tuple[tuple[str, int], ...]:
    """Return executable words and offsets at parenthesis depth zero.

    Strings, quoted identifiers, comments and PostgreSQL dollar-quoted bodies are
    skipped. This is deliberately a lexer, not a SQL parser; it is sufficient for
    detecting outer LIMIT/OFFSET/ORDER BY without rejecting limits in subqueries.
    """
    words: list[tuple[str, int]] = []
    index = 0
    depth = 0
    length = len(sql)
    while index < length:
        character = sql[index]
        if character == "'":
            index = _skip_single_quoted_string(sql, index)
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
            comment_depth = 1
            index += 2
            while index < length and comment_depth:
                if sql.startswith("/*", index):
                    comment_depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    comment_depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        delimiter = _dollar_quote_delimiter(sql, index) if character == "$" else None
        if delimiter is not None:
            end = sql.find(delimiter, index + len(delimiter))
            index = length if end < 0 else end + len(delimiter)
            continue
        if character == "(":
            depth += 1
            index += 1
            continue
        if character == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if _is_identifier_start(character):
            end = index + 1
            while end < length and _is_postgresql_identifier_part(sql[end]):
                end += 1
            if depth == 0:
                words.append((sql[index:end].upper(), index))
            index = end
            continue
        index += 1
    return tuple(words)


def top_level_sql_words(sql: str) -> tuple[str, ...]:
    """Return executable words at parenthesis depth zero."""
    return tuple(word for word, _ in top_level_sql_word_positions(sql))


def contains_top_level_keyword(sql: str, keyword: str) -> bool:
    return keyword.upper() in top_level_sql_words(sql)


def contains_top_level_sequence(sql: str, *keywords: str) -> bool:
    expected = tuple(keyword.upper() for keyword in keywords)
    if not expected:
        return False
    words = top_level_sql_words(sql)
    width = len(expected)
    return any(words[index:index + width] == expected for index in range(len(words) - width + 1))


__all__ = [
    "CompiledQuery",
    "compile_query",
    "contains_sql_keyword",
    "contains_top_level_keyword",
    "contains_top_level_sequence",
    "named_parameter_names",
    "positional_parameter_numbers",
    "sql_token_parenthesis_depths",
    "top_level_sql_word_positions",
    "top_level_sql_words",
]
