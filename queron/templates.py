from __future__ import annotations

import re

REF_PATTERN = re.compile(r"\{\{\s*queron\.ref\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)\s*\}\}")
SOURCE_PATTERN = re.compile(r"\{\{\s*queron\.source\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)\s*\}\}")
RAW_COMPOUND_RELATION_PATTERN = re.compile(
    r"\b(?:from|join)\s+(?!\()(?P<relation>(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*))\s*\.\s*(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*))(?:\s*\.\s*(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*)))?)",
    re.IGNORECASE,
)


def extract_refs(sql: str) -> list[str]:
    return [match.group("name").strip() for match in REF_PATTERN.finditer(str(sql or ""))]


def extract_sources(sql: str) -> list[str]:
    return [match.group("name").strip() for match in SOURCE_PATTERN.finditer(str(sql or ""))]


def render_sql(
    sql: str,
    *,
    resolve_ref,
    resolve_source,
) -> str:
    rendered = str(sql or "")
    rendered = SOURCE_PATTERN.sub(lambda match: resolve_source(match.group("name").strip()), rendered)
    rendered = REF_PATTERN.sub(lambda match: resolve_ref(match.group("name").strip()), rendered)
    return rendered


def find_raw_compound_relations(sql: str) -> list[str]:
    text = str(sql or "")
    scrubbed = SOURCE_PATTERN.sub("__queron_source__", text)
    scrubbed = REF_PATTERN.sub("__queron_ref__", scrubbed)
    relations: list[str] = []
    for match in RAW_COMPOUND_RELATION_PATTERN.finditer(scrubbed):
        relation = match.group("relation").strip()
        if "__queron_" in relation:
            continue
        relations.append(relation)
    return relations


def has_raw_reference_to_name(sql: str, name: str) -> bool:
    token = re.escape(str(name or "").strip())
    if not token:
        return False
    pattern = re.compile(rf"\b(?:from|join)\s+(?!\()(?P<relation>{token})\b", re.IGNORECASE)
    text = SOURCE_PATTERN.sub("__queron_source__", REF_PATTERN.sub("__queron_ref__", str(sql or "")))
    return bool(pattern.search(text))
