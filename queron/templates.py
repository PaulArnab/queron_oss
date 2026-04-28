from __future__ import annotations

import re

REF_PATTERN = re.compile(r"\{\{\s*queron\.ref\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)\s*\}\}")
SOURCE_PATTERN = re.compile(r"\{\{\s*queron\.source\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)\s*\}\}")
LOOKUP_PATTERN = re.compile(r"\{\{\s*queron\.lookup\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)\s*\}\}")
RAW_COMPOUND_RELATION_PATTERN = re.compile(
    r"\b(?:from|join)\s+(?!\()(?P<relation>(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*))\s*\.\s*(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*))(?:\s*\.\s*(?:(?:\"[^\"]+\"|[A-Za-z_][\w$]*)))?)",
    re.IGNORECASE,
)


def extract_refs(sql: str) -> list[str]: 
    return [match.group("name").strip() for match in REF_PATTERN.finditer(str(sql or ""))]


def extract_sources(sql: str) -> list[str]:
    return [match.group("name").strip() for match in SOURCE_PATTERN.finditer(str(sql or ""))]


def extract_lookups(sql: str) -> list[str]:
    return [match.group("name").strip() for match in LOOKUP_PATTERN.finditer(str(sql or ""))]


def render_sql(
    sql: str,
    *,
    resolve_ref,
    resolve_source,
    resolve_lookup=None,
) -> str:
    rendered = str(sql or "")
    rendered = SOURCE_PATTERN.sub(lambda match: resolve_source(match.group("name").strip()), rendered)
    if resolve_lookup is not None:
        rendered = LOOKUP_PATTERN.sub(lambda match: resolve_lookup(match.group("name").strip()), rendered)
    rendered = REF_PATTERN.sub(lambda match: resolve_ref(match.group("name").strip()), rendered)
    return rendered


def find_raw_compound_relations(sql: str) -> list[str]:
    text = str(sql or "")
    scrubbed = SOURCE_PATTERN.sub("__queron_source__", text)
    scrubbed = LOOKUP_PATTERN.sub("__queron_lookup__", scrubbed)
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
    text = LOOKUP_PATTERN.sub(
        "__queron_lookup__",
        SOURCE_PATTERN.sub("__queron_source__", REF_PATTERN.sub("__queron_ref__", str(sql or ""))),
    )
    return bool(pattern.search(text))


def build_init_pipeline_text(*, sample: bool = False) -> str:
    if sample:
        return """import queron

queron.pipeline(pipeline_id="sample_pipeline")


@queron.model.sql(
    name="seed_numbers",
    out="seed_numbers",
    query=\"\"\"
SELECT 1 AS id
UNION ALL
SELECT 2 AS id
UNION ALL
SELECT 3 AS id
\"\"\",
)
def seed_numbers():
    pass


@queron.model.sql(
    name="enriched_numbers",
    out="enriched_numbers",
    query=f\"\"\"
SELECT
  id,
  id * 10 AS scaled_value
FROM {queron.ref("seed_numbers")}
\"\"\",
)
def enriched_numbers():
    pass


@queron.csv.egress(
    name="export_enriched_numbers",
    out="export_enriched_numbers",
    path="exports/enriched_numbers.csv",
    sql=f\"\"\"
SELECT *
FROM {queron.ref("enriched_numbers")}
\"\"\",
    overwrite=True,
)
def export_enriched_numbers():
    pass
"""

    return '''"""Starter Queron pipeline project."""

import queron

queron.pipeline(pipeline_id="starter_pipeline")

# Add pipeline nodes here.
# To replace this starter with the built-in sample scaffold, run:
#   queron init . --sample --force

# Example:
#
# @queron.model.sql(
#     name="seed",
#     out="seed",
#     query="""
# SELECT 1 AS id
# """,
# )
# def seed():
#     pass
'''


def build_init_config_text() -> str:
    return """target: dev

sources: {}
"""


def build_init_gitignore_text() -> str:
    return """.queron/
__pycache__/
*.pyc
.venv/
"""
