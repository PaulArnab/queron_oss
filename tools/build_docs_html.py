from __future__ import annotations

import html
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT = DOCS / "html"

PAGES = [
    "README.md",
    "architecture.md",
    "internals-guide.md",
    "user-guide.md",
    "commands-reference.md",
    "pipeline-dsl.md",
    "python-api.md",
    "cli-reference.md",
    "datatype-mapping.md",
    "examples.md",
]


def slug(text: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    value = re.sub(r"\s+", "-", value)
    return value or "section"


def inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)

    def _link(match: re.Match[str]) -> str:
        label = match.group(1)
        href = match.group(2)
        if href == "README.md":
            href = "index.html"
        elif href.endswith(".md"):
            href = href[:-3] + ".html"
        return f'<a href="{href}">{label}</a>'

    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, escaped)
    return escaped


def render_table(lines: list[str]) -> str:
    rows = []
    for line in lines:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        rows.append(cells)
    header = rows[0]
    body = rows[2:] if len(rows) > 2 else []
    parts = ["<table>", "<thead><tr>"]
    parts.extend(f"<th>{inline(cell)}</th>" for cell in header)
    parts.append("</tr></thead>")
    if body:
        parts.append("<tbody>")
        for row in body:
            parts.append("<tr>")
            parts.extend(f"<td>{inline(cell)}</td>" for cell in row)
            parts.append("</tr>")
        parts.append("</tbody>")
    parts.append("</table>")
    return "\n".join(parts)


def render_interactive_pipeline(text: str) -> str:
    links = {
        "@queron.csv.ingress": "commands-reference.html#file-ingress",
        "@queron.jsonl.ingress": "commands-reference.html#file-ingress",
        "@queron.parquet.ingress": "commands-reference.html#file-ingress",
        "@queron.file.ingress": "commands-reference.html#file-ingress",
        "@queron.python.ingress": "commands-reference.html#python-ingress",
        "@queron.model.sql": "commands-reference.html#local-sql-model",
        "@queron.check.fail_if_count": "commands-reference.html#checks",
        "@queron.check.fail_if_true": "commands-reference.html#checks",
        "@queron.postgres.ingress": "commands-reference.html#database-ingress",
        "@queron.db2.ingress": "commands-reference.html#database-ingress",
        "@queron.mssql.ingress": "commands-reference.html#database-ingress",
        "@queron.mysql.ingress": "commands-reference.html#database-ingress",
        "@queron.mariadb.ingress": "commands-reference.html#database-ingress",
        "@queron.oracle.ingress": "commands-reference.html#database-ingress",
        "@queron.postgres.lookup": "commands-reference.html#database-lookup",
        "@queron.db2.lookup": "commands-reference.html#database-lookup",
        "@queron.mssql.lookup": "commands-reference.html#database-lookup",
        "@queron.mysql.lookup": "commands-reference.html#database-lookup",
        "@queron.mariadb.lookup": "commands-reference.html#database-lookup",
        "@queron.oracle.lookup": "commands-reference.html#database-lookup",
        "@queron.csv.egress": "commands-reference.html#file-egress",
        "@queron.jsonl.egress": "commands-reference.html#file-egress",
        "@queron.parquet.egress": "commands-reference.html#file-egress",
        "@queron.file.egress": "commands-reference.html#file-egress",
        "@queron.postgres.egress": "commands-reference.html#database-egress",
        "@queron.db2.egress": "commands-reference.html#database-egress",
        "@queron.mssql.egress": "commands-reference.html#database-egress",
        "@queron.mysql.egress": "commands-reference.html#database-egress",
        "@queron.mariadb.egress": "commands-reference.html#database-egress",
        "@queron.oracle.egress": "commands-reference.html#database-egress",
        "queron.ref": "commands-reference.html#sql-template-helpers",
        "queron.source": "commands-reference.html#sql-template-helpers",
        "queron.lookup": "commands-reference.html#sql-template-helpers",
        "queron.var": "commands-reference.html#sql-template-helpers",
        "queron.run_pipeline": "commands-reference.html#compile-and-run-api",
        "queron.PostgresBinding": "python-api.html#runtime-bindings",
        "queron.Db2Binding": "python-api.html#runtime-bindings",
        "queron.MssqlBinding": "python-api.html#runtime-bindings",
        "queron.MysqlBinding": "python-api.html#runtime-bindings",
        "queron.MariaDbBinding": "python-api.html#runtime-bindings",
        "queron.OracleBinding": "python-api.html#runtime-bindings",
    }

    def link_tokens(line: str) -> str:
        escaped = html.escape(line)
        for token in sorted(links, key=len, reverse=True):
            escaped_token = html.escape(token)
            href = links[token]
            escaped = escaped.replace(escaped_token, f'<a href="{href}">{escaped_token}</a>')
        return escaped or " "

    rows = []
    for number, line in enumerate(text.splitlines(), start=1):
        rows.append(
            '<div class="code-row">'
            f'<a class="line-no" id="pipeline-L{number}" href="#pipeline-L{number}">{number}</a>'
            f'<code>{link_tokens(line)}</code>'
            "</div>"
        )
    return (
        '<section class="interactive-code">'
        '<div class="code-toolbar">'
        '<strong>pipeline.py</strong>'
        '<span>Click linked decorators, helpers, and bindings for their reference.</span>'
        "</div>"
        '<div class="code-scroll">'
        + "\n".join(rows)
        + "</div></section>"
    )


def render_markdown(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    parts: list[str] = []
    title = "Queron Documentation"
    i = 0
    in_ul = False
    in_ol = False

    def close_lists() -> None:
        nonlocal in_ul, in_ol
        if in_ul:
            parts.append("</ul>")
            in_ul = False
        if in_ol:
            parts.append("</ol>")
            in_ol = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("```"):
            close_lists()
            lang = stripped[3:].strip()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            code_text = html.escape("\n".join(code_lines))
            if lang == "mermaid":
                parts.append(f'<pre class="mermaid">{code_text}</pre>')
            else:
                lang_class = f" language-{html.escape(lang)}" if lang else ""
                parts.append(f'<pre><code class="{lang_class}">{code_text}</code></pre>')
            i += 1
            continue

        if stripped == ":::pipeline-example":
            close_lists()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip() != ":::end-pipeline-example":
                code_lines.append(lines[i])
                i += 1
            parts.append(render_interactive_pipeline("\n".join(code_lines)))
            if i < len(lines):
                i += 1
            continue

        if stripped == "{{PIPELINE_EXAMPLE}}":
            close_lists()
            parts.append("<p><strong>Pipeline example source is missing.</strong></p>")
            i += 1
            continue

        if stripped.startswith("|") and stripped.endswith("|"):
            close_lists()
            table_lines = [line]
            i += 1
            while i < len(lines) and lines[i].strip().startswith("|") and lines[i].strip().endswith("|"):
                table_lines.append(lines[i])
                i += 1
            parts.append(render_table(table_lines))
            continue

        heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if heading:
            close_lists()
            level = len(heading.group(1))
            text_value = heading.group(2).strip()
            if level == 1:
                title = text_value
            parts.append(f'<h{level} id="{slug(text_value)}">{inline(text_value)}</h{level}>')
            i += 1
            continue

        if stripped.startswith("- "):
            if in_ol:
                parts.append("</ol>")
                in_ol = False
            if not in_ul:
                parts.append("<ul>")
                in_ul = True
            parts.append(f"<li>{inline(stripped[2:].strip())}</li>")
            i += 1
            continue

        ordered = re.match(r"^\d+\.\s+(.+)$", stripped)
        if ordered:
            if in_ul:
                parts.append("</ul>")
                in_ul = False
            if not in_ol:
                parts.append("<ol>")
                in_ol = True
            parts.append(f"<li>{inline(ordered.group(1).strip())}</li>")
            i += 1
            continue

        if not stripped:
            close_lists()
            i += 1
            continue

        close_lists()
        parts.append(f"<p>{inline(stripped)}</p>")
        i += 1

    close_lists()
    return title, "\n".join(parts)


def page_template(title: str, body: str, nav: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} - Queron Docs</title>
  <link rel="stylesheet" href="styles.css?v=2">
  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    mermaid.initialize({{ startOnLoad: true, securityLevel: 'loose' }});
  </script>
</head>
<body>
  <aside>
    <div class="brand">Queron Docs</div>
    {nav}
  </aside>
  <main>
    {body}
  </main>
</body>
</html>
"""


def build() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    nav_items = []
    for name in PAGES:
        target = "index.html" if name == "README.md" else Path(name).with_suffix(".html").name
        label = "Home" if name == "README.md" else Path(name).stem.replace("-", " ").title()
        nav_items.append(f'<a href="{target}">{html.escape(label)}</a>')
    nav = "<nav>" + "\n".join(nav_items) + "</nav>"

    css = """
:root { color-scheme: light; --fg: #172026; --muted: #5d6b73; --line: #d9e1e6; --bg: #f7f9fb; --accent: #0f766e; }
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.55 system-ui, -apple-system, Segoe UI, sans-serif; color: var(--fg); background: white; }
aside { position: fixed; inset: 0 auto 0 0; width: 260px; overflow: auto; padding: 24px 18px; background: var(--bg); border-right: 1px solid var(--line); }
main { max-width: 1060px; margin-left: 260px; padding: 36px 56px 80px; }
.brand { font-weight: 700; font-size: 18px; margin-bottom: 18px; }
nav a { display: block; color: var(--fg); text-decoration: none; padding: 7px 8px; border-radius: 6px; }
nav a:hover { background: #eaf1f4; color: var(--accent); }
h1, h2, h3, h4 { line-height: 1.2; margin: 28px 0 12px; }
h1 { font-size: 34px; margin-top: 0; }
h2 { border-top: 1px solid var(--line); padding-top: 24px; }
p { margin: 10px 0; }
code { background: #eef3f5; padding: 2px 5px; border-radius: 4px; font-size: 0.92em; }
pre { overflow: auto; background: #fff !important; color: var(--fg) !important; padding: 16px; border-radius: 8px; border: 1px solid var(--line); }
pre code, pre code[class*="language-"] { background: transparent !important; color: inherit !important; padding: 0; }
table { width: 100%; border-collapse: collapse; margin: 16px 0 24px; font-size: 14px; }
th, td { border: 1px solid var(--line); padding: 8px 10px; vertical-align: top; }
th { background: var(--bg); text-align: left; }
ul, ol { padding-left: 24px; }
.mermaid { background: #fff; color: var(--fg); border: 1px solid var(--line); }
.interactive-code { border: 1px solid var(--line); border-radius: 8px; overflow: hidden; margin: 18px 0 28px; background: #fff; }
.code-toolbar { display: flex; gap: 18px; align-items: center; justify-content: space-between; padding: 12px 14px; color: var(--fg); border-bottom: 1px solid var(--line); background: var(--bg); }
.code-toolbar span { color: var(--muted); font-size: 13px; }
.code-scroll { max-height: 720px; overflow: auto; padding: 10px 0; }
.code-row { display: grid; grid-template-columns: 58px 1fr; min-width: 980px; }
.code-row:hover { background: #f3f7f9; }
.code-row code { display: block; white-space: pre; background: transparent; color: var(--fg); padding: 0 16px 0 8px; border-radius: 0; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 13px; line-height: 1.55; }
.code-row code a { color: #0f766e; text-decoration: none; border-bottom: 1px solid rgba(15, 118, 110, .35); }
.code-row code a:hover { color: #0b5f58; border-bottom-color: #0b5f58; }
.line-no { color: #8a99a3; text-align: right; padding-right: 12px; text-decoration: none; font: 13px/1.55 ui-monospace, SFMono-Regular, Consolas, monospace; user-select: none; }
.line-no:hover { color: var(--fg); }
@media (max-width: 860px) { aside { position: static; width: auto; border-right: 0; border-bottom: 1px solid var(--line); } main { margin-left: 0; padding: 24px; } }
"""
    (OUT / "styles.css").write_text(css.strip() + "\n", encoding="utf-8")

    for name in PAGES:
        source = DOCS / name
        if not source.exists():
            continue
        title, body = render_markdown(source.read_text(encoding="utf-8"))
        target = OUT / ("index.html" if name == "README.md" else source.with_suffix(".html").name)
        target.write_text(page_template(title, body, nav), encoding="utf-8")


if __name__ == "__main__":
    build()
