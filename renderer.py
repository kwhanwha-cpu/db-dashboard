"""Render HTML report from collected data."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


TEMPLATES_DIR = Path(__file__).parent / "templates"


def _fmt_num(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:,.{digits}f}"


def _fmt_change(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,.{digits}f}"


def _fmt_pct(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{digits}f}%"


def render(data: dict[str, Any], summary: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(["html"]),
    )
    env.filters["num"] = _fmt_num
    env.filters["change"] = _fmt_change
    env.filters["pct"] = _fmt_pct
    template = env.get_template("report.html.j2")
    return template.render(data=data, summary=summary)


def save(html: str, base_dir: Path, generated_at: str) -> tuple[Path, Path]:
    """Save the HTML to a timestamped archive path AND a stable `dashboard.html`
    that the browser can poll/refresh. Returns (timestamped_path, dashboard_path)."""
    dt = datetime.fromisoformat(generated_at)
    out_dir = base_dir / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    base_dir.mkdir(parents=True, exist_ok=True)

    archive_path = out_dir / f"snapshot_{dt.strftime('%Y%m%d_%H%M%S')}.html"
    dashboard_path = base_dir / "dashboard.html"

    archive_path.write_text(html, encoding="utf-8")
    dashboard_path.write_text(html, encoding="utf-8")
    return archive_path, dashboard_path
