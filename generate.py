"""Main entrypoint: collect data, run LLM, render HTML."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

import collectors
import analyzer
import renderer


REPORTS_DIR = Path(__file__).parent / "reports"
USAGE_LOG = REPORTS_DIR / "token_usage.json"


def _update_usage_log(usage: dict | None) -> dict:
    """Append this run's usage and return cumulative totals."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    log = {"runs": [], "totals": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "total": 0}}
    if USAGE_LOG.exists():
        try:
            log = json.loads(USAGE_LOG.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    if usage:
        log["runs"].append({
            "at": datetime.now().isoformat(timespec="seconds"),
            "model": usage.get("model"),
            "input": usage.get("input_tokens", 0),
            "output": usage.get("output_tokens", 0),
            "cache_read": usage.get("cache_read_tokens", 0),
            "cache_write": usage.get("cache_write_tokens", 0),
        })
        log["runs"] = log["runs"][-100:]  # keep last 100 runs only
        log["totals"]["input"] += usage.get("input_tokens", 0)
        log["totals"]["output"] += usage.get("output_tokens", 0)
        log["totals"]["cache_read"] += usage.get("cache_read_tokens", 0)
        log["totals"]["cache_write"] += usage.get("cache_write_tokens", 0)
        log["totals"]["total"] = sum(v for k, v in log["totals"].items() if k != "total")
        USAGE_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2))

    return log


def main() -> int:
    load_dotenv()
    print("[1/3] Collecting market data and news...", flush=True)
    data = collectors.collect_all()

    print("[2/3] Generating executive summary via Claude...", flush=True)
    summary = analyzer.generate_executive_summary(data)

    usage_log = _update_usage_log(summary.get("_usage"))
    data["_usage"] = summary.get("_usage")
    data["_usage_totals"] = usage_log["totals"]
    data["_usage_run_count"] = len(usage_log["runs"])

    print("[3/3] Rendering HTML...", flush=True)
    html = renderer.render(data, summary)
    archive, dashboard = renderer.save(html, REPORTS_DIR, data["generated_at"])
    print(f"Dashboard: {dashboard}")
    print(f"Snapshot:  {archive}")
    if summary.get("_usage"):
        u = summary["_usage"]
        print(f"이번 실행 토큰: input {u['input_tokens']:,} / output {u['output_tokens']:,} "
              f"/ cache_read {u['cache_read_tokens']:,} / cache_write {u['cache_write_tokens']:,}")
        print(f"누적 토큰 ({data['_usage_run_count']}회): {usage_log['totals']['total']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
