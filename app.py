"""Web UI to compare FD rates loaded from configurable sources."""
from __future__ import annotations

import json
import re
import sqlite3

from flask import Flask, jsonify, render_template, request

from config import DATABASE_PATH, SOURCES_PATH
from scrapers import init_db, refresh_source

app = Flask(__name__)

CUSTOM_TENOR_BRACKETS: list[dict[str, int | str]] = [
    {"min_days": 7, "max_days": 14, "label": "7-14 days"},
    {"min_days": 15, "max_days": 30, "label": "15-30 days"},
    {"min_days": 30, "max_days": 45, "label": "30-45 days"},
    {"min_days": 45, "max_days": 90, "label": "45-90 days"},
    {"min_days": 90, "max_days": 180, "label": "90-180 days"},
    {"min_days": 180, "max_days": 210, "label": "180-210 days"},
    {"min_days": 210, "max_days": 270, "label": "210-270 days"},
    {"min_days": 270, "max_days": 365, "label": "270-365 days"},
    {"min_days": 365, "max_days": 449, "label": "1 year to 15 months"},
    {"min_days": 450, "max_days": 539, "label": "15 months to 18 months"},
    {"min_days": 540, "max_days": 730, "label": "18 months to 2 years"},
    {"min_days": 731, "max_days": 1095, "label": "2-3 years"},
    {"min_days": 1096, "max_days": 1460, "label": "3-4 years"},
    {"min_days": 1461, "max_days": 1825, "label": "4-5 years"},
]


def get_conn() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def load_sources() -> list[dict]:
    if not SOURCES_PATH.is_file():
        return []
    with open(SOURCES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return list(data.get("sources") or [])


def _shortest_tenor_label(labels: list[str]) -> str | None:
    labels = [x.strip() for x in labels if x and str(x).strip()]
    if not labels:
        return None
    return min(labels, key=len)


def _clean_bank_display_name(name: str) -> str:
    t = (name or "").strip()
    # Remove trailing product descriptors in parentheses.
    t = re.sub(r"\s*\([^)]*\)\s*$", "", t).strip()
    return t or name


def _overlap_days(a_min: int, a_max: int, b_min: int, b_max: int) -> int:
    lo = max(a_min, b_min)
    hi = min(a_max, b_max)
    if hi < lo:
        return 0
    return hi - lo + 1


def _pick_best_overlapping_row(
    rows: list[sqlite3.Row],
    bucket_min: int,
    bucket_max: int,
) -> sqlite3.Row | None:
    """
    Pick one source row for a canonical bucket.
    Priority:
    1) max overlap days
    2) smaller source span
    3) lower min_days (stable tie-break)
    """
    best: tuple[int, int, int] | None = None
    best_row: sqlite3.Row | None = None
    for r in rows:
        src_min = r["min_days"]
        src_max = r["max_days"]
        if src_min is None or src_max is None:
            continue
        ov = _overlap_days(int(src_min), int(src_max), bucket_min, bucket_max)
        if ov <= 0:
            continue
        span = int(src_max) - int(src_min)
        score = (ov, -span, -int(src_min))
        if best is None or score > best:
            best = score
            best_row = r
    return best_row


def build_comparison(conn: sqlite3.Connection) -> dict:
    banks = conn.execute(
        """
        SELECT b.id, b.source_id, b.display_name,
               (SELECT MAX(fetched_at) FROM fd_rates WHERE bank_id = b.id) AS fetched_at
        FROM banks b
        INNER JOIN fd_rates r ON r.bank_id = b.id
        GROUP BY b.id
        ORDER BY b.display_name
        """
    ).fetchall()
    bank_list = []
    for row in banks:
        item = dict(row)
        item["display_name"] = _clean_bank_display_name(str(item.get("display_name") or ""))
        bank_list.append(item)
    if not bank_list:
        return {"banks": [], "rows": []}

    rates = conn.execute(
        """
        SELECT bank_id, min_days, max_days, tenor_label, rate_general, rate_senior
        FROM fd_rates
        WHERE min_days IS NOT NULL AND max_days IS NOT NULL
        ORDER BY bank_id, min_days, max_days
        """
    ).fetchall()

    rates_by_bank: dict[int, list[sqlite3.Row]] = {}
    for r in rates:
        rates_by_bank.setdefault(int(r["bank_id"]), []).append(r)

    rows_out: list[dict] = []
    for bracket in CUSTOM_TENOR_BRACKETS:
        mn = int(bracket["min_days"])
        mx = int(bracket["max_days"])
        label = str(bracket["label"])
        days_subtitle = label if mn < 450 else ""
        cells: dict[str, dict] = {}
        for b in bank_list:
            bid = b["id"]
            bank_rows = rates_by_bank.get(int(bid), [])
            r = _pick_best_overlapping_row(bank_rows, mn, mx)
            key = str(bid)
            if r:
                tl = (r["tenor_label"] or "").strip() or label
                cells[key] = {
                    "tenor_label": tl,
                    "general": r["rate_general"],
                    "senior": r["rate_senior"],
                }
            else:
                cells[key] = {"tenor_label": None, "general": None, "senior": None}

        display_label = label
        rows_out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "label": label,
                "display_label": display_label,
                "days_subtitle": days_subtitle,
                "by_bank_id": cells,
            }
        )

    return {"banks": bank_list, "rows": rows_out}


@app.route("/")
def index():
    conn = get_conn()
    try:
        data = build_comparison(conn)
    finally:
        conn.close()
    return render_template("index.html", comparison=data, sources=load_sources())


@app.route("/api/comparison")
def api_comparison():
    conn = get_conn()
    try:
        return jsonify(build_comparison(conn))
    finally:
        conn.close()


@app.route("/api/sources", methods=["GET"])
def api_sources():
    return jsonify({"sources": load_sources()})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    body = request.get_json(silent=True) or {}
    source_id = (body.get("source_id") or "").strip() or None
    sources = load_sources()
    if source_id:
        sources = [s for s in sources if s.get("id") == source_id]
    if not sources:
        return jsonify({"ok": False, "error": "No matching sources"}), 400

    conn = get_conn()
    results = []
    try:
        for src in sources:
            results.append(refresh_source(conn, src))
    finally:
        conn.close()
    ok = all(r.get("ok") for r in results)
    return jsonify({"ok": ok, "results": results})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
