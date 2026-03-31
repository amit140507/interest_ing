"""Fetch and parse FD rate sources defined in sources.json."""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from config import REQUEST_TIMEOUT, USER_AGENT


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json, text/html, */*"})
    return s


def parse_tenor_to_days(tenor_text: str) -> tuple[int | None, int | None]:
    """Map tenor strings like '7 days to 14 days' to min/max days (best-effort)."""
    t = tenor_text.lower().strip()
    t = re.sub(r"\s+", " ", t)

    def days_from(m: re.Match[str]) -> int:
        return int(m.group(1))

    def months_from(m: re.Match[str]) -> int:
        return int(m.group(1)) * 30

    def years_from(m: re.Match[str]) -> int:
        return int(m.group(1)) * 365

    def parse_side(side: str) -> int | None:
        side = side.strip()
        m = re.search(r"(\d+)\s*years?\s*(\d+)\s*months?\s*(\d+)\s*days?", side)
        if m:
            return int(m.group(1)) * 365 + int(m.group(2)) * 30 + int(m.group(3))
        m = re.search(r"(\d+)\s*years?\s*(\d+)\s*days?", side)
        if m:
            return int(m.group(1)) * 365 + int(m.group(2))
        m = re.search(r"(\d+)\s*months?\s*(\d+)\s*days?", side)
        if m:
            return int(m.group(1)) * 30 + int(m.group(2))
        m = re.search(r"(\d+)\s*years?", side)
        if m:
            return years_from(m)
        m = re.search(r"(\d+)\s*months?", side)
        if m:
            return months_from(m)
        m = re.search(r"(\d+)\s*days?", side)
        if m:
            return days_from(m)
        return None

    m = re.search(r"^\s*(\d+)\s*-\s*(\d+)\s*(days?|months?|years?)\s*$", t)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        unit = m.group(3)
        mult = 1 if "day" in unit else (30 if "month" in unit else 365)
        return min(a, b) * mult, max(a, b) * mult

    if " to " in t:
        left, right = t.split(" to ", 1)
        a, b = parse_side(left), parse_side(right)
        if a is not None and b is not None:
            return min(a, b), max(a, b)
    if "-" in t and "day" in t:
        parts = re.split(r"\s*-\s*", t, maxsplit=1)
        if len(parts) == 2:
            a, b = parse_side(parts[0]), parse_side(parts[1])
            if a is not None and b is not None:
                return min(a, b), max(a, b)

    m = re.search(r"(\d+)\s*days?", t)
    if m:
        d = days_from(m)
        return d, d
    m = re.search(r"(\d+)\s*months?", t)
    if m:
        d = months_from(m)
        return d, d
    m = re.search(r"(\d+)\s*years?", t)
    if m:
        d = years_from(m)
        return d, d
    return None, None


def _float_cell(x: Any) -> float | None:
    if x is None:
        return None
    s = str(x).strip().replace("%", "")
    try:
        return float(s)
    except ValueError:
        return None


def _rate_from_cell_text(text: str) -> float | None:
    """Extract first decimal rate from table cell text (handles *, #, $ footnotes)."""
    if not text:
        return None
    t = text.replace("\xa0", " ").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_kotak_json_array(raw: str) -> list[dict[str, Any]]:
    """
    Kotak get_all_variable_data_latest2.php returns JSON array of
    [min_days, max_days, general_%, senior_%] (strings or numbers).
    """
    data = json.loads(raw.strip())
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, (list, tuple)) or len(row) < 4:
            continue
        try:
            mn = int(float(str(row[0]).strip()))
            mx = int(float(str(row[1]).strip()))
        except (ValueError, TypeError):
            continue
        g = _float_cell(row[2])
        s = _float_cell(row[3])
        if g is None and s is None:
            continue
        label = f"{mn} – {mx} days"
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": label,
                "rate_general": g,
                "rate_senior": s,
            }
        )
    return out


def _normalize_tenor_label(s: str) -> str:
    t = s.replace("\xa0", " ").strip()
    t = re.sub(r"^\*+\s*", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.lower()


def _hdfc_less_than_3cr_tenor_days() -> dict[str, tuple[int, int]]:
    """
    Approximate day ranges for HDFC '< 3 Crore' FD tenor buckets (interest-rates page).
    Used so comparison rows align with other banks' day-based keys.
    """
    pairs: list[tuple[str, int, int]] = [
        ("7 - 14 days", 7, 14),
        ("15-29 days", 15, 29),
        ("30-45 days", 30, 45),
        ("46-60 days", 46, 60),
        ("61-89 days", 61, 89),
        ("90 days <= 6 months", 90, 182),
        ("6 months 1 day <=9 months", 183, 270),
        ("9 months 1 day to < 1 year", 271, 364),
        ("1 year to < 15 months", 365, 449),
        ("15 months to < 18 months", 450, 539),
        ("18 months to < 21 months", 540, 629),
        ("21 months to 2 years", 630, 729),
        ("2 years 1 day to < 2 year 11 months", 730, 1049),
        ("2 years 11 months (35 months)", 1050, 1064),
        ("2 years 11 months 1 day <= 3 year", 1065, 1095),
        ("3 years 1 day to < 4 years 7 months", 1096, 1649),
        ("4 year 7 months (55 months)", 1650, 1650),
        ("4 year 7 months 1 day <=5 years", 1651, 1825),
        ("5 years 1 day to 10 years", 1826, 3650),
    ]
    return {_normalize_tenor_label(a): (b, c) for a, b, c in pairs}


_HDFC_LT3CR = _hdfc_less_than_3cr_tenor_days()


def parse_hdfc_lt3cr_tenor(tenor_raw: str) -> tuple[int | None, int | None]:
    key = _normalize_tenor_label(tenor_raw)
    if key in _HDFC_LT3CR:
        return _HDFC_LT3CR[key]
    return parse_tenor_to_days(tenor_raw)


def parse_html_div_two_rate_columns(html: str, container_id: str) -> list[dict[str, Any]]:
    """
    Table inside #container with columns: tenor, general %, senior %.
    Skips sub-header rows (empty tenor or 'Interest rate' labels).
    """
    if not container_id.strip():
        raise ValueError("container_id is required for html_div_two_rate_columns")
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find(id=container_id.strip())
    if not root:
        raise ValueError(f"Element #{container_id} not found")
    table = root.find("table")
    if not table:
        raise ValueError(f"No <table> inside #{container_id}")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        tenor = cells[0].get_text(strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenor bucket" in tl:
            continue
        if "interest rate" in tl and "per annum" in tl:
            continue
        if "senior citizen" in tl and "per annum" in tl:
            continue

        g = _float_cell(cells[1].get_text())
        sr = _float_cell(cells[2].get_text())
        if g is None and sr is None:
            continue

        mn, mx = parse_hdfc_lt3cr_tenor(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": g,
                "rate_senior": sr,
            }
        )
    return out


def parse_html_sbi_style(html: str) -> list[dict[str, Any]]:
    """First HTML table whose header mentions 'tenors'; tenor col + rate col 3."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        headers = table.find_all("th")
        if not headers or "tenor" not in headers[0].get_text(strip=True).lower():
            continue
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if len(cols) < 3:
                continue
            tenor = cols[0].get_text(strip=True)
            rate_text = cols[2].get_text(strip=True).replace("%", "").strip()
            try:
                rate = float(rate_text)
            except ValueError:
                continue
            mn, mx = parse_tenor_to_days(tenor)
            if mn is None:
                continue
            if mx is None:
                mx = mn
            out.append(
                {
                    "min_days": mn,
                    "max_days": mx,
                    "tenor_label": tenor,
                    "rate_general": rate,
                    "rate_senior": None,
                }
            )
        if out:
            break
    return out


def _parse_icici_tenor_days(tenor: str) -> tuple[int | None, int | None]:
    t = _normalize_tenor_label(tenor)

    m = re.search(r"(\d+)\s*to\s*(\d+)\s*days?", t)
    if m:
        return int(m.group(1)), int(m.group(2))

    m = re.search(r"(\d+)\s*to\s*<\s*(\d+)\s*year", t)
    if m:
        return int(m.group(1)), int(m.group(2)) * 365 - 1

    m = re.search(r"(\d+)\s*year\s*to\s*<\s*(\d+)\s*months?", t)
    if m:
        return int(m.group(1)) * 365, int(m.group(2)) * 30 - 1

    m = re.search(r"(\d+)\s*months?\s*to\s*(\d+)\s*years?", t)
    if m:
        return int(m.group(1)) * 30, int(m.group(2)) * 365

    m = re.search(r"(\d+)\s*years?\s*(\d+)\s*day\s*to\s*(\d+)\s*years?", t)
    if m:
        return int(m.group(1)) * 365 + int(m.group(2)), int(m.group(3)) * 365

    m = re.search(r"(\d+)\s*year\s*to\s*(\d+)\s*days?", t)
    if m:
        return int(m.group(1)) * 365, int(m.group(2))

    return parse_tenor_to_days(tenor)


def _bob_fd_lt3cr_tenor_days() -> dict[str, tuple[int, int]]:
    """Bank of Baroda callable FD below ₹3 Cr — tenor rows in div.fdlandingdata."""
    pairs: list[tuple[str, int, int]] = [
        ("7 days to 14 days", 7, 14),
        ("15 days to 45 days", 15, 45),
        ("46 days to 90 days", 46, 90),
        ("91 days to 180 days", 91, 180),
        ("181 days to 210 days", 181, 210),
        ("211 days to 270 days", 211, 270),
        ("271 days & above and less than 1 year", 271, 364),
        ("1 year", 365, 365),
        ("above 1 year to 400 days", 366, 400),
        ("above 400 days and upto 2 years (except 444 days)", 401, 730),
        ("above 2 years and upto 3 years", 731, 1095),
        ("above 3 years and upto 5 years", 1096, 1825),
        ("above 5 years and upto 10 years", 1826, 3650),
        ("above 10 years (macad only)", 3651, 7300),
        ("bob square drive deposit scheme (444 days)", 444, 444),
    ]
    return {_normalize_tenor_label(a): (b, c) for a, b, c in pairs}


_BOB_FD_LT3CR = _bob_fd_lt3cr_tenor_days()


def parse_bob_fdlanding_tenor(tenor_raw: str) -> tuple[int | None, int | None]:
    key = _normalize_tenor_label(tenor_raw)
    if key in _BOB_FD_LT3CR:
        return _BOB_FD_LT3CR[key]
    return parse_tenor_to_days(tenor_raw)


def parse_html_div_class_fd_table(html: str, container_class: str) -> list[dict[str, Any]]:
    """
    BOB publishes `fdlandingdata` on a <table> (not always a div). We resolve:
    1) <table> whose class list contains `container_class`
    2) else first <table> inside a <div> whose class contains `container_class`

    Expects columns: Tenors | General | Sr. Citizen | Super Sr.
    We store Sr. Citizen as rate_senior for comparison with other banks.
    """
    cls = (container_class or "").strip()
    if not cls:
        raise ValueError("container_class is required for html_div_class_fd_table")

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find(
        "table",
        class_=lambda c: bool(c) and cls in _split_html_classes(c),
    )
    if not table:
        root = soup.find("div", class_=lambda c: bool(c) and cls in _split_html_classes(c))
        if root:
            table = root.find("table")
    if not table:
        raise ValueError(f"No table (or div) with class containing {cls!r}")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 4:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if tl == "tenors" or "tenor" == tl:
            continue
        if "residents" in tl and "general" in tl:
            continue

        gen = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        sr = _rate_from_cell_text(cells[2].get_text(" ", strip=True))
        if gen is None and sr is None:
            continue

        mn, mx = parse_bob_fdlanding_tenor(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def _split_html_classes(c: Any) -> list[str]:
    if isinstance(c, str):
        return c.split()
    if isinstance(c, list):
        return [str(x) for x in c]
    return []


def _looks_like_challenge_page(text: str) -> bool:
    t = (text or "").lower()
    markers = [
        "captcha",
        "cf-challenge",
        "cloudflare",
        "just a moment",
        "attention required",
        "request rejected",
        "radware",
        "incident id",
    ]
    return any(m in t for m in markers)


def parse_icici_fd_interest_json(raw: str) -> list[dict[str, Any]]:
    """
    ICICI fd-interest-rate.json layout:
    { "interestData": [ [ {tenure, c1, c2, ...}, ... ], ... ] }
    We take the first table block (regular FD slab) where c1=general and c2=senior.
    """
    data = json.loads(raw)
    groups = data.get("interestData")
    if not isinstance(groups, list) or not groups:
        return []
    first = groups[0]
    if not isinstance(first, list):
        return []

    out: list[dict[str, Any]] = []
    for row in first:
        if not isinstance(row, dict):
            continue
        tenor = str(row.get("tenure") or "").strip()
        if not tenor:
            continue
        # Skip non-standard line item; it duplicates 5-year tenor and is product-specific.
        if "tax saver" in tenor.lower():
            continue
        mn, mx = _parse_icici_tenor_days(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": _float_cell(row.get("c1")),
                "rate_senior": _float_cell(row.get("c2")),
            }
        )
    return out


def _norm_idfc_tenor_key(s: str) -> str:
    t = s.replace("\xa0", " ")
    t = re.sub(r"[\u2013\u2014]", "-", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def _idfc_lt3cr_main_tenor_days() -> dict[str, tuple[int, int]]:
    """IDFC FIRST main FD table (< ₹3 Cr) — tenor text → approximate day range."""
    pairs: list[tuple[str, int, int]] = [
        ("7 - 14 days", 7, 14),
        ("15 - 29 days", 15, 29),
        ("30 - 45 days", 30, 45),
        ("46 - 90 days", 46, 90),
        ("91 - 180 days", 91, 180),
        ("181 days - less than 1 year", 181, 364),
        ("1 year", 365, 365),
        ("1 year 1 day- 370 days", 366, 370),
        ("371 days to 389 days", 371, 389),
        ("390 days", 390, 390),
        ("391 days - 2 years", 391, 730),
        ("2 years 1 day - 5 years", 731, 1825),
        ("5 years 1 day - 10 years", 1826, 3650),
    ]
    return {_norm_idfc_tenor_key(a): (b, c) for a, b, c in pairs}


_IDFC_LT3CR_MAIN = _idfc_lt3cr_main_tenor_days()


def parse_idfc_fd_tenor(tenor_raw: str) -> tuple[int | None, int | None]:
    key = _norm_idfc_tenor_key(tenor_raw)
    if key in _IDFC_LT3CR_MAIN:
        return _IDFC_LT3CR_MAIN[key]
    return parse_tenor_to_days(tenor_raw)


def _table_has_all_class_tokens(tag: Any, tokens: list[str]) -> bool:
    parts = _split_html_classes(tag.get("class") if hasattr(tag, "get") else None)
    return bool(parts) and all(t in parts for t in tokens)


def parse_idfc_fd_formtable(
    html: str,
    *,
    senior_extra_pct: float | None = 0.5,
) -> list[dict[str, Any]]:
    """
    First <table class='test formtable'> on the FD page: Tenor | rate (< ₹3 Cr).
    The site publishes only the regular rate; senior is optional +senior_extra_pct
    (IDFC states +0.50% p.a. for senior citizens on FDs below ₹3 Cr).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for t in soup.find_all("table"):
        if _table_has_all_class_tokens(t, ["test", "formtable"]):
            table = t
            break
    if not table:
        raise ValueError("No table with classes 'test' and 'formtable'")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenor bucket" in tl:
            continue

        gen = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        if gen is None:
            continue

        sr: float | None
        if senior_extra_pct is None:
            sr = None
        else:
            sr = round(gen + senior_extra_pct, 4)

        mn, mx = parse_idfc_fd_tenor(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def _norm_union_tenor_key(s: str) -> str:
    t = s.replace("\xa0", " ").strip()
    t = re.sub(r"\s+", " ", t)
    return t.lower()


def _union_bank_lt3cr_tenor_days() -> dict[str, tuple[int, int]]:
    """Union Bank Domestic/NRO TD < ₹3 Cr — period text → approximate day range."""
    pairs: list[tuple[str, int, int]] = [
        ("7-14 days", 7, 14),
        ("15 -30 days", 15, 30),
        ("31-45 days", 31, 45),
        ("46 -90 days", 46, 90),
        ("91-120 days", 91, 120),
        ("121-180 days", 121, 180),
        ("181 -270 days", 181, 270),
        ("271-364 days", 271, 364),
        ("1 yr.", 365, 365),
        ("> 1 yr. to 399 days", 366, 399),
        ("400 days", 400, 400),
        ("401 to 443 days", 401, 443),
        ("444 days (new slab)", 444, 444),
        ("445 days to 2 yrs", 445, 730),
        (">2 yrs to 996 days", 731, 996),
        ("997 days", 997, 997),
        (">998 days to 3 yrs", 998, 1095),
        ("> 3 yrs to 10 yrs", 1096, 3650),
    ]
    return {_norm_union_tenor_key(a): (b, c) for a, b, c in pairs}


_UNION_LT3CR = _union_bank_lt3cr_tenor_days()


def parse_union_bank_tenor(tenor_raw: str) -> tuple[int | None, int | None]:
    key = _norm_union_tenor_key(tenor_raw)
    if key in _UNION_LT3CR:
        return _UNION_LT3CR[key]
    return parse_tenor_to_days(tenor_raw)


def parse_union_bank_inner_table(
    html: str,
    *,
    senior_extra_pct: float | None = 0.5,
) -> list[dict[str, Any]]:
    """
    First <div class='inner-table'> on Union Bank rate page: nested <table> with
    Period | rate for < ₹3 Cr. Header uses rowspan; data rows have two <td>s.
    Senior rate defaults to general + senior_extra_pct (site: +0.50% for resident seniors).
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("div", class_=lambda c: bool(c) and "inner-table" in _split_html_classes(c))
    if not root:
        raise ValueError("No div with class 'inner-table'")

    table = root.find("table")
    if not table:
        raise ValueError("No <table> inside div.inner-table")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if tl == "period" or "revised interest rate" in tl:
            continue

        gen = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        if gen is None:
            continue

        if senior_extra_pct is None:
            sr = None
        else:
            sr = round(gen + senior_extra_pct, 4)

        mn, mx = parse_union_bank_tenor(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_pnb_anchor_below_table(
    html: str,
    *,
    anchor_data_tab: str,
) -> list[dict[str, Any]]:
    """
    Parse the first table that appears after the PNB accordion anchor with the
    given data-tab value.
    Expected row shape: Sl No | Period | Public | Senior | Super Senior.
    """
    token = (anchor_data_tab or "").strip()
    if not token:
        raise ValueError("anchor_data_tab is required for pnb_anchor_below_table")

    soup = BeautifulSoup(html, "html.parser")
    anchor = soup.find("a", attrs={"data-tab": token})
    if not anchor:
        raise ValueError(f"No <a> with data-tab={token!r}")

    table = anchor.find_next("table")
    if not table:
        raise ValueError(f"No <table> found after anchor data-tab={token!r}")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 4:
            continue

        tenor = cells[1].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if tl in {"period", "sl. no", "sl no"}:
            continue

        gen = _rate_from_cell_text(cells[2].get_text(" ", strip=True))
        sr = _rate_from_cell_text(cells[3].get_text(" ", strip=True))
        if gen is None and sr is None:
            continue

        m_days = re.search(r"(\d+)\s*to\s*(\d+)\s*days?", tl)
        if m_days:
            mn, mx = int(m_days.group(1)), int(m_days.group(2))
        else:
            mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def _extract_first_n_rates(text: str, n: int) -> list[float]:
    vals: list[float] = []
    for m in re.finditer(r"\d+(?:\.\d+)?", text or ""):
        try:
            vals.append(float(m.group(0)))
        except ValueError:
            continue
        if len(vals) >= n:
            break
    return vals


def parse_canara_msonormal_following_table(html: str) -> list[dict[str, Any]]:
    """
    Canara page: locate <p class='MsoNormal'> containing the term-deposit heading,
    then parse the next table. Tenor is in first cell, followed by rate columns.
    We use first two numeric cells as general/senior for <3 Cr callable section.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for p in soup.find_all("p", class_=lambda c: bool(c) and "MsoNormal" in _split_html_classes(c)):
        t = p.find_next("table")
        if not t:
            continue
        txt = t.get_text(" ", strip=True).lower()
        if "term deposits" in txt and "rate of interest" in txt and "less than rs.3 crore" in txt:
            table = t
            break

    if not table:
        # Fallback to first table that clearly matches domestic < 3 Cr section.
        for t in soup.find_all("table"):
            txt = t.get_text(" ", strip=True).lower()
            if "term deposits" in txt and "rate of interest" in txt and "less than rs.3 crore" in txt:
                table = t
                break
    if not table:
        raise ValueError("No matching Canara term-deposit table found")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "term deposits" in tl or "rate of interest" in tl or "general public" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            # Handles labels like "270 days to less than 1 year"
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*1\s*year", tl)
            if m:
                mn, mx = int(m.group(1)), 364
        if mn is None:
            continue
        if mx is None:
            mx = mn

        rate_cells = [c.get_text(" ", strip=True) for c in cells[1:]]
        rates: list[float] = []
        for rc in rate_cells:
            rates.extend(_extract_first_n_rates(rc, 1))
            if len(rates) >= 2:
                break
        if not rates:
            continue
        gen = rates[0]
        sr = rates[1] if len(rates) > 1 else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_au_first_table(html: str) -> list[dict[str, Any]]:
    """
    AU page: parse the first table on the fixed deposit interest rates page.
    Assumes first column is tenor and next columns include general/senior rates.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on AU FD rates page")

    # Guard against the placeholder snapshot template so AU failures are explicit.
    if "replace this template with a real au fixed-deposit page html snapshot" in html.lower():
        raise ValueError(
            "AU source fallback snapshot is still a template. "
            "Replace snapshots/au_fd_interest_rates.html with real AU page HTML."
        )

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenor" in tl or "maturity" in tl or "period" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue

        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    if not out:
        raise ValueError(
            "AU parser found no rows. Live page is likely blocked and local snapshot is missing/invalid."
        )
    return out


def parse_federal_first_table(html: str) -> list[dict[str, Any]]:
    """
    Federal Bank deposit-rate page: parse the first table
    (Resident Term Deposit Interest Rates).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on Federal Bank deposit-rate page")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "period" in tl or "general public" in tl or "senior citizen" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*1\s*year", tl)
            if m:
                mn, mx = int(m.group(1)), 364
        if mn is None:
            m = re.search(r"above\s*(\d+)\s*months?\s*to\s*less than\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30 + 1, int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"above\s*(\d+)\s*months?\s*to\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30 + 1, int(m.group(2)) * 365
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue
        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_indusind_first_table(html: str) -> list[dict[str, Any]]:
    """
    IndusInd rates page: parse the first table (FD < 3 Cr section).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on IndusInd rates page")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenure" in tl or "rate" in tl or "annualized yield" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*year\s*to\s*below\s*(\d+)\s*year\s*(\d+)\s*months?", tl)
            if m:
                mn = int(m.group(1)) * 365
                mx = int(m.group(2)) * 365 + int(m.group(3)) * 30 - 1
        if mn is None:
            m = re.search(r"above\s*(\d+)\s*years?\s*up to\s*below\s*(\d+)\s*months?", tl)
            if m:
                mn = int(m.group(1)) * 365 + 1
                mx = int(m.group(2)) * 30 - 1
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue
        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_ujjivan_first_table(html: str) -> list[dict[str, Any]]:
    """
    Ujjivan support-interest-rates page: parse table inside div#tabthree
    (Domestic Fixed Deposits and Sampoorna Nidhi).
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("div", id="tabthree")
    if not root:
        raise ValueError("div#tabthree not found on Ujjivan interest-rates page")
    table = root.find("table")
    if not table:
        raise ValueError("No table found inside div#tabthree on Ujjivan interest-rates page")

    senior_extra: float | None = None
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        first = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip().lower()
        if "additional interest rate for senior citizens" in first:
            v = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
            if v is not None:
                senior_extra = v
            break

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenure" in tl or "interest rate" in tl:
            continue
        if "additional interest rate for senior citizens" in tl:
            continue

        # Ujjivan uses patterns like "24 months 1 day to 990 days" and
        # "12 months to < 24 months"; parse these before generic fallback.
        mn, mx = None, None
        m = re.search(r"(\d+)\s*months?\s*1\s*day\s*to\s*(\d+)\s*days?", tl)
        if m:
            mn, mx = int(m.group(1)) * 30 + 1, int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*1\s*day\s*to\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30 + 1, int(m.group(2)) * 30
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 30
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*to\s*<\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 30 - 1
        if mn is None:
            mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        rate = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        if rate is None:
            continue
        sr = round(rate + senior_extra, 4) if senior_extra is not None else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": rate,
                "rate_senior": sr,
            }
        )
    return out


def parse_equitas_overall_interest_pdf(raw_pdf: bytes) -> list[dict[str, Any]]:
    """
    Parse Equitas Overall Interest Rates PDF and extract tenure/rate rows.
    Best effort for < 3 Cr domestic slab lines.
    """
    reader = PdfReader(BytesIO(raw_pdf))
    text = "\n".join((p.extract_text() or "") for p in reader.pages)
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln and ln.strip()]

    out: list[dict[str, Any]] = []
    for ln in lines:
        low = ln.lower()
        if "interest" in low and "rate" in low:
            continue
        if "senior" in low and "additional" in low:
            continue
        if "years" in low and "tax saver" in low:
            continue
        # Keep rows that look like tenure + trailing percent/rate.
        if not re.search(r"(\d+\s*(?:days?|months?|years?))", low):
            continue
        m_rates = re.findall(r"(\d+(?:\.\d+)?)\s*%", ln)
        if not m_rates:
            continue
        # Prefer first percentage in line (deposit rate), later ones are often effective yield.
        rate = float(m_rates[0])
        m_first = re.search(r"(\d+(?:\.\d+)?)\s*%", ln)
        tenor = ln[: m_first.start()].strip(" -:\t") if m_first else ""
        if not tenor:
            continue

        m = re.search(r"(\d+)\s*years?\s*1\s*day\s*(\d+)\s*days?", low)
        if m:
            mn, mx = int(m.group(1)) * 365 + 1, int(m.group(2))
        else:
            mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*to\s*<\s*(\d+)\s*months?", low)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", low)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": rate,
                "rate_senior": None,
            }
        )
    return out


def _extract_equitas_overall_interest_pdf_url(html: str) -> str | None:
    t = html or ""
    # Prefer the explicit "Overall_Interest_Rates" PDF if present.
    m = re.search(
        r"https?://[^\s\"'<>]*overall[_-]?interest[_-]?rates[^\s\"'<>]*\.pdf",
        t,
        flags=re.I,
    )
    if m:
        return m.group(0)
    # Fallback: any PDF URL on page that looks interest/rate related.
    for u in re.findall(r"https?://[^\s\"'<>]+\.pdf", t, flags=re.I):
        ul = u.lower()
        if "interest" in ul and "rate" in ul:
            return u
    return None


def parse_equitas_div_w_full_md_w_3_5(html: str) -> list[dict[str, Any]]:
    """
    Equitas FD page: parse table inside div with classes 'w-full' and 'md:w-3/5'.
    """
    # Guard against placeholder snapshot templates.
    if "replace this template with actual page html from" in html.lower():
        raise ValueError(
            "Equitas source fallback snapshot is still a template. "
            "Replace snapshots/equitas_fd_page.html with real Equitas page HTML."
        )

    soup = BeautifulSoup(html, "html.parser")
    # Legacy selector first (older Equitas markup).
    root = None
    for d in soup.find_all("div"):
        cls = _split_html_classes(d.get("class"))
        if "w-full" in cls and "md:w-3/5" in cls:
            root = d
            break

    table = root.find("table") if root else None
    if table is None:
        # Fallback: pick first table that looks like an FD rates table.
        for t in soup.find_all("table"):
            txt = t.get_text(" ", strip=True).lower()
            if (
                ("tenure" in txt or "maturity" in txt or "period" in txt)
                and ("rate" in txt or "interest" in txt)
            ):
                table = t
                break
    if table is None:
        raise ValueError("No rates table found in Equitas snapshot/page HTML")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenure" in tl or "period" in tl or "maturity" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*to\s*<\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 30 - 1
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue
        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    if not out:
        raise ValueError(
            "Equitas parser found no rows. Live page is likely blocked and local snapshot is missing/invalid."
        )
    return out


def parse_bandhan_second_rctable_3cr_10cr(html: str) -> list[dict[str, Any]]:
    """
    Bandhan rates page: second div.rctable.
    Parse only the column '₹3 cr to ₹10 cr' with tenure from first column.
    """
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.find_all("div", class_=lambda c: bool(c) and "rctable" in _split_html_classes(c))
    if len(blocks) < 2:
        raise ValueError("Second div.rctable not found on Bandhan page")
    table = blocks[1].find("table")
    if not table:
        raise ValueError("No table inside second div.rctable")

    rows = table.find_all("tr")
    if not rows:
        return []

    col_idx: int | None = None
    for tr in rows[:3]:
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        for i, c in enumerate(cells):
            h = c.get_text(" ", strip=True).lower().replace(",", "")
            if "3 cr to 10 cr" in h or "₹3 cr to ₹10 cr" in h or "rs.3 cr to rs.10 cr" in h:
                col_idx = i
                break
        if col_idx is not None:
            break
    if col_idx is None:
        raise ValueError("Could not find '₹3 cr to ₹10 cr' column in second rctable")

    out: list[dict[str, Any]] = []
    for tr in rows:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= col_idx:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenure" in tl or "with premature" in tl or "without premature" in tl:
            continue

        rate = _rate_from_cell_text(cells[col_idx].get_text(" ", strip=True))
        if rate is None:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*<\s*(\d+)\s*month", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"(\d+)\s*month\s*<\s*(\d+)\s*month", tl)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"(\d+)\s*years?\s*to\s*less than\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 365, int(m.group(2)) * 365 - 1
        if mn is None:
            continue
        if mx is None:
            mx = mn

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": rate,
                "rate_senior": None,
            }
        )
    return out


def parse_idbi_interestrate_termdeposit_section(html: str) -> list[dict[str, Any]]:
    """
    IDBI page: table inside section/div with id 'InterestRate-TermDeposit'.
    Uses General Customers and Sr. Citizen columns.
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find(id="InterestRate-TermDeposit")
    if not root:
        raise ValueError("Section id='InterestRate-TermDeposit' not found")
    table = root.find("table")
    if not table:
        raise ValueError("No table inside #InterestRate-TermDeposit")

    def _parse_idbi_tenor_days(tenor_text: str) -> tuple[int | None, int | None]:
        # Remove parenthetical notes like "(except 555 days & 700 Days)".
        clean = re.sub(r"\([^)]*\)", "", tenor_text).replace("\xa0", " ").strip()
        tl = re.sub(r"\s+", " ", clean.lower())

        # 07-30 days / 46- 60 days
        m = re.search(r"\b(\d+)\s*-\s*(\d+)\s*days?\b", tl)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            return min(a, b), max(a, b)

        # 6 months 1 day to 270 days
        m = re.search(r"\b(\d+)\s*months?\s*(\d+)\s*days?\s*to\s*(\d+)\s*days?\b", tl)
        if m:
            a = int(m.group(1)) * 30 + int(m.group(2))
            b = int(m.group(3))
            return min(a, b), max(a, b)

        # 91 days to 6 months
        m = re.search(r"\b(\d+)\s*days?\s*to\s*(\d+)\s*months?\b", tl)
        if m:
            a, b = int(m.group(1)), int(m.group(2)) * 30
            return min(a, b), max(a, b)

        # 271 days to < 1 year
        m = re.search(r"\b(\d+)\s*days?\s*to\s*<?\s*(\d+)\s*year", tl)
        if m:
            a, b = int(m.group(1)), int(m.group(2)) * 365 - 1
            return min(a, b), max(a, b)

        # >1 Year to 2 Years
        m = re.search(r">\s*(\d+)\s*year\s*to\s*(\d+)\s*years?", tl)
        if m:
            a, b = int(m.group(1)) * 365 + 1, int(m.group(2)) * 365
            return min(a, b), max(a, b)

        # 3 years to <5 years
        m = re.search(r"\b(\d+)\s*years?\s*to\s*<?\s*(\d+)\s*years?", tl)
        if m:
            a = int(m.group(1)) * 365
            b = int(m.group(2)) * 365 - (1 if "<" in tl else 0)
            return min(a, b), max(a, b)

        # 5 years
        m = re.search(r"\b(\d+)\s*years?\b", tl)
        if m:
            d = int(m.group(1)) * 365
            return d, d

        # 370 days / 1111 days
        m = re.search(r"\b(\d+)\s*days?\b", tl)
        if m:
            d = int(m.group(1))
            return d, d

        return parse_tenor_to_days(clean)

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "maturity slab" in tl or "retail term deposits" in tl or "interest rate" in tl:
            continue
        if "tax saving" in tl or "vasundhara" in tl or "aarogya" in tl:
            continue

        mn, mx = _parse_idbi_tenor_days(tenor)
        if mn is None:
            continue
        if mx is None:
            mx = mn

        gen = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        sr = _rate_from_cell_text(cells[2].get_text(" ", strip=True)) if len(cells) > 2 else None
        if gen is None and sr is None:
            continue

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_indianbank_first_table(html: str) -> list[dict[str, Any]]:
    """
    Indian Bank deposit-rates page: parse first table
    (Interest Rates on Domestic Retail Term Deposits).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on Indian Bank deposit-rates page")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "period" in tl or "tenor" in tl or "interest rates" in tl or "existing rate" in tl:
            continue
        if "ind secure" in tl or "ind green" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*to\s*less than\s*(\d+)\s*year", tl)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 365 - 1
        if mn is None:
            m = re.search(r"above\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 365 + 1, int(m.group(1)) * 365 + 3650
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue
        # First numeric value in row is the revised public rate.
        gen = nums[-1] if len(nums) >= 2 else nums[0]

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": None,
            }
        )
    return out


def parse_rbl_collapse584_table(html: str) -> list[dict[str, Any]]:
    """
    RBL interest-rates page: table inside div id='collapse584'
    (Fixed Deposits – Less than INR 3 crores – Premature Withdrawal Allowed).
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find(id="collapse584")
    if not root:
        raise ValueError("div id='collapse584' not found")
    table = root.find("table")
    if not table:
        raise ValueError("No table inside #collapse584")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "period of deposit" in tl or "general citizen" in tl or "deposits below inr" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 30
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 30 - 1
        if mn is None:
            m = re.search(r"(\d+)\s*months?\s*to\s*(\d+)\s*months?", tl)
            if m:
                mn, mx = int(m.group(1)) * 30, int(m.group(2)) * 30
        if mn is None:
            continue
        if mx is None:
            mx = mn

        gen = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        sr = _rate_from_cell_text(cells[3].get_text(" ", strip=True)) if len(cells) > 3 else None
        if gen is None and sr is None:
            continue

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_csb_domestic_deposits(html: str) -> list[dict[str, Any]]:
    """
    CSB interest-rates page: parse table right after <h3 id='domestic_deposites'>.
    Stores general + senior citizen columns where available.
    """
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True).lower()
    if "captcha" in page_text and "radware" in page_text:
        raise ValueError("CSB page returned captcha/challenge page; cannot parse rate table")

    # Prefer exact selector requested, but tolerate minor id variant seen on some pages.
    anchor = soup.find("h3", id="domestic_deposites")
    if not anchor:
        anchor = soup.find("h3", id="domestic_deposits")
    if not anchor:
        # Last-resort fallback: heading text marker.
        for h in soup.find_all("h3"):
            htxt = h.get_text(" ", strip=True).lower()
            if "domestic" in htxt and "deposit" in htxt:
                anchor = h
                break
    if not anchor:
        raise ValueError("Heading h3#domestic_deposites not found (or page structure changed)")

    # There are multiple tables under this heading (savings, term, tax saver, senior).
    # Select the domestic term-deposit table explicitly by heading text.
    general_table = None
    senior_table = None
    for t in anchor.find_all_next("table", limit=12):
        txt = t.get_text(" ", strip=True).lower()
        if general_table is None and "domestic term deposits" in txt and "below rs. 3 crore" in txt:
            general_table = t
        if senior_table is None and "senior citizen term deposits" in txt and "below rs. 3 crore" in txt:
            senior_table = t
        if general_table is not None and senior_table is not None:
            break
    if general_table is None:
        raise ValueError("No CSB domestic term-deposit table found under domestic_deposites")

    def _parse_days_for_csb_tenor(tenor_text: str) -> tuple[int | None, int | None]:
        mn, mx = parse_tenor_to_days(tenor_text)
        if mn is not None:
            return mn, mx
        tl = tenor_text.lower()
        m = re.search(r"above\s*(\d+)\s*months?\s*to\s*(?:less than\s*)?(\d+)\s*months?", tl)
        if m:
            a, b = int(m.group(1)) * 30 + 1, int(m.group(2)) * 30
            return a, b
        m = re.search(r"above\s*(\d+)\s*months?\s*to\s*(\d+)\s*years?", tl)
        if m:
            a, b = int(m.group(1)) * 30 + 1, int(m.group(2)) * 365
            return a, b
        m = re.search(r"above\s*(\d+)\s*years?\s*to\s*(\d+)\s*years?", tl)
        if m:
            a, b = int(m.group(1)) * 365 + 1, int(m.group(2)) * 365
            return a, b
        return None, None

    def _extract_rows(table: Any) -> dict[str, tuple[int, int, float]]:
        rows: dict[str, tuple[int, int, float]] = {}
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texts = [c.get_text(" ", strip=True).replace("\xa0", " ").strip() for c in cells]
            if not any(texts):
                continue
            joined = " ".join(texts).lower()
            if "slab" in joined and "deposit tenor" in joined:
                continue
            if "interest rates" in joined and "term deposits" in joined:
                continue

            # CSB table shape is typically [slab, tenor, rate].
            tenor = texts[1] if len(texts) >= 3 else texts[0]
            if not tenor:
                continue
            mn, mx = _parse_days_for_csb_tenor(tenor)
            if mn is None:
                continue
            if mx is None:
                mx = mn

            nums: list[float] = []
            for c in cells[1:]:
                v = _rate_from_cell_text(c.get_text(" ", strip=True))
                if v is not None:
                    nums.append(v)
            if not nums:
                continue
            rows[tenor.lower()] = (mn, mx, nums[-1])
        return rows

    general_rows = _extract_rows(general_table)
    senior_rows = _extract_rows(senior_table) if senior_table is not None else {}

    out: list[dict[str, Any]] = []
    for tenor_key, (mn, mx, gen) in general_rows.items():
        sr_item = senior_rows.get(tenor_key)
        sr = sr_item[2] if sr_item is not None else None
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor_key,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_cityunion_second_customers_1(html: str) -> list[dict[str, Any]]:
    """
    City Union Bank deposit-interest-rate page: parse the second table with
    id='customers-1' (Domestic/NRO Callable Term Deposit block).
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", id="customers-1")
    if len(tables) < 2:
        raise ValueError("Second table with id='customers-1' not found")
    table = tables[1]

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if (
            "period" in tl
            or "rate of interest" in tl
            or "general" == tl
            or "senior citizen" in tl
            or "domestic/nro callable term deposit" in tl
            or tl.startswith("from ")
            or "nro deposits" in tl
            or "tax saver" in tl
        ):
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 365
        if mn is None:
            m = re.search(r"over\s*(\d+)\s*years?\s*up ?to\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 365 + 1, int(m.group(2)) * 365
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue

        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def parse_dcb_depositrates_block(html: str) -> list[dict[str, Any]]:
    """
    DCB fixed-deposit page: parse first table inside
    div class='DepositRates_datatable-block__qGF_p'.
    """
    soup = BeautifulSoup(html, "html.parser")
    token = "DepositRates_datatable-block__qGF_p"
    root = soup.find("div", class_=lambda c: bool(c) and token in _split_html_classes(c))
    if not root:
        raise ValueError("Element div.DepositRates_datatable-block__qGF_p not found")
    table = root.find("table")
    if not table:
        raise ValueError("No <table> inside div.DepositRates_datatable-block__qGF_p")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenor" in tl or "period" in tl or "maturity" in tl or "interest rate" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"over\s*(\d+)\s*years?\s*up to\s*(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 365 + 1, int(m.group(2)) * 365
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue

        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    return out


def fetch_parse_dcb_via_api(base_url: str) -> list[dict[str, Any]]:
    """
    DCB rates page is client-rendered; fetch structured data from the site's
    internal API interceptor and parse the resident fixed-deposit table.
    """
    m = re.match(r"^(https?://[^/]+)", (base_url or "").strip())
    if not m:
        raise ValueError("Invalid DCB base URL")
    origin = m.group(1)

    sess = _session()
    payload = {"url": "/rates?id=resident-fixed-deposit-interest-rates", "method": "GET"}
    r = sess.post(f"{origin}/api/api-interceptor", json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    obj = r.json()

    blocks = (((obj.get("data") or {}).get("data")) or [])
    if not blocks:
        raise ValueError("DCB API returned empty data for resident-fixed-deposit-interest-rates")

    details_html = ""
    for item in blocks[0].get("rate_details") or []:
        title = str(item.get("title") or "").lower()
        if "resident indian fixed deposit interest rates" in title:
            details_html = str(item.get("details") or "")
            break
    if not details_html:
        details_html = str((blocks[0].get("rate_details") or [{}])[0].get("details") or "")
    if not details_html.strip():
        raise ValueError("DCB API response missing rate_details HTML")

    soup = BeautifulSoup(details_html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found inside DCB API rate_details HTML")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if "tenure" in tl or "deposit interest rate" in tl or "general" in tl or "senior" in tl:
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m1 = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m1:
                mn, mx = int(m1.group(1)), int(m1.group(2))
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue

        gen = nums[0]
        # DCB resident table is usually: general_rate, general_yield, senior_rate, ...
        sr = nums[2] if len(nums) >= 3 else (nums[1] if len(nums) >= 2 else None)
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    if not out:
        raise ValueError("DCB API table parsed but yielded zero rows")
    return out


def parse_dhan_parent_accordion_domestic_nro(html: str) -> list[dict[str, Any]]:
    """
    Dhanlaxmi interest-rates page: first table inside
    div.parent_accordion.term-deposits-domestic-nro-deposits.
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find(
        "div",
        class_=lambda c: bool(c)
        and "parent_accordion" in _split_html_classes(c)
        and "term-deposits-domestic-nro-deposits" in _split_html_classes(c),
    )
    if not root:
        raise ValueError("No div with classes parent_accordion and term-deposits-domestic-nro-deposits")
    table = root.find("table")
    if not table:
        raise ValueError("No <table> inside target Dhanlaxmi parent_accordion block")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if (
            "term deposits" in tl
            or "all maturities" in tl
            or "rates of interest" in tl
            or "w.e.f" in tl
            or "senior citizens are eligible" in tl
        ):
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*one\s*year", tl)
            if m:
                mn, mx = int(m.group(1)), 364
        if mn is None:
            m = re.search(r"above\s*(\d+)\s*years?\s*upto.*?(\d+)\s*years?", tl)
            if m:
                mn, mx = int(m.group(1)) * 365 + 1, int(m.group(2)) * 365
        if mn is None:
            continue
        if mx is None:
            mx = mn

        rate = _rate_from_cell_text(cells[1].get_text(" ", strip=True))
        if rate is None:
            continue

        # Site states +0.50% for senior citizens on 1 year and above.
        sr = round(rate + 0.5, 4) if mn >= 365 else rate
        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": rate,
                "rate_senior": sr,
            }
        )
    return out


def parse_yes_first_table(html: str) -> list[dict[str, Any]]:
    """
    YES Bank fixed-deposit page: parse first table.
    """
    if "replace this template with actual page html from" in html.lower():
        raise ValueError(
            "YES source fallback snapshot is still a template. "
            "Replace snapshots/yes_fd_page.html with real YES page HTML."
        )

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on YES Bank fixed-deposit page")

    out: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        tenor = cells[0].get_text(" ", strip=True).replace("\xa0", " ").strip()
        if not tenor:
            continue
        tl = tenor.lower()
        if (
            "tenor" in tl
            or "period" in tl
            or "maturity" in tl
            or "interest rate" in tl
            or "general" in tl and "senior" in tl
        ):
            continue

        mn, mx = parse_tenor_to_days(tenor)
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*(\d+)\s*days?", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2))
        if mn is None:
            m = re.search(r"(\d+)\s*days?\s*to\s*less than\s*(\d+)\s*year", tl)
            if m:
                mn, mx = int(m.group(1)), int(m.group(2)) * 365 - 1
        if mn is None:
            continue
        if mx is None:
            mx = mn

        nums: list[float] = []
        for c in cells[1:]:
            v = _rate_from_cell_text(c.get_text(" ", strip=True))
            if v is not None:
                nums.append(v)
        if not nums:
            continue
        gen = nums[0]
        sr = nums[1] if len(nums) > 1 else None

        out.append(
            {
                "min_days": mn,
                "max_days": mx,
                "tenor_label": tenor,
                "rate_general": gen,
                "rate_senior": sr,
            }
        )
    if not out:
        raise ValueError("YES parser found no rows from available table content")
    return out


PARSERS = {
    "kotak_json_array": parse_kotak_json_array,
    "json_four_column": parse_kotak_json_array,
    "html_sbi_style": parse_html_sbi_style,
    "icici_fd_interest_json": parse_icici_fd_interest_json,
    "canara_msonormal_following_table": parse_canara_msonormal_following_table,
    "au_first_table": parse_au_first_table,
    "federal_first_table": parse_federal_first_table,
    "indusind_first_table": parse_indusind_first_table,
    "ujjivan_first_table": parse_ujjivan_first_table,
    "equitas_div_w_full_md_w_3_5": parse_equitas_div_w_full_md_w_3_5,
    "bandhan_second_rctable_3cr_10cr": parse_bandhan_second_rctable_3cr_10cr,
    "idbi_interestrate_termdeposit_section": parse_idbi_interestrate_termdeposit_section,
    "indianbank_first_table": parse_indianbank_first_table,
    "rbl_collapse584_table": parse_rbl_collapse584_table,
    "csb_domestic_deposits": parse_csb_domestic_deposits,
    "cityunion_second_customers_1": parse_cityunion_second_customers_1,
    "dcb_depositrates_block": parse_dcb_depositrates_block,
    "dhan_parent_accordion_domestic_nro": parse_dhan_parent_accordion_domestic_nro,
    "yes_first_table": parse_yes_first_table,
}


def fetch_and_parse(
    parser_name: str,
    url: str,
    *,
    container_id: str | None = None,
    container_class: str | None = None,
    source: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    name = (parser_name or "").strip()
    src = source or {}
    text: str | None = None
    fetch_error: Exception | None = None

    # If a local snapshot is configured, prefer it first for deterministic parsing.
    local_html_path = str(src.get("local_html_path") or "").strip()
    if local_html_path:
        p = Path(local_html_path)
        if not p.is_absolute():
            p = (Path(__file__).resolve().parent / p).resolve()
        if p.is_file():
            text = p.read_text(encoding="utf-8", errors="ignore")

    if url and text is None:
        try:
            sess = _session()
            r = sess.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            text = r.text
        except Exception as e:
            fetch_error = e

    if text is not None:
        local_html_path = str(src.get("local_html_path") or "").strip()
        if local_html_path and _looks_like_challenge_page(text):
            p = Path(local_html_path)
            if not p.is_absolute():
                p = (Path(__file__).resolve().parent / p).resolve()
            if p.is_file():
                text = p.read_text(encoding="utf-8", errors="ignore")
            else:
                raise ValueError(
                    f"Live page is blocked/challenge page and local_html_path not found: {p}"
                )

    if text is None:
        local_html_path = str(src.get("local_html_path") or "").strip()
        if local_html_path:
            p = Path(local_html_path)
            if not p.is_absolute():
                p = (Path(__file__).resolve().parent / p).resolve()
            if p.is_file():
                text = p.read_text(encoding="utf-8", errors="ignore")
            elif fetch_error is not None:
                raise ValueError(
                    f"Live fetch failed: {fetch_error}; local_html_path not found: {p}"
                ) from fetch_error
            else:
                raise ValueError(f"local_html_path not found: {p}")

    if text is None and fetch_error is not None:
        raise fetch_error
    if text is None:
        raise ValueError("No HTML source available (both live fetch and local fallback unavailable)")

    if name == "html_div_two_rate_columns":
        return parse_html_div_two_rate_columns(text, container_id or "")

    if name == "html_div_class_fd_table":
        return parse_html_div_class_fd_table(text, container_class or "")

    if name == "equitas_div_w_full_md_w_3_5":
        try:
            return parse_equitas_div_w_full_md_w_3_5(text)
        except Exception as html_err:
            pdf_url = _extract_equitas_overall_interest_pdf_url(text)
            if not pdf_url:
                raise html_err
            sess = _session()
            r = sess.get(pdf_url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            rows = parse_equitas_overall_interest_pdf(r.content)
            if not rows:
                raise ValueError("Equitas PDF parsed but produced no rows") from html_err
            return rows

    if name == "dcb_depositrates_block":
        try:
            return fetch_parse_dcb_via_api(url)
        except Exception:
            return parse_dcb_depositrates_block(text)

    if name == "idfc_fd_formtable":
        if "senior_extra_pct" in src:
            v = src.get("senior_extra_pct")
            bump = None if v is None else float(v)
        else:
            bump = 0.5
        return parse_idfc_fd_formtable(text, senior_extra_pct=bump)

    if name == "union_bank_inner_table":
        if "senior_extra_pct" in src:
            v = src.get("senior_extra_pct")
            bump = None if v is None else float(v)
        else:
            bump = 0.5
        return parse_union_bank_inner_table(text, senior_extra_pct=bump)

    if name == "pnb_anchor_below_table":
        tab = str(src.get("anchor_data_tab") or "")
        return parse_pnb_anchor_below_table(text, anchor_data_tab=tab)

    if name not in PARSERS:
        raise ValueError(
            f"Unknown parser: {parser_name}. Known: {list(PARSERS) + ['html_div_two_rate_columns', 'html_div_class_fd_table', 'idfc_fd_formtable', 'union_bank_inner_table', 'pnb_anchor_below_table']}"
        )
    return PARSERS[name](text)


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS banks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fd_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id INTEGER NOT NULL,
            min_days INTEGER,
            max_days INTEGER,
            tenor_label TEXT,
            rate_general REAL,
            rate_senior REAL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (bank_id) REFERENCES banks(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_fd_rates_bank ON fd_rates(bank_id);
        CREATE INDEX IF NOT EXISTS idx_fd_rates_tenor ON fd_rates(min_days, max_days);
        """
    )
    conn.commit()


def upsert_bank(conn: sqlite3.Connection, source_id: str, display_name: str) -> int:
    cur = conn.execute("SELECT id FROM banks WHERE source_id = ?", (source_id,))
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE banks SET display_name = ? WHERE id = ?",
            (display_name, row[0]),
        )
        conn.commit()
        return int(row[0])
    conn.execute(
        "INSERT INTO banks (source_id, display_name) VALUES (?, ?)",
        (source_id, display_name),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def replace_rates(
    conn: sqlite3.Connection,
    bank_id: int,
    rows: list[dict[str, Any]],
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("DELETE FROM fd_rates WHERE bank_id = ?", (bank_id,))
    for row in rows:
        conn.execute(
            """
            INSERT INTO fd_rates (
                bank_id, min_days, max_days, tenor_label,
                rate_general, rate_senior, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bank_id,
                row.get("min_days"),
                row.get("max_days"),
                row.get("tenor_label"),
                row.get("rate_general"),
                row.get("rate_senior"),
                now,
            ),
        )
    conn.commit()


def refresh_source(conn: sqlite3.Connection, source: dict[str, Any]) -> dict[str, Any]:
    """Fetch one source and write to DB. Returns { ok, source_id, error?, count? }."""
    sid = source.get("id") or ""
    url = source.get("url") or ""
    parser = source.get("parser") or "kotak_json_array"
    bank_name = source.get("bank_name") or sid
    try:
        rows = fetch_and_parse(
            parser,
            url,
            container_id=source.get("container_id"),
            container_class=source.get("container_class"),
            source=source,
        )
        bank_id = upsert_bank(conn, sid, bank_name)
        replace_rates(conn, bank_id, rows)
        return {"ok": True, "source_id": sid, "count": len(rows)}
    except Exception as e:
        return {"ok": False, "source_id": sid, "error": str(e)}
