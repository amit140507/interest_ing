"""Capture CSB interest-rates HTML in a real browser session.

This helper opens a visible Chromium window (Playwright headful mode), so you
can solve any anti-bot challenge manually. After you confirm the CSB rates
table is visible, it saves the rendered page HTML to the snapshot file used by
scrapers.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


DEFAULT_URL = "https://www.csb.bank.in/interest-rates"
DEFAULT_OUT = Path("snapshots/csb_interest_rates.html")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Update CSB local snapshot using headful browser automation."
    )
    p.add_argument("--url", default=DEFAULT_URL, help="Target CSB page URL")
    p.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help="Output HTML path (default: snapshots/csb_interest_rates.html)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception:
        print("Playwright is required but not installed.")
        print("Install with:")
        print("  pip install playwright")
        print("  python -m playwright install chromium")
        return 1

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1400, "height": 960})
        page = context.new_page()

        print(f"Opening: {args.url}")
        try:
            page.goto(args.url, wait_until="domcontentloaded", timeout=120_000)
        except PlaywrightTimeoutError:
            print("Initial load timeout; browser stays open so you can continue manually.")

        print("")
        print("In the opened browser:")
        print("1) Complete any challenge if prompted")
        print("2) Ensure the CSB domestic deposit table is visible")
        input("Press Enter here to save rendered HTML snapshot... ")

        html = page.content()
        out_path.write_text(html, encoding="utf-8")
        print(f"Saved snapshot to: {out_path}")

        context.close()
        browser.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
