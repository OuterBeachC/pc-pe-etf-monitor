"""
Data retrieval for ETF holdings files.

Three retrieval strategies matching the automation configs in app.py:
  1. csv    — Direct HTTP download via requests (replaces curl)
  2. browser — Headless Playwright scrape for JS-rendered pages
  3. csv+edgar — HTTP download + SEC EDGAR N-PORT filing fetch

Downloaded files are saved to data/holdings/YYYYMMDD/<TICKER>_holdings.<ext>
"""

import csv
import io
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests

from backend.config import (
    DATA_DIR,
    EDGAR_SEARCH_URL,
    EDGAR_USER_AGENT,
    ETF_SOURCES,
    HEADERS,
    REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _today_dir() -> Path:
    """Return today's holdings directory, creating it if needed."""
    d = DATA_DIR / datetime.now().strftime("%Y%m%d")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _out_path(ticker: str, ext: str, output_dir: Path | None = None) -> Path:
    """Build output file path for a given ticker."""
    d = output_dir or _today_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{ticker}_holdings{ext}"


def _retry_get(url: str, headers: dict | None = None, timeout: int = REQUEST_TIMEOUT,
               max_retries: int = 3) -> requests.Response:
    """GET with exponential backoff on transient failures."""
    headers = headers or HEADERS
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning("Attempt %d failed for %s: %s — retrying in %ds",
                               attempt + 1, url, exc, wait)
                time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts: {last_exc}")


# ─── CSV / Direct Download ────────────────────────────────────────────────────

def download_csv(ticker: str, output_dir: Path | None = None) -> Path:
    """Download holdings file via direct HTTP (replaces curl commands)."""
    cfg = ETF_SOURCES[ticker]
    download_url = cfg["download_url"]
    ext = cfg["file_ext"]

    logger.info("Downloading %s from %s", ticker, download_url)
    resp = _retry_get(download_url)

    out = _out_path(ticker, ext, output_dir)
    out.write_bytes(resp.content)
    logger.info("Saved %s (%d bytes) → %s", ticker, len(resp.content), out)
    return out


# ─── Browser-Based Scraping ───────────────────────────────────────────────────

def download_browser(ticker: str, output_dir: Path | None = None) -> Path:
    """Scrape holdings from JS-rendered pages using Playwright.

    Requires: pip install playwright && playwright install chromium
    Falls back to requests+BeautifulSoup if Playwright is unavailable.
    """
    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    selector = cfg.get("selector", "table")
    out = _out_path(ticker, ".csv", output_dir)

    # Try Playwright first
    try:
        return _scrape_with_playwright(ticker, url, selector, out)
    except ImportError:
        logger.warning("Playwright not installed — falling back to requests+BS4 for %s", ticker)
        return _scrape_with_bs4(ticker, url, out)


def _scrape_with_playwright(ticker: str, url: str, selector: str, out: Path) -> Path:
    """Scrape holdings table using Playwright headless browser."""
    from playwright.sync_api import sync_playwright

    logger.info("Scraping %s via Playwright from %s", ticker, url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector(selector, timeout=30_000)

        # Extract table data from the page
        rows = page.evaluate("""(selector) => {
            const table = document.querySelector(selector + ' table') ||
                          document.querySelector(selector).closest('table') ||
                          document.querySelector('table');
            if (!table) return [];
            return [...table.rows].map(row =>
                [...row.cells].map(cell => cell.textContent.trim())
            );
        }""", selector)

        browser.close()

    if not rows:
        raise RuntimeError(f"No table data found for {ticker} at {url}")

    # Write as CSV
    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)

    logger.info("Scraped %s (%d rows) → %s", ticker, len(rows), out)
    return out


def _scrape_with_bs4(ticker: str, url: str, out: Path) -> Path:
    """Fallback: scrape holdings table with requests + BeautifulSoup."""
    from bs4 import BeautifulSoup

    logger.info("Scraping %s via BS4 from %s", ticker, url)
    resp = _retry_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    table = (
        soup.find("table", class_="holdings-table")
        or soup.find("table", {"data-testid": "holdings-table"})
        or soup.find("table")
    )
    if not table:
        raise RuntimeError(f"No holdings table found for {ticker} at {url}")

    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)

    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)

    logger.info("Scraped %s (%d rows) → %s", ticker, len(rows), out)
    return out


# ─── SEC EDGAR ────────────────────────────────────────────────────────────────

def download_edgar_filing(ticker: str, output_dir: Path | None = None) -> Path | None:
    """Fetch the latest N-PORT filing from SEC EDGAR for an ETF.

    Returns path to saved JSON filing, or None if no filing found.
    """
    cfg = ETF_SOURCES[ticker]
    query = cfg.get("edgar_query", ticker)
    form = cfg.get("edgar_form", "NPORT-P")

    logger.info("Searching EDGAR for %s (form=%s, query=%s)", ticker, form, query)

    search_url = f"{EDGAR_SEARCH_URL}?q={query}&forms={form}"
    headers = {"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"}

    try:
        resp = _retry_get(search_url, headers=headers)
        data = resp.json()
    except Exception as exc:
        logger.error("EDGAR search failed for %s: %s", ticker, exc)
        return None

    # SEC rate-limits to 10 req/s — be polite
    time.sleep(0.2)

    out = _out_path(ticker, "_edgar.json", output_dir)
    out.write_text(json.dumps(data, indent=2))
    logger.info("Saved EDGAR data for %s → %s", ticker, out)
    return out


# ─── Composite Downloads ──────────────────────────────────────────────────────

def download_etf(ticker: str, output_dir: Path | None = None) -> dict:
    """Download holdings for a single ETF using the appropriate method.

    Returns a dict with paths to downloaded files:
        {"holdings": Path, "edgar": Path | None}
    """
    if ticker not in ETF_SOURCES:
        raise ValueError(f"Unknown ETF ticker: {ticker}")

    cfg = ETF_SOURCES[ticker]
    method = cfg["method"]
    result = {"ticker": ticker, "holdings": None, "edgar": None, "error": None}

    try:
        if method == "csv":
            result["holdings"] = download_csv(ticker, output_dir)
        elif method == "browser":
            result["holdings"] = download_browser(ticker, output_dir)
        elif method == "csv+edgar":
            result["holdings"] = download_csv(ticker, output_dir)
        else:
            raise ValueError(f"Unknown method '{method}' for {ticker}")
    except Exception as exc:
        logger.error("Failed to download %s holdings: %s", ticker, exc)
        result["error"] = str(exc)

    # Fetch EDGAR data if configured
    if cfg.get("edgar_query"):
        try:
            result["edgar"] = download_edgar_filing(ticker, output_dir)
        except Exception as exc:
            logger.warning("EDGAR fetch failed for %s: %s", ticker, exc)

    return result


def download_all(output_dir: Path | None = None, tickers: list[str] | None = None) -> list[dict]:
    """Download holdings for all (or specified) ETFs.

    Returns list of result dicts from download_etf().
    """
    tickers = tickers or list(ETF_SOURCES.keys())
    output_dir = output_dir or _today_dir()

    logger.info("Starting bulk download for %d ETFs → %s", len(tickers), output_dir)
    results = []

    for ticker in tickers:
        result = download_etf(ticker, output_dir)
        results.append(result)
        # Polite delay between requests
        time.sleep(0.5)

    succeeded = sum(1 for r in results if r["holdings"] is not None)
    failed = sum(1 for r in results if r["error"] is not None)
    logger.info("Bulk download complete: %d succeeded, %d failed", succeeded, failed)

    return results
