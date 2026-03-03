"""
Data retrieval for ETF holdings files.

Four retrieval strategies matching the automation configs in app.py:
  1. csv        -- Direct HTTP download via requests (replaces curl)
  2. browser    -- Headless Playwright scrape for JS-rendered pages
  3. csv+edgar  -- HTTP download + SEC EDGAR N-PORT filing fetch
  4. invesco    -- Selenium-based export from Invesco holdings page

Downloaded files are saved to data/holdings/YYYYMMDD/<TICKER>_holdings.<ext>
"""

import csv
import glob
import io
import json
import logging
import os
import shutil
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
                logger.warning("Attempt %d failed for %s: %s -- retrying in %ds",
                               attempt + 1, url, exc, wait)
                time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts: {last_exc}")


# ─── Last-Modified Tracking ──────────────────────────────────────────────────

def _meta_path(ticker: str) -> Path:
    """Path to the Last-Modified metadata file for a ticker."""
    meta_dir = DATA_DIR / ".meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    return meta_dir / f"{ticker.lower()}_last_modified.txt"


def _check_last_modified(ticker: str, url: str) -> bool:
    """Check if remote file has been updated since last download.

    Returns True if the file is new or updated, False if unchanged.
    """
    meta = _meta_path(ticker)
    try:
        resp = requests.head(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            logger.warning("HEAD request failed for %s: %d", ticker, resp.status_code)
            return True  # Download anyway if we can't check
    except requests.RequestException as exc:
        logger.warning("HEAD request error for %s: %s", ticker, exc)
        return True

    remote_modified = resp.headers.get("Last-Modified")
    if not remote_modified:
        logger.debug("No Last-Modified header for %s, downloading anyway", ticker)
        return True

    local_modified = meta.read_text().strip() if meta.exists() else None
    if local_modified == remote_modified:
        logger.info("%s unchanged (Last-Modified: %s)", ticker, remote_modified)
        return False

    logger.info("%s updated: remote=%s, local=%s", ticker, remote_modified, local_modified)
    return True


def _save_last_modified(ticker: str, url: str) -> None:
    """Save the Last-Modified header from a URL for future comparison."""
    try:
        resp = requests.head(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            last_modified = resp.headers.get("Last-Modified")
            if last_modified:
                _meta_path(ticker).write_text(last_modified)
    except requests.RequestException:
        pass  # Non-critical -- skip silently


# ─── CSV / Direct Download ────────────────────────────────────────────────────

def download_csv(ticker: str, output_dir: Path | None = None,
                 check_modified: bool = False) -> Path:
    """Download holdings file via direct HTTP (replaces curl commands).

    If check_modified is True, uses Last-Modified header to skip unchanged files.
    """
    cfg = ETF_SOURCES[ticker]
    download_url = cfg["download_url"]
    ext = cfg["file_ext"]

    # Check if file has changed (for SSGA daily files)
    if check_modified and not _check_last_modified(ticker, download_url):
        out = _out_path(ticker, ext, output_dir)
        if out.exists():
            logger.info("Using cached %s (unchanged on server)", ticker)
            return out

    logger.info("Downloading %s from %s", ticker, download_url)
    resp = _retry_get(download_url)

    # Validate content -- reject HTML error pages saved as CSV/XLSX
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" in content_type and ext in (".csv", ".xlsx", ".xls"):
        raise RuntimeError(
            f"Expected data file for {ticker} but got HTML response "
            f"(Content-Type: {content_type}). URL may be incorrect: {download_url}"
        )

    out = _out_path(ticker, ext, output_dir)
    out.write_bytes(resp.content)
    logger.info("Saved %s (%d bytes) -> %s", ticker, len(resp.content), out)

    # Save Last-Modified for future checks
    _save_last_modified(ticker, download_url)

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
        logger.warning("Playwright not installed -- falling back to requests+BS4 for %s", ticker)
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

    logger.info("Scraped %s (%d rows) -> %s", ticker, len(rows), out)
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

    logger.info("Scraped %s (%d rows) -> %s", ticker, len(rows), out)
    return out


# ─── Invesco Selenium Download ────────────────────────────────────────────────

def download_invesco(ticker: str, output_dir: Path | None = None,
                     headless: bool = True) -> Path:
    """Download Invesco ETF holdings via Selenium browser automation.

    Navigates to the Invesco holdings page, handles the role-selection popup,
    and clicks the Export Data button to trigger a CSV download.

    Requires: pip install selenium webdriver-manager
    """
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    download_pattern = cfg.get("download_pattern", f"*{ticker.lower()}*holdings*.csv")
    out = _out_path(ticker, ".csv", output_dir)

    # Use a temp directory for Selenium downloads, then move to output_dir
    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix=f"invesco_{ticker}_")
    tmp_dir_abs = os.path.abspath(tmp_dir)

    # Delete any pre-existing files matching the pattern in tmp
    for old in glob.glob(os.path.join(tmp_dir_abs, "*.csv")):
        os.remove(old)

    # Track existing CSVs before download
    existing_files = set(glob.glob(os.path.join(tmp_dir_abs, "*.csv")))

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": tmp_dir_abs,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })

    logger.info("Downloading %s via Selenium from %s", ticker, url)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)
        time.sleep(4)

        # Handle "Select your role" popup
        _invesco_click_role(driver)
        time.sleep(4)

        # Handle cookie consent
        _invesco_accept_cookies(driver)
        time.sleep(3)

        # Scroll to make Export button visible
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(1)

        # Click the Export Data button
        if not _invesco_click_export(driver):
            raise RuntimeError(f"Could not find Export button for {ticker} on Invesco page")

        # Wait for download to complete
        downloaded = _invesco_wait_for_download(tmp_dir_abs, existing_files, timeout=15)
        if not downloaded:
            raise RuntimeError(f"No CSV file downloaded for {ticker} after timeout")

        # Move the downloaded file to our output path
        shutil.copy2(downloaded, str(out))
        logger.info("Saved %s -> %s", ticker, out)
        return out

    finally:
        driver.quit()
        # Clean up temp directory
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _invesco_click_role(driver) -> bool:
    """Click the 'Individual Investor' button on Invesco's role popup."""
    from selenium.webdriver.common.by import By

    # Method 1: Find button by text
    buttons = driver.find_elements(By.TAG_NAME, "button")
    for btn in buttons:
        try:
            if "Individual Investor" in btn.text:
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Clicked Individual Investor button")
                return True
        except Exception:
            continue

    # Method 2: Try data-attribute selector
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "[data-audiencetype='Investor']")
        driver.execute_script("arguments[0].click();", btn)
        logger.debug("Clicked via data-audiencetype selector")
        return True
    except Exception:
        pass

    logger.warning("Could not find Invesco role selector, continuing anyway")
    return False


def _invesco_accept_cookies(driver) -> bool:
    """Accept cookie consent if the banner appears."""
    from selenium.webdriver.common.by import By

    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            if "Accept" in btn.text:
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Accepted cookies")
                return True
    except Exception:
        pass
    return False


def _invesco_click_export(driver) -> bool:
    """Find and click the Export Data button on the Invesco holdings page."""
    from selenium.webdriver.common.by import By

    # Try buttons
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        try:
            text = btn.text.lower()
            if "export" in text or "download" in text:
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Clicked export button: '%s'", btn.text.strip())
                return True
        except Exception:
            continue

    # Try links
    for link in driver.find_elements(By.TAG_NAME, "a"):
        try:
            text = link.text.lower()
            href = (link.get_attribute("href") or "").lower()
            if "export" in text or "download" in text or "export" in href:
                driver.execute_script("arguments[0].click();", link)
                logger.debug("Clicked export link: '%s'", link.text.strip())
                return True
        except Exception:
            continue

    return False


def _invesco_wait_for_download(download_dir: str, existing_files: set,
                               timeout: int = 15) -> str | None:
    """Wait for a new CSV to appear in the download directory."""
    for i in range(timeout):
        time.sleep(1)
        current_files = set(glob.glob(os.path.join(download_dir, "*.csv")))
        new_files = current_files - existing_files
        downloading = glob.glob(os.path.join(download_dir, "*.crdownload"))

        if new_files and not downloading:
            downloaded = list(new_files)[0]
            logger.debug("Download complete: %s", downloaded)
            return downloaded

        logger.debug("Waiting for download... (%d/%ds)", i + 1, timeout)

    return None


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
    logger.info("Saved EDGAR data for %s -> %s", ticker, out)
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
            result["holdings"] = download_csv(ticker, output_dir, check_modified=True)
        elif method == "invesco":
            result["holdings"] = download_invesco(ticker, output_dir)
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

    logger.info("Starting bulk download for %d ETFs -> %s", len(tickers), output_dir)
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
