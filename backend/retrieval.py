"""
Data retrieval for ETF holdings files.

Retrieval strategies:
  1. csv           -- Direct HTTP download via requests
  2. csv_dated     -- Direct HTTP download with date-templated URL
  3. csv+edgar     -- HTTP download + SEC EDGAR N-PORT filing fetch
  4. browser       -- Headless Playwright / BS4 table scrape
  5. selenium      -- Selenium click-to-download (generic provider)
  6. selenium_scrape -- Selenium navigate + extract table from page/modal
  7. invesco       -- Invesco-specific Selenium flow (role popup + export)

Downloaded files are saved to data/holdings/YYYYMMDD/<TICKER>_holdings.<ext>
"""

import csv
import glob
import io
import json
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta
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
            return True
    except requests.RequestException as exc:
        logger.warning("HEAD request error for %s: %s", ticker, exc)
        return True

    remote_modified = resp.headers.get("Last-Modified")
    if not remote_modified:
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
        pass


# ─── CSV / Direct Download ────────────────────────────────────────────────────

def download_csv(ticker: str, output_dir: Path | None = None,
                 check_modified: bool = False) -> Path:
    """Download holdings file via direct HTTP.

    If check_modified is True, uses Last-Modified header to skip unchanged files.
    """
    cfg = ETF_SOURCES[ticker]
    download_url = cfg["download_url"]
    ext = cfg["file_ext"]

    if check_modified and not _check_last_modified(ticker, download_url):
        out = _out_path(ticker, ext, output_dir)
        if out.exists():
            logger.info("Using cached %s (unchanged on server)", ticker)
            return out

    logger.info("Downloading %s from %s", ticker, download_url)
    resp = _retry_get(download_url)

    # Reject HTML error pages saved as data files
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" in content_type and ext in (".csv", ".xlsx", ".xls"):
        raise RuntimeError(
            f"Expected data file for {ticker} but got HTML response "
            f"(Content-Type: {content_type}). URL may be incorrect: {download_url}"
        )

    out = _out_path(ticker, ext, output_dir)
    out.write_bytes(resp.content)
    logger.info("Saved %s (%d bytes) -> %s", ticker, len(resp.content), out)

    _save_last_modified(ticker, download_url)
    return out


def download_csv_dated(ticker: str, output_dir: Path | None = None) -> Path:
    """Download holdings file from a date-templated URL.

    Tries today's date first, then yesterday if today's file isn't available.
    Uses strftime codes in download_url_template (e.g., %%Y_%%m_%%d).
    """
    cfg = ETF_SOURCES[ticker]
    template = cfg["download_url_template"]
    ext = cfg["file_ext"]

    for offset in (0, 1):
        dt = datetime.now() - timedelta(days=offset)
        url = dt.strftime(template)
        label = "today" if offset == 0 else "yesterday"

        logger.info("Trying %s %s URL: %s", ticker, label, url)
        try:
            resp = _retry_get(url, max_retries=1)

            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type and ext in (".csv", ".xlsx", ".xls"):
                logger.warning("%s %s URL returned HTML, skipping", ticker, label)
                continue

            out = _out_path(ticker, ext, output_dir)
            out.write_bytes(resp.content)
            logger.info("Saved %s (%d bytes, %s) -> %s",
                        ticker, len(resp.content), label, out)
            return out

        except Exception as exc:
            logger.warning("%s %s download failed: %s", ticker, label, exc)
            continue

    raise RuntimeError(f"No file available for {ticker} (tried today and yesterday)")


# ─── Browser-Based Scraping ───────────────────────────────────────────────────

def download_browser(ticker: str, output_dir: Path | None = None) -> Path:
    """Scrape holdings from JS-rendered pages using Playwright.

    Falls back to requests+BeautifulSoup if Playwright is unavailable.
    """
    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    selector = cfg.get("selector", "table")
    out = _out_path(ticker, ".csv", output_dir)

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


# ─── Selenium Shared Utilities ───────────────────────────────────────────────

def _make_chrome_driver(download_dir: str, headless: bool = True):
    """Create a Chrome WebDriver with download directory configured."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def _selenium_click_by_text(driver, text: str) -> bool:
    """Find and click a button or link containing the given text."""
    from selenium.webdriver.common.by import By

    # Try buttons first
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        try:
            if text.lower() in btn.text.lower():
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Clicked button: '%s'", btn.text.strip())
                return True
        except Exception:
            continue

    # Try links
    for link in driver.find_elements(By.TAG_NAME, "a"):
        try:
            if text.lower() in link.text.lower():
                driver.execute_script("arguments[0].scrollIntoView(true);", link)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", link)
                logger.debug("Clicked link: '%s'", link.text.strip())
                return True
        except Exception:
            continue

    # Try other clickable elements (spans, divs, inputs)
    for tag in ("span", "div", "input"):
        for el in driver.find_elements(By.TAG_NAME, tag):
            try:
                el_text = el.text or el.get_attribute("value") or ""
                if text.lower() in el_text.lower():
                    driver.execute_script("arguments[0].scrollIntoView(true);", el)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", el)
                    logger.debug("Clicked %s: '%s'", tag, el_text.strip())
                    return True
            except Exception:
                continue

    return False


def _wait_for_download(download_dir: str, existing_files: set,
                       timeout: int = 20) -> str | None:
    """Wait for a new file to appear in the download directory."""
    patterns = ("*.csv", "*.xls", "*.xlsx")
    for i in range(timeout):
        time.sleep(1)
        current_files = set()
        for pat in patterns:
            current_files.update(glob.glob(os.path.join(download_dir, pat)))

        new_files = current_files - existing_files
        downloading = glob.glob(os.path.join(download_dir, "*.crdownload"))

        if new_files and not downloading:
            downloaded = sorted(new_files, key=os.path.getmtime)[-1]
            logger.debug("Download complete: %s", downloaded)
            return downloaded

        logger.debug("Waiting for download... (%d/%ds)", i + 1, timeout)

    return None


# ─── Generic Selenium Download ───────────────────────────────────────────────

def download_selenium(ticker: str, output_dir: Path | None = None,
                      headless: bool = True) -> Path:
    """Download ETF holdings by clicking through buttons on the provider's page.

    Uses the selenium_actions list from config to determine which buttons to click.
    Each action is: {"text": "Button Text", "wait_after": seconds}
    """
    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    actions = cfg.get("selenium_actions", [])
    ext = cfg.get("file_ext", ".csv")
    out = _out_path(ticker, ext, output_dir)

    tmp_dir = tempfile.mkdtemp(prefix=f"selenium_{ticker}_")
    tmp_dir_abs = os.path.abspath(tmp_dir)

    existing_files = set()
    for pat in ("*.csv", "*.xls", "*.xlsx"):
        existing_files.update(glob.glob(os.path.join(tmp_dir_abs, pat)))

    logger.info("Downloading %s via Selenium from %s", ticker, url)
    driver = _make_chrome_driver(tmp_dir_abs, headless=headless)

    try:
        driver.get(url)
        time.sleep(4)

        for i, action in enumerate(actions):
            text = action["text"]
            wait_after = action.get("wait_after", 3)

            logger.debug("Selenium action %d/%d for %s: click '%s'",
                         i + 1, len(actions), ticker, text)

            if not _selenium_click_by_text(driver, text):
                logger.warning("Could not find '%s' button for %s", text, ticker)

            time.sleep(wait_after)

        downloaded = _wait_for_download(tmp_dir_abs, existing_files)
        if not downloaded:
            raise RuntimeError(f"No file downloaded for {ticker} after clicking actions")

        shutil.copy2(downloaded, str(out))
        logger.info("Saved %s -> %s", ticker, out)
        return out

    finally:
        driver.quit()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── Selenium Table Scrape ───────────────────────────────────────────────────

def scrape_selenium(ticker: str, output_dir: Path | None = None,
                    headless: bool = True) -> Path:
    """Navigate to page with Selenium, click to expand, then extract table data.

    Used for sites like XOVR where clicking "VIEW ALL HOLDINGS" opens a
    modal/expanded section containing the holdings table.
    """
    from selenium.webdriver.common.by import By

    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    actions = cfg.get("selenium_actions", [])
    selector = cfg.get("selector", "table")
    out = _out_path(ticker, ".csv", output_dir)

    logger.info("Scraping %s via Selenium from %s", ticker, url)
    driver = _make_chrome_driver("/tmp", headless=headless)

    try:
        driver.get(url)
        time.sleep(4)

        for action in actions:
            text = action["text"]
            wait_after = action.get("wait_after", 3)

            if not _selenium_click_by_text(driver, text):
                logger.warning("Could not find '%s' button for %s", text, ticker)

            time.sleep(wait_after)

        # Extract table data
        tables = driver.find_elements(By.CSS_SELECTOR, selector)
        if not tables:
            tables = driver.find_elements(By.TAG_NAME, "table")

        if not tables:
            raise RuntimeError(f"No table found for {ticker} at {url}")

        # Use the largest table (most rows)
        best_rows = []
        for table in tables:
            rows_data = []
            trs = table.find_elements(By.TAG_NAME, "tr")
            for tr in trs:
                cells = tr.find_elements(By.TAG_NAME, "th") + tr.find_elements(By.TAG_NAME, "td")
                row = [cell.text.strip() for cell in cells]
                if any(row):
                    rows_data.append(row)
            if len(rows_data) > len(best_rows):
                best_rows = rows_data

        if not best_rows:
            raise RuntimeError(f"Table found but no data for {ticker}")

        with open(out, "w", newline="") as f:
            writer = csv.writer(f)
            for row in best_rows:
                writer.writerow(row)

        logger.info("Scraped %s (%d rows) -> %s", ticker, len(best_rows), out)
        return out

    finally:
        driver.quit()


# ─── Invesco Selenium Download ────────────────────────────────────────────────

def download_invesco(ticker: str, output_dir: Path | None = None,
                     headless: bool = True) -> Path:
    """Download Invesco ETF holdings via Selenium browser automation.

    Handles the Invesco-specific role-selection popup and cookie consent
    before clicking the Export Data button.
    """
    cfg = ETF_SOURCES[ticker]
    url = cfg["url"]
    out = _out_path(ticker, ".csv", output_dir)

    tmp_dir = tempfile.mkdtemp(prefix=f"invesco_{ticker}_")
    tmp_dir_abs = os.path.abspath(tmp_dir)

    existing_files = set(glob.glob(os.path.join(tmp_dir_abs, "*.csv")))

    logger.info("Downloading %s via Invesco Selenium from %s", ticker, url)
    driver = _make_chrome_driver(tmp_dir_abs, headless=headless)

    try:
        driver.get(url)
        time.sleep(4)

        _invesco_click_role(driver)
        time.sleep(4)

        _invesco_accept_cookies(driver)
        time.sleep(3)

        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(1)

        if not _selenium_click_by_text(driver, "Export"):
            raise RuntimeError(f"Could not find Export button for {ticker}")

        downloaded = _wait_for_download(tmp_dir_abs, existing_files)
        if not downloaded:
            raise RuntimeError(f"No CSV file downloaded for {ticker}")

        shutil.copy2(downloaded, str(out))
        logger.info("Saved %s -> %s", ticker, out)
        return out

    finally:
        driver.quit()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _invesco_click_role(driver) -> bool:
    """Click the 'Individual Investor' button on Invesco's role popup."""
    from selenium.webdriver.common.by import By

    for btn in driver.find_elements(By.TAG_NAME, "button"):
        try:
            if "Individual Investor" in btn.text:
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Clicked Individual Investor button")
                return True
        except Exception:
            continue

    try:
        btn = driver.find_element(By.CSS_SELECTOR, "[data-audiencetype='Investor']")
        driver.execute_script("arguments[0].click();", btn)
        return True
    except Exception:
        pass

    logger.warning("Could not find Invesco role selector, continuing anyway")
    return False


def _invesco_accept_cookies(driver) -> bool:
    """Accept cookie consent if the banner appears."""
    from selenium.webdriver.common.by import By

    try:
        for btn in driver.find_elements(By.TAG_NAME, "button"):
            if "Accept" in btn.text:
                driver.execute_script("arguments[0].click();", btn)
                logger.debug("Accepted cookies")
                return True
    except Exception:
        pass
    return False


# ─── SEC EDGAR ────────────────────────────────────────────────────────────────

def download_edgar_filing(ticker: str, output_dir: Path | None = None) -> Path | None:
    """Fetch the latest N-PORT filing from SEC EDGAR for an ETF."""
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

    # SEC rate-limits to 10 req/s -- be polite
    time.sleep(0.2)

    out = _out_path(ticker, "_edgar.json", output_dir)
    out.write_text(json.dumps(data, indent=2))
    logger.info("Saved EDGAR data for %s -> %s", ticker, out)
    return out


# ─── Composite Downloads ──────────────────────────────────────────────────────

def download_etf(ticker: str, output_dir: Path | None = None) -> dict:
    """Download holdings for a single ETF using the appropriate method."""
    if ticker not in ETF_SOURCES:
        raise ValueError(f"Unknown ETF ticker: {ticker}")

    cfg = ETF_SOURCES[ticker]
    method = cfg["method"]
    result = {"ticker": ticker, "holdings": None, "edgar": None, "error": None}

    try:
        if method == "csv":
            result["holdings"] = download_csv(ticker, output_dir)
        elif method == "csv_dated":
            result["holdings"] = download_csv_dated(ticker, output_dir)
        elif method == "csv+edgar":
            result["holdings"] = download_csv(ticker, output_dir, check_modified=True)
        elif method == "browser":
            result["holdings"] = download_browser(ticker, output_dir)
        elif method == "selenium":
            result["holdings"] = download_selenium(ticker, output_dir)
        elif method == "selenium_scrape":
            result["holdings"] = scrape_selenium(ticker, output_dir)
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
    """Download holdings for all (or specified) ETFs."""
    tickers = tickers or list(ETF_SOURCES.keys())
    output_dir = output_dir or _today_dir()

    logger.info("Starting bulk download for %d ETFs -> %s", len(tickers), output_dir)
    results = []

    for ticker in tickers:
        result = download_etf(ticker, output_dir)
        results.append(result)
        time.sleep(0.5)

    succeeded = sum(1 for r in results if r["holdings"] is not None)
    failed = sum(1 for r in results if r["error"] is not None)
    logger.info("Bulk download complete: %d succeeded, %d failed", succeeded, failed)

    return results
