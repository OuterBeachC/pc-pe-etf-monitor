"""
Configuration for ETF data retrieval — URLs, paths, and constants.

Each ETF_SOURCES entry mirrors the automation configs in app.py and defines
the download method, URL, expected file format, and any special handling.
"""

import os
from pathlib import Path

# ─── Directories ──────────────────────────────────────────────────────────────
BASE_DIR = Path(os.environ.get("ETF_DATA_DIR", "data"))
DATA_DIR = BASE_DIR / "holdings"
PARSED_DIR = BASE_DIR / "parsed"
ALERTS_DIR = BASE_DIR / "alerts"
LOG_DIR = BASE_DIR / "logs"

# ─── Alert Thresholds ────────────────────────────────────────────────────────
PRICE_MOVE_THRESHOLD = 5.0         # percent — flag holdings with moves > this
PRIVATE_ALLOC_THRESHOLD = 15.0     # percent — SEC illiquidity guideline
PRIVATE_TICKERS = {"SPACEX", "XAI", "ANTHR", "APC-1", "APC-2", "APC-3"}

# ─── Request Defaults ────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 30               # seconds
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"}

# ─── ETF Source Definitions ───────────────────────────────────────────────────
# method: "csv"      → direct HTTP download (requests.get)
# method: "browser"  → needs headless browser (Playwright)
# method: "csv+edgar"→ CSV download + SEC EDGAR for opaque sleeve

ETF_SOURCES = {
    # ── Private Credit ──
    "BIZD": {
        "name": "VanEck BDC Income ETF",
        "category": "Private Credit",
        "method": "csv",
        "url": "https://www.vaneck.com/us/en/investments/bdc-income-etf-bizd/holdings/",
        "download_url": "https://www.vaneck.com/us/en/investments/bdc-income-etf-bizd/holdings/?download=csv",
        "file_ext": ".csv",
        "notes": "Direct CSV link available. Append date parameter.",
    },
    "PBDC": {
        "name": "Putnam BDC Income ETF",
        "category": "Private Credit",
        "method": "browser",
        "url": "https://www.franklintempleton.com/.../PBDC",
        "selector": '[data-testid="holdings-table"]',
        "file_ext": ".csv",
        "notes": "Holdings table rendered client-side. Use headless browser.",
    },
    "VPC": {
        "name": "Virtus Private Credit Strategy ETF",
        "category": "Private Credit",
        "method": "browser",
        "url": "https://www.virtus.com/products/virtus-private-credit-strategy-etf#holdings",
        "selector": "table.holdings-table",
        "file_ext": ".csv",
        "notes": "Paginated table. No direct CSV. Scrape with headless browser.",
    },
    "PRIV": {
        "name": "SPDR SSGA IG Public & Private Credit ETF",
        "category": "Private Credit",
        "method": "csv+edgar",
        "url": "https://www.ssga.com/.../priv",
        "download_url": "https://www.ssga.com/.../holdings-daily-us-en-priv.xlsx",
        "edgar_query": "PRIV",
        "edgar_form": "NPORT-P",
        "file_ext": ".xlsx",
        "notes": "Public holdings via XLSX. Private sleeve (~8%) opaque — use EDGAR quarterly.",
    },
    "PCMM": {
        "name": "BondBloxx Private Credit CLO ETF",
        "category": "Private Credit",
        "method": "csv",
        "url": "https://bondbloxxetf.com/bondbloxx-private-credit-clo-etf/",
        "download_url": "https://bondbloxxetf.com/fund-data/pcmm-holdings.csv",
        "file_ext": ".csv",
        "notes": "Holdings CSV with CUSIP, coupon, maturity.",
    },
    "PCR": {
        "name": "Simplify VettaFi Private Credit Strategy ETF",
        "category": "Private Credit",
        "method": "csv",
        "url": "https://www.simplify.us/etfs/pcr",
        "download_url": "https://www.simplify.us/etfs/pcr/holdings?format=csv",
        "file_ext": ".csv",
        "notes": "CSV export. Credit hedge positions (TRS) require manual monitoring.",
    },
    "HBDC": {
        "name": "Hilton BDC Corporate Bond ETF",
        "category": "Private Credit",
        "method": "csv",
        "url": "https://www.hiltoncapitalmanagement.com/hbdc",
        "download_url": "https://www.hiltoncapitalmanagement.com/hbdc/holdings/download",
        "file_ext": ".csv",
        "notes": "BDC bond holdings with CUSIP, coupon, maturity, rating.",
    },
    "PRSD": {
        "name": "State Street Short Duration IG Public Private Credit ETF",
        "category": "Private Credit",
        "method": "csv+edgar",
        "url": "https://www.ssga.com/.../prsd",
        "download_url": "https://www.ssga.com/.../holdings-daily-us-en-prsd.xlsx",
        "edgar_query": "PRSD",
        "edgar_form": "NPORT-P",
        "file_ext": ".xlsx",
        "notes": "Same as PRIV. Public sleeve transparent, private sleeve opaque.",
    },
    # ── Private Equity ──
    "XOVR": {
        "name": "ERShares Private-Public Crossover ETF",
        "category": "Private Equity",
        "method": "browser",
        "url": "https://entrepreneurshares.com/",
        "selector": '[class*="holdings"]',
        "edgar_query": "ERShares XOVR",
        "edgar_form": "NPORT-P",
        "file_ext": ".csv",
        "notes": "SpaceX SPV fair-valued daily under Rule 2a-5. Monitor N-PORT filings.",
    },
    "AGIX": {
        "name": "KraneShares AI & Technology ETF",
        "category": "Private Equity",
        "method": "csv",
        "url": "https://kraneshares.com/etf/agix/",
        "download_url": "https://kraneshares.com/etf/agix/holdings/?format=csv",
        "file_ext": ".csv",
        "notes": "CSV export available. Private positions (SpaceX, Anthropic) held via SPVs.",
    },
    "RONB": {
        "name": "Baron First Principles ETF",
        "category": "Private Equity",
        "method": "csv",
        "url": "https://www.baroncapitalgroup.com/product-detail/baron-first-principles-etf-ronb",
        "download_url": "https://www.baroncapitalgroup.com/.../ronb/holdings/download",
        "file_ext": ".csv",
        "notes": "SpaceX 21.5%, xAI 5.4% — ~27% private. Monitor 15% illiquidity rule.",
    },
}

# ─── SEC EDGAR ────────────────────────────────────────────────────────────────
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_USER_AGENT = "PCPEETFMonitor/1.0 (contact@example.com)"  # SEC requires identifying UA

# ─── Tickers requiring EDGAR supplementary data ──────────────────────────────
EDGAR_ETFS = {
    ticker: cfg for ticker, cfg in ETF_SOURCES.items()
    if cfg["method"] == "csv+edgar" or cfg.get("edgar_query")
}
