"""
Configuration for ETF data retrieval -- URLs, paths, and constants.

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
PRICE_MOVE_THRESHOLD = 5.0         # percent -- flag holdings with moves > this
PRIVATE_ALLOC_THRESHOLD = 15.0     # percent -- SEC illiquidity guideline
PRIVATE_TICKERS = {"SPACEX", "XAI", "ANTHR", "APC-1", "APC-2", "APC-3"}

# ─── Request Defaults ────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 30               # seconds
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"}

# ─── ETF Source Definitions ───────────────────────────────────────────────────
# method: "csv"            -> direct HTTP download (requests.get)
# method: "csv_dated"      -> direct download with date-templated URL
# method: "csv+edgar"      -> HTTP download + SEC EDGAR for opaque sleeve
# method: "browser"        -> Playwright/BS4 table scrape
# method: "selenium"       -> Selenium click-to-download (generic)
# method: "selenium_scrape"-> Selenium navigate + extract table from page/modal
# method: "invesco"        -> Invesco-specific Selenium flow (role popup + export)

ETF_SOURCES = {
    # ── Private Credit ──
    "BIZD": {
        "name": "VanEck BDC Income ETF",
        "category": "Private Credit",
        "method": "selenium",
        "url": "https://www.vaneck.com/us/en/investments/bdc-income-etf-bizd/holdings/",
        "selenium_actions": [
            {"text": "Download XLS", "wait_after": 5},
        ],
        "file_ext": ".xls",
        "notes": "Selenium clicks Download XLS button on VanEck holdings page.",
    },
    "HYIN": {
        "name": "WisdomTree Alternative Income Fund",
        "category": "Private Credit",
        "method": "selenium",
        "url": "https://www.wisdomtree.com/investments/etfs/alternative/hyin#",
        "selenium_actions": [
            {"text": "ALL HOLDINGS", "wait_after": 3},
            {"text": "Export Holdings", "wait_after": 5},
        ],
        "file_ext": ".csv",
        "notes": "Click ALL HOLDINGS to expand, then Export Holdings to download.",
    },
    "PBDC": {
        "name": "Putnam BDC Income ETF",
        "category": "Private Credit",
        "method": "selenium",
        "url": "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39500/SINGLCLASS/putnam-bdc-income-etf/PBDC#portfolio",
        "selenium_actions": [
            {"text": "XLS", "wait_after": 5},
        ],
        "file_ext": ".xls",
        "notes": "Franklin Templeton. Click XLS button on portfolio tab.",
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
        "url": "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-priv.xlsx",
        "download_url": "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-priv.xlsx",
        "edgar_query": "PRIV",
        "edgar_form": "NPORT-P",
        "file_ext": ".xlsx",
        "notes": "Public holdings via XLSX. Private sleeve (~8%) opaque -- use EDGAR quarterly.",
    },
    "PCMM": {
        "name": "BondBloxx Private Credit CLO ETF",
        "category": "Private Credit",
        "method": "selenium",
        "url": "https://bondbloxxetf.com/bondbloxx-private-credit-clo-etf/#portfolio",
        "selenium_actions": [
            {"text": "Download CSV", "wait_after": 5},
        ],
        "file_ext": ".csv",
        "notes": "BondBloxx. Click Download CSV on portfolio section.",
    },
    "PCR": {
        "name": "Simplify VettaFi Private Credit Strategy ETF",
        "category": "Private Credit",
        "method": "csv_dated",
        "url": "https://www.simplify.us/etfs/pcr",
        "download_url_template": "https://www.simplify.us/sites/default/files/excel_holdings/%Y_%m_%d_Simplify_Portfolio_EOD_Tracker.xlsx",
        "filter_fund": "PCR",
        "file_ext": ".xlsx",
        "notes": "Multi-fund XLSX. Filter rows where FUND NAME column = PCR.",
    },
    "HBDC": {
        "name": "Hilton BDC Corporate Bond ETF",
        "category": "Private Credit",
        "method": "browser",
        "url": "https://www.hiltonetfs.com/hbdc-all-holdings",
        "selector": "table",
        "file_ext": ".csv",
        "notes": "Scrape table from page. Verify date in '(as of MM/DD/YYYY)' header.",
    },
    "PRSD": {
        "name": "State Street Short Duration IG Public Private Credit ETF",
        "category": "Private Credit",
        "method": "csv+edgar",
        "url": "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-prsd.xlsx",
        "download_url": "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-prsd.xlsx",
        "edgar_query": "PRSD",
        "edgar_form": "NPORT-P",
        "file_ext": ".xlsx",
        "notes": "Same as PRIV. Public sleeve transparent, private sleeve opaque.",
    },
    # ── Invesco (Selenium download) ──
    "GTO": {
        "name": "Invesco Total Return Bond ETF",
        "category": "Private Credit",
        "method": "invesco",
        "url": "https://www.invesco.com/us/financial-products/etfs/holdings?audienceType=Investor&ticker=GTO",
        "download_pattern": "invesco_total_return_bond_etf-monthly_holdings*.csv",
        "file_ext": ".csv",
        "notes": "Selenium export from Invesco holdings page. Monthly holdings CSV.",
    },
    "GTOC": {
        "name": "Invesco Core Fixed Income ETF",
        "category": "Private Credit",
        "method": "invesco",
        "url": "https://www.invesco.com/us/financial-products/etfs/holdings?audienceType=Investor&ticker=GTOC",
        "download_pattern": "invesco_core_fixed_income_etf-monthly_holdings*.csv",
        "file_ext": ".csv",
        "notes": "Selenium export from Invesco holdings page. Monthly holdings CSV.",
    },
    # ── Private Equity ──
    "XOVR": {
        "name": "ERShares Private-Public Crossover ETF",
        "category": "Private Equity",
        "method": "selenium_scrape",
        "url": "https://entrepreneurshares.com/xovr-etf/#fund-top-10-holdings",
        "selenium_actions": [
            {"text": "VIEW ALL HOLDINGS", "wait_after": 3},
        ],
        "selector": "table",
        "edgar_query": "ERShares XOVR",
        "edgar_form": "NPORT-P",
        "file_ext": ".csv",
        "notes": "Click VIEW ALL HOLDINGS, scrape modal table. SpaceX SPV fair-valued daily.",
    },
    "AGIX": {
        "name": "KraneShares AI & Technology ETF",
        "category": "Private Equity",
        "method": "csv_dated",
        "url": "https://kraneshares.com/etf/agix/",
        "download_url_template": "https://kraneshares.com/csv/%m_%d_%Y_agix_holdings.csv",
        "file_ext": ".csv",
        "notes": "Date-stamped CSV. Private positions (SpaceX, Anthropic) held via SPVs.",
    },
    "RONB": {
        "name": "Baron First Principles ETF",
        "category": "Private Equity",
        "method": "csv_dated",
        "url": "https://www.baroncapitalgroup.com/product-detail/baron-first-principles-etf-ronb",
        "download_url_template": "https://www.baroncapitalgroup.com/api/product/media/csv/RONB-HOLDINGS-%Y%m%d-0.csv?product_type=etf-downloads&id=a02798d8-cb16-49e0-bbdc-eb1315aa4cbf",
        "file_ext": ".csv",
        "notes": "Date-stamped CSV via API. SpaceX ~21.5%%, xAI ~5.4%% -- ~27%% private.",
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
