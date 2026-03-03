# Private Credit & Equity ETF Monitor

Streamlit dashboard and automated pipeline tracking 15 private credit and private equity ETFs.

## Quick Start

```bash
pip install -r requirements.txt
streamlit run app.py
```

Opens at `http://localhost:8501`

### Selenium-based ETFs (optional)

Several ETFs require Chrome + Selenium for download. To enable these:

```bash
pip install selenium webdriver-manager
```

Chrome or Chromium must be installed on the system. The `webdriver-manager` package handles ChromeDriver automatically.

## Running the Pipeline

```bash
python -m backend                           # Full run, all ETFs
python -m backend --tickers BIZD,PRIV       # Specific ETFs only
python -m backend --parse-only              # Parse existing files, skip download
python -m backend --output /tmp/test        # Custom output directory
```

The pipeline runs five steps:
1. **Download** holdings from each ETF provider
2. **Parse** downloaded files into a standard format
3. **Diff** with previous day's holdings
4. **Alert** on price moves, private allocation thresholds, and holdings changes
5. **Persist** results to SQLite database

Output goes to `data/holdings/YYYYMMDD/` with logs in `data/logs/`.

## ETFs Tracked

### Private Credit (12)

| Ticker | Name | Method | Source |
|--------|------|--------|--------|
| BIZD | VanEck BDC Income ETF | Selenium | VanEck (XLS download) |
| HYIN | WisdomTree Alternative Income Fund | Selenium | WisdomTree (Export Holdings) |
| PBDC | Putnam BDC Income ETF | Selenium | Franklin Templeton (XLS) |
| VPC | Virtus Private Credit Strategy ETF | Browser scrape | Virtus |
| PRIV | SPDR SSGA IG Public & Private Credit ETF | Direct XLSX + EDGAR | SSGA |
| PCMM | BondBloxx Private Credit CLO ETF | Selenium | BondBloxx (CSV download) |
| PCR | Simplify VettaFi Private Credit Strategy ETF | Date-templated XLSX | Simplify |
| HBDC | Hilton BDC Corporate Bond ETF | Browser scrape | Hilton ETFs |
| PRSD | State Street Short Duration IG Public Private Credit ETF | Direct XLSX + EDGAR | SSGA |
| GTO | Invesco Total Return Bond ETF | Selenium (Invesco) | Invesco |
| GTOC | Invesco Core Fixed Income ETF | Selenium (Invesco) | Invesco |

### Private Equity (3)

| Ticker | Name | Method | Source |
|--------|------|--------|--------|
| XOVR | ERShares Private-Public Crossover ETF | Selenium scrape | EntrepreneurShares |
| AGIX | KraneShares AI & Technology ETF | Date-templated CSV | KraneShares |
| RONB | Baron First Principles ETF | Date-templated CSV | Baron Capital |

## Retrieval Methods

| Method | Description | Dependencies |
|--------|-------------|--------------|
| `csv` | Direct HTTP download | requests |
| `csv_dated` | HTTP download with date-templated URL (tries today, then yesterday) | requests |
| `csv+edgar` | Direct download + SEC EDGAR N-PORT filing for opaque sleeve | requests |
| `browser` | Playwright headless scrape, falls back to requests + BeautifulSoup | playwright or beautifulsoup4 |
| `selenium` | Selenium clicks configurable buttons to trigger file download | selenium, webdriver-manager |
| `selenium_scrape` | Selenium clicks to expand content, then extracts table data | selenium, webdriver-manager |
| `invesco` | Invesco-specific Selenium flow (role popup + cookie consent + export) | selenium, webdriver-manager |

## Project Structure

```
backend/
  __main__.py       # Entry point for python -m backend
  config.py         # ETF sources, URLs, retrieval methods, thresholds
  retrieval.py      # Download/scrape logic for all retrieval methods
  parsers.py        # Parse CSV/XLSX/HTML into standard holdings format
  pipeline.py       # Daily pipeline orchestrator (download -> parse -> diff -> alert -> persist)
  alerts.py         # Price move, allocation, and holdings change detection
  database.py       # SQLite persistence layer
  seed.py           # Initial database seeding with ETF metadata
app.py              # Streamlit dashboard
requirements.txt    # Python dependencies
data/               # Output directory (gitignored)
  holdings/         # Raw downloaded files by date (YYYYMMDD/)
  parsed/           # Parsed JSON holdings by date
  alerts/           # Generated alerts by date
  logs/             # Pipeline run logs
  database.db       # SQLite database
```

## Configuration

ETF sources are defined in `backend/config.py` in the `ETF_SOURCES` dict. Each entry specifies:
- `method` -- retrieval strategy (see table above)
- `url` -- provider page URL
- `download_url` or `download_url_template` -- direct download URL (for csv/csv_dated methods)
- `selenium_actions` -- list of button clicks for Selenium methods
- `file_ext` -- expected file extension
- `filter_fund` -- optional fund-name filter for multi-fund files (e.g., PCR)
- `edgar_query` / `edgar_form` -- SEC EDGAR filing parameters

Alert thresholds:
- `PRICE_MOVE_THRESHOLD` (default 5%) -- flag holdings with price changes exceeding this
- `PRIVATE_ALLOC_THRESHOLD` (default 15%) -- SEC illiquidity guideline

## Deploy to Streamlit Cloud

1. Push to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select the repo and set main file to `app.py`
5. Click Deploy

Note: Selenium-based ETF downloads will not work on Streamlit Cloud. The dashboard will display data from the SQLite database, which should be populated by running the pipeline locally or on a server with Chrome installed.
