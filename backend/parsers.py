"""
Parse downloaded holdings files into the format expected by app.py.

Each ETF in app.py expects top_holdings as:
    [{"name": str, "ticker": str, "weight": float, "price": float, "change": float}, ...]

Parsers handle CSV, XLSX, and HTML table formats with provider-specific
column mapping since each issuer uses different column names.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.config import ETF_SOURCES, PARSED_DIR

logger = logging.getLogger(__name__)


# ─── Column Mapping ───────────────────────────────────────────────────────────
# Maps provider column names -> our standard names.
# Parsers try each alias until one matches.

COLUMN_ALIASES = {
    "name": ["name", "holding", "security", "description", "issuer", "security name",
             "holding name", "company", "fund name"],
    "ticker": ["ticker", "symbol", "cusip", "isin", "identifier", "security identifier"],
    "weight": ["weight", "weight (%)", "% of net assets", "portfolio %", "pct",
               "percent", "weighting", "% of fund", "market value %", "allocation"],
    "price": ["price", "market price", "closing price", "last price", "nav",
              "market value per share", "close"],
    "shares": ["shares", "quantity", "shares held", "par value", "notional"],
    "market_value": ["market value", "value", "market val", "notional value",
                     "total value", "mv"],
    "coupon": ["coupon", "coupon rate", "coupon (%)"],
    "maturity": ["maturity", "maturity date", "mat date"],
    "rating": ["rating", "credit rating", "s&p rating", "moody's"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map provider-specific column names to standard names."""
    col_lower = {c: c.strip().lower() for c in df.columns}
    rename_map = {}

    for std_name, aliases in COLUMN_ALIASES.items():
        for orig_col, lower_col in col_lower.items():
            if lower_col in aliases and std_name not in rename_map.values():
                rename_map[orig_col] = std_name
                break

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _clean_numeric(val):
    """Parse a potentially messy numeric string."""
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", "").replace("%", "").replace("$", "")
    if s in ("", "-", "--", "N/A", "n/a"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_holdings_list(df: pd.DataFrame, max_rows: int = 10) -> list[dict]:
    """Convert a DataFrame to the top_holdings list format expected by app.py."""
    df = _normalize_columns(df)

    holdings = []
    for _, row in df.head(max_rows).iterrows():
        holding = {
            "name": str(row.get("name", "Unknown")).strip(),
            "ticker": str(row.get("ticker", "")).strip(),
            "weight": _clean_numeric(row.get("weight", 0)),
            "price": _clean_numeric(row.get("price", 0)),
            "change": _clean_numeric(row.get("change", 0)),
        }
        # Include extra fields if present (bonds, CLOs)
        if "coupon" in row.index:
            holding["coupon"] = _clean_numeric(row["coupon"])
        if "maturity" in row.index:
            holding["maturity"] = str(row["maturity"]).strip()
        if "rating" in row.index:
            holding["rating"] = str(row["rating"]).strip()
        if "market_value" in row.index:
            holding["market_value"] = _clean_numeric(row["market_value"])

        holdings.append(holding)

    return holdings


# ─── File Parsers ─────────────────────────────────────────────────────────────

def parse_csv(filepath: Path) -> pd.DataFrame:
    """Parse a CSV holdings file, handling common formatting quirks."""
    # Try reading and skip metadata rows (some providers add headers above data)
    try:
        df = pd.read_csv(filepath)
    except Exception:
        # Some CSVs have non-standard encoding or delimiter
        df = pd.read_csv(filepath, encoding="latin-1", sep=None, engine="python")

    # Drop rows that are all NaN (separator rows)
    df = df.dropna(how="all")

    # Some providers put a "Total" or summary row at the bottom
    if len(df) > 0:
        last_row_str = " ".join(str(v) for v in df.iloc[-1].values if pd.notna(v)).lower()
        if any(word in last_row_str for word in ("total", "subtotal", "grand total")):
            df = df.iloc[:-1]

    return df


def parse_xlsx(filepath: Path) -> pd.DataFrame:
    """Parse an XLSX holdings file (used by SSGA for PRIV/PRSD).

    Handles BadZipFile errors from corrupt or non-XLSX files (e.g. HTML
    error pages saved with .xlsx extension).
    """
    from zipfile import BadZipFile

    # SSGA files typically have metadata rows at the top
    try:
        df = pd.read_excel(filepath, engine="openpyxl")
    except BadZipFile:
        logger.error("File is not a valid XLSX (BadZipFile): %s", filepath)
        return pd.DataFrame()
    except Exception:
        try:
            # Try skipping header rows
            df = pd.read_excel(filepath, engine="openpyxl", skiprows=4)
        except BadZipFile:
            logger.error("File is not a valid XLSX (BadZipFile): %s", filepath)
            return pd.DataFrame()

    df = df.dropna(how="all")

    # Find the actual header row if first column looks like metadata
    for i in range(min(10, len(df))):
        row_values = [str(v).strip().lower() for v in df.iloc[i].values if pd.notna(v)]
        if any(alias in row_values for aliases in COLUMN_ALIASES.values() for alias in aliases):
            df.columns = df.iloc[i]
            df = df.iloc[i + 1:].reset_index(drop=True)
            break

    return df


def parse_etf_file(ticker: str, filepath: Path) -> list[dict]:
    """Parse a single ETF's holdings file into the app.py format.

    Returns list of holding dicts: [{"name", "ticker", "weight", "price", "change"}, ...]
    """
    if not filepath.exists():
        logger.warning("File not found: %s", filepath)
        return []

    ext = filepath.suffix.lower()

    if ext == ".csv":
        df = parse_csv(filepath)
    elif ext in (".xlsx", ".xls"):
        df = parse_xlsx(filepath)
    else:
        logger.warning("Unsupported file format %s for %s", ext, ticker)
        return []

    if df.empty:
        logger.warning("Empty data for %s from %s", ticker, filepath)
        return []

    # Sort by weight descending if weight column exists
    df = _normalize_columns(df)
    if "weight" in df.columns:
        df["weight"] = df["weight"].apply(_clean_numeric)
        df = df.sort_values("weight", ascending=False)

    holdings = _to_holdings_list(df)
    logger.info("Parsed %s: %d holdings from %s", ticker, len(holdings), filepath)
    return holdings


# ─── Directory-Level Parsing ──────────────────────────────────────────────────

def parse_holdings_dir(holdings_dir: Path) -> dict[str, list[dict]]:
    """Parse all holdings files in a dated directory.

    Returns: {"BIZD": [...holdings...], "PBDC": [...holdings...], ...}

    Errors parsing individual files are caught and logged so that one
    corrupt file does not crash the entire pipeline.
    """
    results = {}

    for ticker, cfg in ETF_SOURCES.items():
        ext = cfg["file_ext"]
        filepath = holdings_dir / f"{ticker}_holdings{ext}"

        if filepath.exists():
            try:
                holdings = parse_etf_file(ticker, filepath)
                if holdings:
                    results[ticker] = holdings
            except Exception as exc:
                logger.error("Failed to parse %s from %s: %s", ticker, filepath, exc)
        else:
            logger.debug("No file for %s at %s", ticker, filepath)

    logger.info("Parsed %d/%d ETFs from %s", len(results), len(ETF_SOURCES), holdings_dir)
    return results


def save_parsed(parsed: dict[str, list[dict]], date_str: str | None = None) -> Path:
    """Save parsed holdings to JSON in the parsed directory."""
    date_str = date_str or datetime.now().strftime("%Y%m%d")
    PARSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PARSED_DIR / f"holdings_{date_str}.json"
    out.write_text(json.dumps(parsed, indent=2, default=str))
    logger.info("Saved parsed holdings -> %s", out)
    return out


def load_parsed(date_str: str | None = None) -> dict[str, list[dict]] | None:
    """Load previously parsed holdings from JSON.

    Returns None if no parsed file exists for the given date.
    """
    date_str = date_str or datetime.now().strftime("%Y%m%d")
    path = PARSED_DIR / f"holdings_{date_str}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None
