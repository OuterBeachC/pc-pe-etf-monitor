"""
Backend file retrieval for PC/PE ETF Monitor.

Downloads holdings data from ETF providers, parses into the format
expected by app.py, and generates alerts for notable changes.
"""

from backend.config import DATA_DIR, PARSED_DIR, ETF_SOURCES
from backend.database import Database
from backend.retrieval import download_all, download_etf
from backend.parsers import parse_holdings_dir, parse_etf_file
from backend.alerts import check_price_moves, check_private_allocation, diff_holdings
from backend.pipeline import run_pipeline

__all__ = [
    "DATA_DIR",
    "PARSED_DIR",
    "ETF_SOURCES",
    "Database",
    "download_all",
    "download_etf",
    "parse_holdings_dir",
    "parse_etf_file",
    "check_price_moves",
    "check_private_allocation",
    "diff_holdings",
    "run_pipeline",
]
