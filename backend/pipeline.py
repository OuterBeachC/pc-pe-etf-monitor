"""
Daily pipeline orchestrator for ETF holdings retrieval.

This is the Python equivalent of the master bash pipeline in app.py's
Automation tab. It coordinates:
  1. Download holdings from all ETF providers
  2. Parse downloaded files into standard format
  3. Compare with previous day's holdings
  4. Generate alerts (price moves, private allocation, diffs)
  5. Save results

Usage:
    python -m backend.pipeline                    # Run full pipeline
    python -m backend.pipeline --tickers BIZD,RONB  # Run specific ETFs
    python -m backend.pipeline --parse-only       # Skip download, just parse existing files
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from backend.config import DATA_DIR, ETF_SOURCES, LOG_DIR
from backend.database import Database
from backend.retrieval import download_all, _today_dir
from backend.parsers import parse_holdings_dir, save_parsed, load_parsed
from backend.alerts import (
    check_price_moves,
    check_private_allocation,
    diff_holdings,
    save_alerts,
)

logger = logging.getLogger(__name__)


def _setup_logging(date_str: str) -> None:
    """Configure logging to both console and dated log file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{date_str}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ],
    )


def run_pipeline(
    tickers: list[str] | None = None,
    skip_download: bool = False,
    output_dir: Path | None = None,
) -> dict:
    """Run the full daily pipeline.

    Args:
        tickers: List of ETF tickers to process (None = all)
        skip_download: If True, skip download and parse existing files
        output_dir: Override output directory (default: data/holdings/YYYYMMDD)

    Returns:
        Pipeline result dict with download results, parsed data, and alerts.
    """
    date_str = datetime.now().strftime("%Y%m%d")
    _setup_logging(date_str)

    output_dir = output_dir or _today_dir()
    logger.info("=" * 60)
    logger.info("PC/PE ETF Pipeline -- %s", date_str)
    logger.info("Output: %s", output_dir)
    logger.info("ETFs: %s", ", ".join(tickers) if tickers else "ALL")
    logger.info("=" * 60)

    result = {
        "date": date_str,
        "output_dir": str(output_dir),
        "downloads": [],
        "parsed": {},
        "alerts": {},
        "errors": [],
    }

    # Open database connection for the run
    db = Database()
    run_id = db.start_pipeline_run(date_str)
    logger.info("Pipeline run #%d started, writing to %s", run_id, db.path)

    # ── Step 1: Download ──
    if not skip_download:
        logger.info("Step 1/5: Downloading holdings...")
        download_results = download_all(output_dir, tickers)
        result["downloads"] = [
            {k: str(v) if isinstance(v, Path) else v for k, v in r.items()}
            for r in download_results
        ]
        errors = [r for r in download_results if r.get("error")]
        if errors:
            for e in errors:
                logger.error("Download failed: %s -- %s", e["ticker"], e["error"])
                result["errors"].append(f"Download {e['ticker']}: {e['error']}")
    else:
        logger.info("Step 1/5: Download skipped (--parse-only)")

    # ── Step 2: Parse ──
    logger.info("Step 2/5: Parsing holdings files...")
    parsed = parse_holdings_dir(output_dir)
    save_parsed(parsed, date_str)
    result["parsed"] = {t: len(h) for t, h in parsed.items()}

    if not parsed:
        logger.warning("No holdings parsed -- pipeline stopping early")
        db.complete_pipeline_run(run_id, result)
        db.close()
        return result

    # ── Step 3: Diff with yesterday ──
    logger.info("Step 3/5: Comparing with previous day...")
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    yesterday_parsed = load_parsed(yesterday_str)

    holdings_diff = {}
    if yesterday_parsed:
        holdings_diff = diff_holdings(parsed, yesterday_parsed)
        logger.info("Changes found in %d ETFs", len(holdings_diff))
    else:
        logger.info("No previous day data -- skipping diff")

    # ── Step 4: Generate alerts ──
    logger.info("Step 4/5: Generating alerts...")
    price_alerts = check_price_moves(parsed)
    allocation_alerts = check_private_allocation(parsed)

    alerts_path = save_alerts(price_alerts, allocation_alerts, holdings_diff, date_str)
    result["alerts"] = {
        "price_moves": len(price_alerts),
        "allocation_warnings": sum(1 for a in allocation_alerts if a["exceeds"]),
        "holdings_changes": len(holdings_diff),
        "file": str(alerts_path),
    }

    # ── Step 5: Persist to database ──
    logger.info("Step 5/5: Writing to database...")
    db.insert_holdings(date_str, parsed)
    db.insert_alerts(date_str, price_alerts, allocation_alerts, holdings_diff)
    db.complete_pipeline_run(run_id, result)

    stats = db.table_stats()
    logger.info("Database stats: %s", stats)
    db.close()

    # ── Summary ──
    logger.info("=" * 60)
    logger.info("Pipeline complete")
    logger.info("  Downloaded: %d ETFs", len(result["downloads"]))
    logger.info("  Parsed:     %d ETFs", len(parsed))
    logger.info("  Alerts:     %d price, %d allocation, %d diff",
                len(price_alerts),
                sum(1 for a in allocation_alerts if a["exceeds"]),
                len(holdings_diff))
    logger.info("  Database:   %s", db.path)
    if result["errors"]:
        logger.warning("  Errors:     %d", len(result["errors"]))
    logger.info("=" * 60)

    return result


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PC/PE ETF Holdings Daily Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backend.pipeline                        # Full run, all ETFs
  python -m backend.pipeline --tickers BIZD,RONB    # Specific ETFs only
  python -m backend.pipeline --parse-only           # Parse existing files
  python -m backend.pipeline --output /tmp/test     # Custom output dir
        """,
    )
    parser.add_argument(
        "--tickers",
        type=lambda s: s.split(","),
        default=None,
        help="Comma-separated list of ETF tickers (default: all)",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip download, only parse existing files in today's directory",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override output directory path",
    )

    args = parser.parse_args()

    result = run_pipeline(
        tickers=args.tickers,
        skip_download=args.parse_only,
        output_dir=args.output,
    )

    # Print summary as JSON for scripting
    print(json.dumps(result, indent=2, default=str))

    # Exit with error code if there were failures
    sys.exit(1 if result["errors"] else 0)


if __name__ == "__main__":
    main()
