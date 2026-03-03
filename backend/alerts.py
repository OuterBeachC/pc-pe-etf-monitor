"""
Alert generation for ETF holdings monitoring.

Three alert categories matching the app.py risk monitoring:
  1. Price moves     -- Flag holdings with price changes exceeding threshold
  2. Private allocation -- Check private/illiquid % vs SEC 15% guideline
  3. Holdings diff   -- Compare day-over-day changes to detect rebalances
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from backend.config import (
    ALERTS_DIR,
    PRICE_MOVE_THRESHOLD,
    PRIVATE_ALLOC_THRESHOLD,
    PRIVATE_TICKERS,
)

logger = logging.getLogger(__name__)


# ─── Price Move Alerts ────────────────────────────────────────────────────────

def check_price_moves(
    parsed_holdings: dict[str, list[dict]],
    threshold: float = PRICE_MOVE_THRESHOLD,
) -> list[dict]:
    """Flag holdings with absolute price change exceeding the threshold.

    Args:
        parsed_holdings: {"TICKER": [{"name", "ticker", "weight", "price", "change"}, ...]}
        threshold: Minimum absolute % change to flag (default 5.0)

    Returns:
        List of alert dicts sorted by absolute change descending:
        [{"etf", "holding", "ticker", "change", "weight", "impact"}, ...]
    """
    alerts = []

    for etf_ticker, holdings in parsed_holdings.items():
        for h in holdings:
            change = abs(h.get("change", 0))
            if change >= threshold:
                weight = h.get("weight", 0)
                alerts.append({
                    "etf": etf_ticker,
                    "holding": h.get("name", "Unknown"),
                    "ticker": h.get("ticker", ""),
                    "change": h.get("change", 0),
                    "weight": weight,
                    "impact": round(weight * h.get("change", 0) / 100, 3),
                    "severity": "high" if change >= threshold * 2 else "medium",
                })

    alerts.sort(key=lambda a: abs(a["change"]), reverse=True)
    logger.info("Price move alerts: %d holdings exceed %.1f%% threshold", len(alerts), threshold)
    return alerts


# ─── Private Allocation Alerts ────────────────────────────────────────────────

def check_private_allocation(
    parsed_holdings: dict[str, list[dict]],
    threshold: float = PRIVATE_ALLOC_THRESHOLD,
    private_tickers: set[str] | None = None,
) -> list[dict]:
    """Check each ETF's private/illiquid allocation against the SEC guideline.

    The SEC 15% illiquidity rule limits open-end funds (including ETFs) from
    holding more than 15% of net assets in illiquid investments.

    Args:
        parsed_holdings: {"TICKER": [holdings...]}
        threshold: Max allowed private allocation % (default 15.0)
        private_tickers: Set of ticker symbols considered private/illiquid

    Returns:
        List of alert dicts for ETFs exceeding the threshold:
        [{"etf", "private_pct", "threshold", "holdings", "severity"}, ...]
    """
    private_tickers = private_tickers or PRIVATE_TICKERS
    alerts = []

    for etf_ticker, holdings in parsed_holdings.items():
        private_holdings = []
        private_pct = 0.0

        for h in holdings:
            hticker = h.get("ticker", "").upper()
            hname = h.get("name", "").lower()
            is_private = (
                hticker in private_tickers
                or "(private)" in hname
                or "spv" in hname
                or "private credit" in hname
            )
            if is_private:
                weight = h.get("weight", 0)
                private_pct += weight
                private_holdings.append({
                    "name": h.get("name", "Unknown"),
                    "ticker": hticker,
                    "weight": weight,
                })

        if private_pct > 0:
            severity = "critical" if private_pct >= threshold else "info"
            alert = {
                "etf": etf_ticker,
                "private_pct": round(private_pct, 2),
                "threshold": threshold,
                "exceeds": private_pct >= threshold,
                "holdings": private_holdings,
                "severity": severity,
            }
            if private_pct >= threshold:
                logger.warning(
                    "%s private allocation %.1f%% EXCEEDS %.1f%% threshold",
                    etf_ticker, private_pct, threshold,
                )
            alerts.append(alert)

    alerts.sort(key=lambda a: a["private_pct"], reverse=True)
    return alerts


# ─── Holdings Diff ────────────────────────────────────────────────────────────

def diff_holdings(
    today: dict[str, list[dict]],
    yesterday: dict[str, list[dict]],
) -> dict[str, dict]:
    """Compare today's vs yesterday's holdings to detect changes.

    Returns per-ETF diff:
    {
        "TICKER": {
            "added": [holdings new today],
            "removed": [holdings gone today],
            "weight_changes": [{"ticker", "name", "old_weight", "new_weight", "delta"}],
        }
    }
    """
    diffs = {}

    all_tickers = set(today.keys()) | set(yesterday.keys())

    for etf in all_tickers:
        today_holdings = {h.get("ticker", h.get("name")): h for h in today.get(etf, [])}
        yesterday_holdings = {h.get("ticker", h.get("name")): h for h in yesterday.get(etf, [])}

        today_ids = set(today_holdings.keys())
        yesterday_ids = set(yesterday_holdings.keys())

        added = [today_holdings[t] for t in today_ids - yesterday_ids]
        removed = [yesterday_holdings[t] for t in yesterday_ids - today_ids]

        weight_changes = []
        for t in today_ids & yesterday_ids:
            old_w = yesterday_holdings[t].get("weight", 0)
            new_w = today_holdings[t].get("weight", 0)
            delta = new_w - old_w
            if abs(delta) >= 0.1:  # Only report changes >= 0.1%
                weight_changes.append({
                    "ticker": t,
                    "name": today_holdings[t].get("name", t),
                    "old_weight": round(old_w, 2),
                    "new_weight": round(new_w, 2),
                    "delta": round(delta, 2),
                })

        weight_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)

        if added or removed or weight_changes:
            diffs[etf] = {
                "added": added,
                "removed": removed,
                "weight_changes": weight_changes,
            }

    if diffs:
        logger.info("Holdings changes detected in %d ETFs", len(diffs))
    else:
        logger.info("No holdings changes detected")

    return diffs


# ─── Save Alerts ──────────────────────────────────────────────────────────────

def save_alerts(
    price_alerts: list[dict],
    allocation_alerts: list[dict],
    diff_results: dict[str, dict],
    date_str: str | None = None,
) -> Path:
    """Save all alerts to a dated JSON file."""
    date_str = date_str or datetime.now().strftime("%Y%m%d")
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)

    out = ALERTS_DIR / f"alerts_{date_str}.json"
    payload = {
        "date": date_str,
        "generated_at": datetime.now().isoformat(),
        "price_moves": price_alerts,
        "private_allocation": allocation_alerts,
        "holdings_changes": diff_results,
        "summary": {
            "price_alerts": len(price_alerts),
            "allocation_warnings": sum(1 for a in allocation_alerts if a["exceeds"]),
            "etfs_with_changes": len(diff_results),
        },
    }
    out.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Alerts saved -> %s", out)
    return out


def load_alerts(date_str: str | None = None) -> dict | None:
    """Load alerts from a dated JSON file. Returns None if not found."""
    date_str = date_str or datetime.now().strftime("%Y%m%d")
    path = ALERTS_DIR / f"alerts_{date_str}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None
