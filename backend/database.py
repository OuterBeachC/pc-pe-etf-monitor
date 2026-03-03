"""
SQLite database layer for persisting all ETF holdings and pipeline data.

Tables:
  etf_metadata   -- Static ETF info (ticker, name, issuer, category, etc.)
  holdings       -- Daily holdings snapshots per ETF
  aum_history    -- AUM over time
  price_history  -- ETF price over time
  alerts         -- Generated alerts (price moves, allocation, diffs)
  pipeline_runs  -- Log of each pipeline execution

Usage:
  from backend.database import Database

  db = Database()              # uses default data/database.db
  db.upsert_etf_metadata(etfs)
  db.insert_holdings("20260302", parsed)
  holdings = db.get_latest_holdings("BIZD")
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from backend.config import BASE_DIR

logger = logging.getLogger(__name__)

DB_PATH = BASE_DIR / "database.db"


class Database:
    """SQLite wrapper for ETF monitor data."""

    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path else DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._init_schema()

    # ─── Connection ───────────────────────────────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ─── Schema ───────────────────────────────────────────────────────────────

    def _init_schema(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS etf_metadata (
                ticker        TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                issuer        TEXT,
                type          TEXT,
                category      TEXT,
                holdings_type TEXT,
                aum           REAL,
                aum_change_3m REAL,
                expense_ratio REAL,
                total_expense REAL,
                yield_30d     REAL,
                inception     TEXT,
                price         REAL,
                price_change  REAL,
                nav           REAL,
                prem_disc     REAL,
                holdings_count INTEGER,
                holdings_source TEXT,
                holdings_format TEXT,
                updated_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS holdings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL,
                etf_ticker      TEXT NOT NULL,
                holding_name    TEXT NOT NULL,
                holding_ticker  TEXT,
                weight          REAL,
                price           REAL,
                change          REAL,
                market_value    REAL,
                coupon          REAL,
                maturity        TEXT,
                rating          TEXT,
                FOREIGN KEY (etf_ticker) REFERENCES etf_metadata(ticker)
            );
            CREATE INDEX IF NOT EXISTS idx_holdings_date_etf
                ON holdings(date, etf_ticker);
            CREATE INDEX IF NOT EXISTS idx_holdings_etf
                ON holdings(etf_ticker);

            CREATE TABLE IF NOT EXISTS aum_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                date       TEXT NOT NULL,
                etf_ticker TEXT NOT NULL,
                aum        REAL NOT NULL,
                FOREIGN KEY (etf_ticker) REFERENCES etf_metadata(ticker),
                UNIQUE(date, etf_ticker)
            );
            CREATE INDEX IF NOT EXISTS idx_aum_date
                ON aum_history(date);

            CREATE TABLE IF NOT EXISTS price_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                date       TEXT NOT NULL,
                etf_ticker TEXT NOT NULL,
                price      REAL NOT NULL,
                nav        REAL,
                prem_disc  REAL,
                FOREIGN KEY (etf_ticker) REFERENCES etf_metadata(ticker),
                UNIQUE(date, etf_ticker)
            );
            CREATE INDEX IF NOT EXISTS idx_price_date
                ON price_history(date);

            CREATE TABLE IF NOT EXISTS alerts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                date         TEXT NOT NULL,
                alert_type   TEXT NOT NULL,
                etf_ticker   TEXT,
                severity     TEXT,
                detail       TEXT,
                created_at   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_date
                ON alerts(date);

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                date          TEXT NOT NULL,
                started_at    TEXT NOT NULL,
                completed_at  TEXT,
                status        TEXT NOT NULL DEFAULT 'running',
                etfs_downloaded INTEGER DEFAULT 0,
                etfs_parsed     INTEGER DEFAULT 0,
                alerts_generated INTEGER DEFAULT 0,
                errors        TEXT,
                summary       TEXT
            );
        """)
        self.conn.commit()
        logger.debug("Database schema initialized: %s", self.path)

    # ─── ETF Metadata ─────────────────────────────────────────────────────────

    def upsert_etf_metadata(self, etfs: list[dict]):
        """Insert or update ETF metadata."""
        now = datetime.now().isoformat()
        rows = []
        for e in etfs:
            rows.append((
                e["ticker"], e["name"], e.get("issuer"), e.get("type"),
                e.get("category"), e.get("holdings_type"),
                e.get("aum"), e.get("aum_change_3m"),
                e.get("expense_ratio"), e.get("total_expense"),
                e.get("yield_30d"), e.get("inception"),
                e.get("price"), e.get("price_change"),
                e.get("nav"), e.get("prem_disc"),
                e.get("holdings_count"),
                e.get("holdings_source"), e.get("holdings_format"),
                now,
            ))
        self.conn.executemany("""
            INSERT INTO etf_metadata
                (ticker, name, issuer, type, category, holdings_type,
                 aum, aum_change_3m, expense_ratio, total_expense,
                 yield_30d, inception, price, price_change, nav, prem_disc,
                 holdings_count, holdings_source, holdings_format, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                name=excluded.name, issuer=excluded.issuer,
                type=excluded.type, category=excluded.category,
                holdings_type=excluded.holdings_type,
                aum=excluded.aum, aum_change_3m=excluded.aum_change_3m,
                expense_ratio=excluded.expense_ratio,
                total_expense=excluded.total_expense,
                yield_30d=excluded.yield_30d, inception=excluded.inception,
                price=excluded.price, price_change=excluded.price_change,
                nav=excluded.nav, prem_disc=excluded.prem_disc,
                holdings_count=excluded.holdings_count,
                holdings_source=excluded.holdings_source,
                holdings_format=excluded.holdings_format,
                updated_at=excluded.updated_at
        """, rows)
        self.conn.commit()
        logger.info("Upserted metadata for %d ETFs", len(rows))

    def get_etf_metadata(self, ticker: str | None = None) -> list[dict]:
        """Get ETF metadata. If ticker is None, returns all."""
        if ticker:
            rows = self.conn.execute(
                "SELECT * FROM etf_metadata WHERE ticker = ?", (ticker,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM etf_metadata").fetchall()
        return [dict(r) for r in rows]

    # ─── Holdings ─────────────────────────────────────────────────────────────

    def insert_holdings(self, date_str: str, parsed: dict[str, list[dict]]):
        """Insert parsed holdings for a given date.

        Replaces any existing holdings for the same date+ETF to allow re-runs.
        """
        for etf_ticker, holdings in parsed.items():
            # Remove existing data for this date+ETF (allows re-runs)
            self.conn.execute(
                "DELETE FROM holdings WHERE date = ? AND etf_ticker = ?",
                (date_str, etf_ticker),
            )
            rows = []
            for h in holdings:
                rows.append((
                    date_str, etf_ticker,
                    h.get("name", "Unknown"),
                    h.get("ticker", ""),
                    h.get("weight", 0),
                    h.get("price", 0),
                    h.get("change", 0),
                    h.get("market_value"),
                    h.get("coupon"),
                    h.get("maturity"),
                    h.get("rating"),
                ))
            self.conn.executemany("""
                INSERT INTO holdings
                    (date, etf_ticker, holding_name, holding_ticker,
                     weight, price, change, market_value, coupon, maturity, rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

        self.conn.commit()
        total = sum(len(h) for h in parsed.values())
        logger.info("Inserted %d holdings across %d ETFs for %s",
                     total, len(parsed), date_str)

    def get_holdings(self, etf_ticker: str, date_str: str | None = None) -> list[dict]:
        """Get holdings for an ETF on a specific date (or latest available)."""
        if date_str:
            rows = self.conn.execute(
                "SELECT * FROM holdings WHERE etf_ticker = ? AND date = ? ORDER BY weight DESC",
                (etf_ticker, date_str),
            ).fetchall()
        else:
            # Get the most recent date for this ETF
            latest = self.conn.execute(
                "SELECT MAX(date) as d FROM holdings WHERE etf_ticker = ?",
                (etf_ticker,),
            ).fetchone()
            if not latest or not latest["d"]:
                return []
            rows = self.conn.execute(
                "SELECT * FROM holdings WHERE etf_ticker = ? AND date = ? ORDER BY weight DESC",
                (etf_ticker, latest["d"]),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_holdings_all(self) -> dict[str, list[dict]]:
        """Get the most recent holdings for all ETFs.

        Returns: {"BIZD": [holdings...], "RONB": [holdings...], ...}
        """
        latest_date = self.conn.execute(
            "SELECT MAX(date) as d FROM holdings"
        ).fetchone()
        if not latest_date or not latest_date["d"]:
            return {}

        date_str = latest_date["d"]
        rows = self.conn.execute(
            "SELECT * FROM holdings WHERE date = ? ORDER BY etf_ticker, weight DESC",
            (date_str,),
        ).fetchall()

        result: dict[str, list[dict]] = {}
        for r in rows:
            rd = dict(r)
            etf = rd["etf_ticker"]
            if etf not in result:
                result[etf] = []
            result[etf].append({
                "name": rd["holding_name"],
                "ticker": rd["holding_ticker"],
                "weight": rd["weight"],
                "price": rd["price"],
                "change": rd["change"],
                **({"market_value": rd["market_value"]} if rd["market_value"] else {}),
                **({"coupon": rd["coupon"]} if rd["coupon"] else {}),
                **({"maturity": rd["maturity"]} if rd["maturity"] else {}),
                **({"rating": rd["rating"]} if rd["rating"] else {}),
            })

        logger.info("Loaded holdings for %d ETFs from %s", len(result), date_str)
        return result

    def get_holdings_dates(self, etf_ticker: str | None = None) -> list[str]:
        """Get all dates that have holdings data, optionally filtered by ETF."""
        if etf_ticker:
            rows = self.conn.execute(
                "SELECT DISTINCT date FROM holdings WHERE etf_ticker = ? ORDER BY date DESC",
                (etf_ticker,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT DISTINCT date FROM holdings ORDER BY date DESC"
            ).fetchall()
        return [r["date"] for r in rows]

    # ─── AUM History ──────────────────────────────────────────────────────────

    def insert_aum(self, date_str: str, etf_ticker: str, aum: float):
        """Insert or update AUM for a given date and ETF."""
        self.conn.execute("""
            INSERT INTO aum_history (date, etf_ticker, aum)
            VALUES (?, ?, ?)
            ON CONFLICT(date, etf_ticker) DO UPDATE SET aum=excluded.aum
        """, (date_str, etf_ticker, aum))
        self.conn.commit()

    def insert_aum_bulk(self, records: list[tuple[str, str, float]]):
        """Bulk insert AUM records: [(date, etf_ticker, aum), ...]"""
        self.conn.executemany("""
            INSERT INTO aum_history (date, etf_ticker, aum)
            VALUES (?, ?, ?)
            ON CONFLICT(date, etf_ticker) DO UPDATE SET aum=excluded.aum
        """, records)
        self.conn.commit()
        logger.info("Inserted %d AUM records", len(records))

    def get_aum_history(self, etf_ticker: str, limit: int = 180) -> list[dict]:
        """Get AUM history for an ETF, most recent first."""
        rows = self.conn.execute(
            "SELECT * FROM aum_history WHERE etf_ticker = ? ORDER BY date DESC LIMIT ?",
            (etf_ticker, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Price History ────────────────────────────────────────────────────────

    def insert_price(self, date_str: str, etf_ticker: str, price: float,
                     nav: float | None = None, prem_disc: float | None = None):
        """Insert or update price for a given date and ETF."""
        self.conn.execute("""
            INSERT INTO price_history (date, etf_ticker, price, nav, prem_disc)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, etf_ticker) DO UPDATE SET
                price=excluded.price, nav=excluded.nav, prem_disc=excluded.prem_disc
        """, (date_str, etf_ticker, price, nav, prem_disc))
        self.conn.commit()

    def insert_price_bulk(self, records: list[tuple[str, str, float, float | None, float | None]]):
        """Bulk insert price records: [(date, etf_ticker, price, nav, prem_disc), ...]"""
        self.conn.executemany("""
            INSERT INTO price_history (date, etf_ticker, price, nav, prem_disc)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, etf_ticker) DO UPDATE SET
                price=excluded.price, nav=excluded.nav, prem_disc=excluded.prem_disc
        """, records)
        self.conn.commit()
        logger.info("Inserted %d price records", len(records))

    def get_price_history(self, etf_ticker: str, limit: int = 180) -> list[dict]:
        """Get price history for an ETF, most recent first."""
        rows = self.conn.execute(
            "SELECT * FROM price_history WHERE etf_ticker = ? ORDER BY date DESC LIMIT ?",
            (etf_ticker, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Alerts ───────────────────────────────────────────────────────────────

    def insert_alerts(self, date_str: str, price_alerts: list[dict],
                      allocation_alerts: list[dict], diff_results: dict[str, dict]):
        """Persist all generated alerts for a pipeline run."""
        now = datetime.now().isoformat()

        # Remove existing alerts for this date (allows re-runs)
        self.conn.execute("DELETE FROM alerts WHERE date = ?", (date_str,))

        rows = []
        for a in price_alerts:
            rows.append((
                date_str, "price_move", a.get("etf"), a.get("severity", "medium"),
                json.dumps(a, default=str), now,
            ))
        for a in allocation_alerts:
            rows.append((
                date_str, "private_allocation", a.get("etf"), a.get("severity", "info"),
                json.dumps(a, default=str), now,
            ))
        for etf, diff in diff_results.items():
            rows.append((
                date_str, "holdings_change", etf, "info",
                json.dumps(diff, default=str), now,
            ))

        if rows:
            self.conn.executemany("""
                INSERT INTO alerts (date, alert_type, etf_ticker, severity, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
            self.conn.commit()
            logger.info("Inserted %d alerts for %s", len(rows), date_str)

    def get_alerts(self, date_str: str | None = None,
                   alert_type: str | None = None) -> list[dict]:
        """Get alerts, optionally filtered by date and/or type."""
        query = "SELECT * FROM alerts WHERE 1=1"
        params: list = []
        if date_str:
            query += " AND date = ?"
            params.append(date_str)
        if alert_type:
            query += " AND alert_type = ?"
            params.append(alert_type)
        query += " ORDER BY date DESC, id DESC"

        rows = self.conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("detail"):
                d["detail"] = json.loads(d["detail"])
            result.append(d)
        return result

    # ─── Pipeline Runs ────────────────────────────────────────────────────────

    def start_pipeline_run(self, date_str: str) -> int:
        """Record the start of a pipeline run. Returns the run ID."""
        cur = self.conn.execute("""
            INSERT INTO pipeline_runs (date, started_at, status)
            VALUES (?, ?, 'running')
        """, (date_str, datetime.now().isoformat()))
        self.conn.commit()
        return cur.lastrowid

    def complete_pipeline_run(self, run_id: int, result: dict):
        """Update a pipeline run with final results."""
        self.conn.execute("""
            UPDATE pipeline_runs SET
                completed_at = ?,
                status = ?,
                etfs_downloaded = ?,
                etfs_parsed = ?,
                alerts_generated = ?,
                errors = ?,
                summary = ?
            WHERE id = ?
        """, (
            datetime.now().isoformat(),
            "error" if result.get("errors") else "success",
            len(result.get("downloads", [])),
            len(result.get("parsed", {})),
            result.get("alerts", {}).get("price_moves", 0)
            + result.get("alerts", {}).get("allocation_warnings", 0),
            json.dumps(result.get("errors", []), default=str) if result.get("errors") else None,
            json.dumps(result, default=str),
            run_id,
        ))
        self.conn.commit()

    def get_pipeline_runs(self, limit: int = 30) -> list[dict]:
        """Get recent pipeline runs."""
        rows = self.conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Full ETF Load (for app.py) ──────────────────────────────────────────

    def load_etf_data(self) -> list[dict]:
        """Build the full ETF list that app.py expects.

        Combines etf_metadata, latest holdings, aum_history, and price_history
        into the same dict structure that was previously hardcoded in app.py.
        Returns an empty list if the database has not been seeded.
        """
        meta = self.get_etf_metadata()
        if not meta:
            return []

        holdings_map = self.get_latest_holdings_all()

        etfs = []
        for m in meta:
            ticker = m["ticker"]

            # Top holdings from latest pipeline run
            top_holdings = holdings_map.get(ticker, [])

            # AUM history (oldest first for charts)
            aum_rows = self.get_aum_history(ticker, limit=12)
            aum_history = [
                {"month": r["date"], "aum": r["aum"]}
                for r in reversed(aum_rows)
            ]

            # Price history (oldest first for charts)
            price_rows = self.get_price_history(ticker, limit=12)
            price_history = [
                {"date": r["date"], "price": r["price"]}
                for r in reversed(price_rows)
            ]

            etfs.append({
                "ticker": ticker,
                "name": m["name"],
                "issuer": m["issuer"],
                "type": m["type"],
                "category": m["category"],
                "holdings_type": m["holdings_type"],
                "aum": m["aum"] or 0,
                "aum_change_3m": m["aum_change_3m"] or 0,
                "expense_ratio": m["expense_ratio"] or 0,
                "total_expense": m["total_expense"] or 0,
                "yield_30d": m["yield_30d"] or 0,
                "inception": m["inception"] or "",
                "price": m["price"] or 0,
                "price_change": m["price_change"] or 0,
                "nav": m["nav"] or 0,
                "prem_disc": m["prem_disc"] or 0,
                "holdings_count": m["holdings_count"] or len(top_holdings),
                "holdings_source": m["holdings_source"] or "",
                "holdings_format": m["holdings_format"] or "",
                "top_holdings": top_holdings,
                "aum_history": aum_history,
                "price_history": price_history,
                "_data_source": "database",
            })

        return etfs

    # ─── Utility ──────────────────────────────────────────────────────────────

    def table_stats(self) -> dict[str, int]:
        """Get row counts for all tables."""
        tables = ["etf_metadata", "holdings", "aum_history",
                  "price_history", "alerts", "pipeline_runs"]
        stats = {}
        for t in tables:
            row = self.conn.execute(f"SELECT COUNT(*) as n FROM {t}").fetchone()
            stats[t] = row["n"]
        return stats
