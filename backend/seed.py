"""
Seed the database with ETF metadata, initial holdings, AUM, and price history.

Run once to initialize:
    python -m backend.seed

Safe to re-run -- uses upserts so existing data is updated, not duplicated.
"""

import logging
import sys

from backend.database import Database

logger = logging.getLogger(__name__)

# ─── Month labels -> approximate YYYYMMDD dates for history seeding ────────────
MONTH_DATES = {
    "Sep": "20250901", "Oct": "20251001", "Nov": "20251101",
    "Dec": "20251201", "Jan": "20260101", "Feb": "20260201",
}

# ─── ETF Definitions ─────────────────────────────────────────────────────────
# All data previously hardcoded in app.py's load_etf_data().

ETFS = [
    # ── Private Credit ──
    {
        "ticker": "BIZD", "name": "VanEck BDC Income ETF", "issuer": "VanEck",
        "type": "Passive", "category": "Private Credit", "holdings_type": "BDCs",
        "aum": 1600, "aum_change_3m": 97.82, "expense_ratio": 0.42, "total_expense": 13.33,
        "yield_30d": 9.1, "inception": "2013-02-11", "price": 16.42, "price_change": -2.1,
        "nav": 16.55, "prem_disc": -0.79, "holdings_count": 26,
        "holdings_source": "https://www.vaneck.com/us/en/investments/bdc-income-etf-bizd/holdings/",
        "holdings_format": "CSV Download",
        "top_holdings": [
            {"name": "Ares Capital Corp", "ticker": "ARCC", "weight": 20.3, "price": 21.45, "change": -1.2},
            {"name": "Blue Owl Capital Corp", "ticker": "OBDC", "weight": 8.2, "price": 14.89, "change": -3.1},
            {"name": "Blackstone Secured Lending", "ticker": "BXSL", "weight": 7.5, "price": 30.21, "change": 0.8},
            {"name": "FS KKR Capital Corp", "ticker": "FSK", "weight": 6.8, "price": 19.77, "change": -1.5},
            {"name": "Main Street Capital", "ticker": "MAIN", "weight": 6.1, "price": 52.30, "change": 0.4},
            {"name": "Golub Capital BDC", "ticker": "GBDC", "weight": 5.4, "price": 15.12, "change": -0.9},
            {"name": "Owl Rock Capital Corp", "ticker": "ORCC", "weight": 4.8, "price": 13.88, "change": -2.4},
            {"name": "BlackRock TCP Capital", "ticker": "TCPC", "weight": 4.2, "price": 8.15, "change": -8.7},
            {"name": "Prospect Capital Corp", "ticker": "PSEC", "weight": 3.9, "price": 4.52, "change": -5.2},
            {"name": "Sixth Street Specialty", "ticker": "TPVG", "weight": 3.1, "price": 12.44, "change": -1.8},
        ],
        "aum_history": [{"month": "Sep", "aum": 1350}, {"month": "Oct", "aum": 1420}, {"month": "Nov", "aum": 1480}, {"month": "Dec", "aum": 1510}, {"month": "Jan", "aum": 1550}, {"month": "Feb", "aum": 1600}],
        "price_history": [{"date": "Sep", "price": 17.85}, {"date": "Oct", "price": 17.42}, {"date": "Nov", "price": 17.10}, {"date": "Dec", "price": 16.88}, {"date": "Jan", "price": 16.65}, {"date": "Feb", "price": 16.42}],
    },
    {
        "ticker": "PBDC", "name": "Putnam BDC Income ETF", "issuer": "Putnam / Franklin Templeton",
        "type": "Active", "category": "Private Credit", "holdings_type": "BDCs",
        "aum": 236, "aum_change_3m": 22.66, "expense_ratio": 0.75, "total_expense": 10.86,
        "yield_30d": 9.8, "inception": "2022-09-29", "price": 28.88, "price_change": -3.4,
        "nav": 29.12, "prem_disc": -0.82, "holdings_count": 32,
        "holdings_source": "https://www.franklintempleton.com/.../PBDC",
        "holdings_format": "Website Table / CSV",
        "top_holdings": [
            {"name": "Ares Capital Corp", "ticker": "ARCC", "weight": 14.8, "price": 21.45, "change": -1.2},
            {"name": "Blackstone Secured Lending", "ticker": "BXSL", "weight": 9.1, "price": 30.21, "change": 0.8},
            {"name": "Blue Owl Capital Corp", "ticker": "OBDC", "weight": 7.6, "price": 14.89, "change": -3.1},
            {"name": "Main Street Capital", "ticker": "MAIN", "weight": 6.9, "price": 52.30, "change": 0.4},
            {"name": "FS KKR Capital Corp", "ticker": "FSK", "weight": 5.5, "price": 19.77, "change": -1.5},
            {"name": "Golub Capital BDC", "ticker": "GBDC", "weight": 5.2, "price": 15.12, "change": -0.9},
            {"name": "Owl Rock Capital Corp", "ticker": "ORCC", "weight": 4.4, "price": 13.88, "change": -2.4},
            {"name": "Hercules Capital", "ticker": "HTGC", "weight": 3.8, "price": 19.55, "change": 1.2},
            {"name": "Trinity Capital", "ticker": "TRIN", "weight": 3.2, "price": 14.22, "change": -0.6},
            {"name": "Gladstone Investment", "ticker": "GAIN", "weight": 2.9, "price": 13.78, "change": -1.1},
        ],
        "aum_history": [{"month": "Sep", "aum": 170}, {"month": "Oct", "aum": 185}, {"month": "Nov", "aum": 200}, {"month": "Dec", "aum": 215}, {"month": "Jan", "aum": 225}, {"month": "Feb", "aum": 236}],
        "price_history": [{"date": "Sep", "price": 33.50}, {"date": "Oct", "price": 32.80}, {"date": "Nov", "price": 31.45}, {"date": "Dec", "price": 30.20}, {"date": "Jan", "price": 29.55}, {"date": "Feb", "price": 28.88}],
    },
    {
        "ticker": "VPC", "name": "Virtus Private Credit Strategy ETF", "issuer": "Virtus",
        "type": "Passive", "category": "Private Credit", "holdings_type": "BDCs + CEFs",
        "aum": 52, "aum_change_3m": 3.1, "expense_ratio": 0.75, "total_expense": 9.86,
        "yield_30d": 14.2, "inception": "2019-02-07", "price": 23.15, "price_change": -4.2,
        "nav": 23.48, "prem_disc": -1.40, "holdings_count": 44,
        "holdings_source": "https://www.virtus.com/products/virtus-private-credit-strategy-etf#holdings",
        "holdings_format": "Website Table",
        "top_holdings": [
            {"name": "BlackRock TCP Capital", "ticker": "TCPC", "weight": 2.5, "price": 8.15, "change": -8.7},
            {"name": "Prospect Capital Corp", "ticker": "PSEC", "weight": 2.4, "price": 4.52, "change": -5.2},
            {"name": "Oxford Lane Capital", "ticker": "OXLC", "weight": 2.3, "price": 5.11, "change": -6.1},
            {"name": "Ares Capital Corp", "ticker": "ARCC", "weight": 2.2, "price": 21.45, "change": -1.2},
            {"name": "Eagle Point Credit", "ticker": "ECC", "weight": 2.1, "price": 8.44, "change": -3.8},
            {"name": "Owl Rock Capital Corp", "ticker": "ORCC", "weight": 2.0, "price": 13.88, "change": -2.4},
            {"name": "Blue Owl Capital Corp", "ticker": "OBDC", "weight": 1.9, "price": 14.89, "change": -3.1},
            {"name": "FS KKR Capital Corp", "ticker": "FSK", "weight": 1.8, "price": 19.77, "change": -1.5},
            {"name": "Golub Capital BDC", "ticker": "GBDC", "weight": 1.7, "price": 15.12, "change": -0.9},
            {"name": "PennantPark Floating Rate", "ticker": "PFLT", "weight": 1.6, "price": 10.88, "change": -2.0},
        ],
        "aum_history": [{"month": "Sep", "aum": 40}, {"month": "Oct", "aum": 42}, {"month": "Nov", "aum": 45}, {"month": "Dec", "aum": 48}, {"month": "Jan", "aum": 50}, {"month": "Feb", "aum": 52}],
        "price_history": [{"date": "Sep", "price": 26.20}, {"date": "Oct", "price": 25.80}, {"date": "Nov", "price": 25.10}, {"date": "Dec", "price": 24.55}, {"date": "Jan", "price": 23.85}, {"date": "Feb", "price": 23.15}],
    },
    {
        "ticker": "PRIV", "name": "SPDR SSGA IG Public & Private Credit ETF", "issuer": "State Street / Apollo",
        "type": "Active", "category": "Private Credit", "holdings_type": "IG Bonds + Direct Private Credit",
        "aum": 152, "aum_change_3m": 18.5, "expense_ratio": 0.70, "total_expense": 0.70,
        "yield_30d": 5.8, "inception": "2025-02-26", "price": 25.22, "price_change": 0.3,
        "nav": 25.19, "prem_disc": 0.12, "holdings_count": 180,
        "holdings_source": "https://www.ssga.com/.../priv",
        "holdings_format": "CSV Download (partial -- private sleeve opaque)",
        "top_holdings": [
            {"name": "US Treasury Notes", "ticker": "UST", "weight": 28.5, "price": 99.12, "change": 0.1},
            {"name": "Corp IG Bonds Basket", "ticker": "IG", "weight": 35.2, "price": 100.44, "change": 0.05},
            {"name": "CLO Mezzanine Tranche", "ticker": "CLO-M", "weight": 12.8, "price": 98.50, "change": -0.2},
            {"name": "MBS Pass-Through", "ticker": "MBS", "weight": 8.3, "price": 97.88, "change": -0.1},
            {"name": "Apollo Private Credit I", "ticker": "APC-1", "weight": 3.2, "price": 100.00, "change": 0.0},
            {"name": "Apollo Private Credit II", "ticker": "APC-2", "weight": 2.8, "price": 100.00, "change": 0.0},
            {"name": "Apollo Private Credit III", "ticker": "APC-3", "weight": 2.1, "price": 99.95, "change": 0.0},
            {"name": "ABS Auto Loans", "ticker": "ABS-A", "weight": 1.8, "price": 99.70, "change": 0.02},
            {"name": "Corp IG Financials", "ticker": "IG-F", "weight": 1.5, "price": 100.22, "change": 0.08},
            {"name": "CMBS", "ticker": "CMBS", "weight": 1.2, "price": 98.15, "change": -0.15},
        ],
        "aum_history": [{"month": "Sep", "aum": 85}, {"month": "Oct", "aum": 98}, {"month": "Nov", "aum": 112}, {"month": "Dec", "aum": 128}, {"month": "Jan", "aum": 140}, {"month": "Feb", "aum": 152}],
        "price_history": [{"date": "Sep", "price": 25.00}, {"date": "Oct", "price": 25.05}, {"date": "Nov", "price": 25.10}, {"date": "Dec", "price": 25.12}, {"date": "Jan", "price": 25.18}, {"date": "Feb", "price": 25.22}],
    },
    {
        "ticker": "PCMM", "name": "BondBloxx Private Credit CLO ETF", "issuer": "BondBloxx",
        "type": "Active", "category": "Private Credit", "holdings_type": "Private Credit CLOs",
        "aum": 46, "aum_change_3m": 8.2, "expense_ratio": 0.68, "total_expense": 0.68,
        "yield_30d": 7.4, "inception": "2024-10-01", "price": 49.88, "price_change": -0.5,
        "nav": 49.92, "prem_disc": -0.08, "holdings_count": 38,
        "holdings_source": "https://bondbloxxetf.com/bondbloxx-private-credit-clo-etf/",
        "holdings_format": "CSV Download",
        "top_holdings": [
            {"name": "Churchill MM CLO 2024-I A", "ticker": "CHUR-A", "weight": 4.8, "price": 99.65, "change": -0.1},
            {"name": "Owl Rock CLO VII Ltd A", "ticker": "OWL-A", "weight": 4.2, "price": 99.42, "change": -0.15},
            {"name": "Golub Capital MM CLO 31 A", "ticker": "GLUB-A", "weight": 3.9, "price": 99.55, "change": -0.08},
            {"name": "Ares CLO LVII A", "ticker": "ARES-A", "weight": 3.5, "price": 99.72, "change": 0.05},
            {"name": "Blackstone MM CLO VI B", "ticker": "BX-B", "weight": 3.2, "price": 98.80, "change": -0.22},
        ],
        "aum_history": [{"month": "Sep", "aum": 18}, {"month": "Oct", "aum": 22}, {"month": "Nov", "aum": 28}, {"month": "Dec", "aum": 34}, {"month": "Jan", "aum": 40}, {"month": "Feb", "aum": 46}],
        "price_history": [{"date": "Sep", "price": 50.10}, {"date": "Oct", "price": 50.05}, {"date": "Nov", "price": 50.00}, {"date": "Dec", "price": 49.95}, {"date": "Jan", "price": 49.90}, {"date": "Feb", "price": 49.88}],
    },
    {
        "ticker": "PCR", "name": "Simplify VettaFi Private Credit Strategy ETF", "issuer": "Simplify",
        "type": "Active", "category": "Private Credit", "holdings_type": "BDCs + CEFs + Credit Hedges",
        "aum": 28, "aum_change_3m": 5.4, "expense_ratio": 0.50, "total_expense": 7.85,
        "yield_30d": 11.5, "inception": "2025-09-23", "price": 24.70, "price_change": -2.8,
        "nav": 24.95, "prem_disc": -1.00, "holdings_count": 35,
        "holdings_source": "https://www.simplify.us/etfs/pcr",
        "holdings_format": "Website Table / CSV",
        "top_holdings": [
            {"name": "Ares Capital Corp", "ticker": "ARCC", "weight": 8.5, "price": 21.45, "change": -1.2},
            {"name": "Blue Owl Capital Corp", "ticker": "OBDC", "weight": 6.2, "price": 14.89, "change": -3.1},
            {"name": "Blackstone Secured Lending", "ticker": "BXSL", "weight": 5.8, "price": 30.21, "change": 0.8},
            {"name": "FS KKR Capital Corp", "ticker": "FSK", "weight": 4.9, "price": 19.77, "change": -1.5},
            {"name": "Long Quality TRS", "ticker": "TRS-L", "weight": 4.5, "price": 101.20, "change": 0.3},
        ],
        "aum_history": [{"month": "Sep", "aum": 10}, {"month": "Oct", "aum": 14}, {"month": "Nov", "aum": 18}, {"month": "Dec", "aum": 21}, {"month": "Jan", "aum": 25}, {"month": "Feb", "aum": 28}],
        "price_history": [{"date": "Sep", "price": 25.00}, {"date": "Oct", "price": 25.20}, {"date": "Nov", "price": 25.35}, {"date": "Dec", "price": 25.10}, {"date": "Jan", "price": 24.90}, {"date": "Feb", "price": 24.70}],
    },
    {
        "ticker": "HBDC", "name": "Hilton BDC Corporate Bond ETF", "issuer": "Hilton Capital Mgmt",
        "type": "Passive", "category": "Private Credit", "holdings_type": "BDC Corporate Bonds",
        "aum": 82, "aum_change_3m": 12.8, "expense_ratio": 0.39, "total_expense": 0.39,
        "yield_30d": 6.2, "inception": "2025-06-10", "price": 25.55, "price_change": -0.8,
        "nav": 25.60, "prem_disc": -0.20, "holdings_count": 48,
        "holdings_source": "https://www.hiltoncapitalmanagement.com/hbdc",
        "holdings_format": "CSV Download",
        "top_holdings": [
            {"name": "Ares Capital 3.875% 2026", "ticker": "ARCC-BD", "weight": 5.2, "price": 99.20, "change": -0.1},
            {"name": "Owl Rock 3.75% 2025", "ticker": "ORCC-BD", "weight": 4.8, "price": 99.85, "change": 0.02},
            {"name": "FS KKR 3.125% 2028", "ticker": "FSK-BD", "weight": 4.5, "price": 97.50, "change": -0.15},
            {"name": "Blue Owl 4.0% 2027", "ticker": "OBDC-BD", "weight": 4.2, "price": 98.90, "change": -0.08},
            {"name": "Main Street 3.0% 2026", "ticker": "MAIN-BD", "weight": 3.8, "price": 99.60, "change": 0.05},
        ],
        "aum_history": [{"month": "Sep", "aum": 50}, {"month": "Oct", "aum": 55}, {"month": "Nov", "aum": 62}, {"month": "Dec", "aum": 70}, {"month": "Jan", "aum": 76}, {"month": "Feb", "aum": 82}],
        "price_history": [{"date": "Sep", "price": 25.00}, {"date": "Oct", "price": 25.15}, {"date": "Nov", "price": 25.30}, {"date": "Dec", "price": 25.40}, {"date": "Jan", "price": 25.50}, {"date": "Feb", "price": 25.55}],
    },
    {
        "ticker": "PRSD", "name": "State Street Short Duration IG Public Private Credit ETF", "issuer": "State Street / Apollo",
        "type": "Active", "category": "Private Credit", "holdings_type": "Short Duration IG + Direct Private Credit",
        "aum": 32, "aum_change_3m": 4.8, "expense_ratio": 0.59, "total_expense": 0.59,
        "yield_30d": 5.2, "inception": "2025-09-09", "price": 25.08, "price_change": 0.1,
        "nav": 25.06, "prem_disc": 0.08, "holdings_count": 145,
        "holdings_source": "https://www.ssga.com/.../prsd",
        "holdings_format": "CSV Download (partial)",
        "top_holdings": [
            {"name": "US Treasury Bills", "ticker": "T-BILL", "weight": 32.0, "price": 99.92, "change": 0.01},
            {"name": "Short Duration Corp IG", "ticker": "SD-IG", "weight": 30.5, "price": 99.80, "change": 0.02},
            {"name": "CLO AAA Tranche", "ticker": "CLO-AAA", "weight": 14.2, "price": 99.85, "change": 0.0},
            {"name": "Agency MBS Short", "ticker": "MBS-S", "weight": 8.8, "price": 99.70, "change": -0.02},
            {"name": "Apollo Private Credit I", "ticker": "APC-1", "weight": 3.0, "price": 100.00, "change": 0.0},
        ],
        "aum_history": [{"month": "Sep", "aum": 25}, {"month": "Oct", "aum": 26}, {"month": "Nov", "aum": 28}, {"month": "Dec", "aum": 29}, {"month": "Jan", "aum": 31}, {"month": "Feb", "aum": 32}],
        "price_history": [{"date": "Sep", "price": 25.00}, {"date": "Oct", "price": 25.02}, {"date": "Nov", "price": 25.03}, {"date": "Dec", "price": 25.05}, {"date": "Jan", "price": 25.06}, {"date": "Feb", "price": 25.08}],
    },
    # ── Private Equity ──
    {
        "ticker": "XOVR", "name": "ERShares Private-Public Crossover ETF", "issuer": "ERShares",
        "type": "Active", "category": "Private Equity", "holdings_type": "Public Equities + Private SPVs",
        "aum": 1110, "aum_change_3m": 114.89, "expense_ratio": 0.75, "total_expense": 0.75,
        "yield_30d": 0.0, "inception": "2017-11-07", "price": 17.47, "price_change": -4.2,
        "nav": 17.55, "prem_disc": -0.46, "holdings_count": 35,
        "holdings_source": "https://entrepreneurshares.com/",
        "holdings_format": "Website Table / CSV",
        "top_holdings": [
            {"name": "SPV Exposure to SpaceX", "ticker": "SPACEX", "weight": 10.05, "price": 350.00, "change": 2.5},
            {"name": "NVIDIA Corp", "ticker": "NVDA", "weight": 5.87, "price": 131.50, "change": -3.8},
            {"name": "Meta Platforms", "ticker": "META", "weight": 4.84, "price": 635.20, "change": 1.2},
            {"name": "Palantir Technologies", "ticker": "PLTR", "weight": 4.01, "price": 95.40, "change": -5.6},
            {"name": "Arista Networks", "ticker": "ANET", "weight": 3.74, "price": 88.50, "change": -2.1},
            {"name": "Interactive Brokers", "ticker": "IBKR", "weight": 3.46, "price": 198.30, "change": 1.8},
            {"name": "AppLovin Corp", "ticker": "APP", "weight": 3.30, "price": 340.80, "change": -8.2},
            {"name": "Rocket Lab Corp", "ticker": "RKLB", "weight": 3.22, "price": 22.15, "change": -4.5},
            {"name": "DoorDash", "ticker": "DASH", "weight": 3.10, "price": 195.40, "change": 0.6},
            {"name": "Robinhood Markets", "ticker": "HOOD", "weight": 3.05, "price": 52.80, "change": -2.3},
        ],
        "aum_history": [{"month": "Sep", "aum": 380}, {"month": "Oct", "aum": 580}, {"month": "Nov", "aum": 820}, {"month": "Dec", "aum": 1050}, {"month": "Jan", "aum": 1150}, {"month": "Feb", "aum": 1110}],
        "price_history": [{"date": "Sep", "price": 14.20}, {"date": "Oct", "price": 16.80}, {"date": "Nov", "price": 19.50}, {"date": "Dec", "price": 21.78}, {"date": "Jan", "price": 18.90}, {"date": "Feb", "price": 17.47}],
    },
    {
        "ticker": "AGIX", "name": "KraneShares AI & Technology ETF", "issuer": "KraneShares",
        "type": "Active", "category": "Private Equity", "holdings_type": "AI/Tech + Private (SpaceX, Anthropic)",
        "aum": 181, "aum_change_3m": 99.71, "expense_ratio": 0.99, "total_expense": 0.99,
        "yield_30d": 0.0, "inception": "2024-07-17", "price": 33.44, "price_change": 2.8,
        "nav": 33.52, "prem_disc": -0.24, "holdings_count": 42,
        "holdings_source": "https://kraneshares.com/etf/agix/",
        "holdings_format": "Website Table / CSV Download",
        "top_holdings": [
            {"name": "NVIDIA Corp", "ticker": "NVDA", "weight": 5.35, "price": 131.50, "change": -3.8},
            {"name": "Microsoft Corp", "ticker": "MSFT", "weight": 4.09, "price": 408.50, "change": -1.2},
            {"name": "Meta Platforms", "ticker": "META", "weight": 3.90, "price": 635.20, "change": 1.2},
            {"name": "Alphabet Inc", "ticker": "GOOGL", "weight": 3.36, "price": 176.80, "change": -0.5},
            {"name": "SpaceX (Private)", "ticker": "SPACEX", "weight": 3.35, "price": 350.00, "change": 2.5},
            {"name": "Apple Inc", "ticker": "AAPL", "weight": 3.21, "price": 228.60, "change": 0.3},
            {"name": "Broadcom Inc", "ticker": "AVGO", "weight": 3.16, "price": 198.40, "change": -2.8},
            {"name": "Taiwan Semiconductor", "ticker": "TSM", "weight": 3.01, "price": 185.90, "change": -1.5},
            {"name": "Amazon.com", "ticker": "AMZN", "weight": 2.62, "price": 218.30, "change": 0.8},
            {"name": "Anthropic PBC (Private)", "ticker": "ANTHR", "weight": 2.59, "price": 180.00, "change": 5.0},
        ],
        "aum_history": [{"month": "Sep", "aum": 55}, {"month": "Oct", "aum": 72}, {"month": "Nov", "aum": 95}, {"month": "Dec", "aum": 130}, {"month": "Jan", "aum": 160}, {"month": "Feb", "aum": 181}],
        "price_history": [{"date": "Sep", "price": 28.50}, {"date": "Oct", "price": 31.20}, {"date": "Nov", "price": 35.80}, {"date": "Dec", "price": 40.01}, {"date": "Jan", "price": 36.50}, {"date": "Feb", "price": 33.44}],
    },
    {
        "ticker": "RONB", "name": "Baron First Principles ETF", "issuer": "Baron Capital",
        "type": "Active", "category": "Private Equity", "holdings_type": "Growth + Private (SpaceX 21.5%, xAI 5.4%)",
        "aum": 67, "aum_change_3m": 47.94, "expense_ratio": 1.00, "total_expense": 1.00,
        "yield_30d": 0.0, "inception": "2025-12-01", "price": 23.50, "price_change": -6.2,
        "nav": 23.49, "prem_disc": 0.04, "holdings_count": 28,
        "holdings_source": "https://www.baroncapitalgroup.com/product-detail/baron-first-principles-etf-ronb",
        "holdings_format": "CSV Download (Daily + Quarterly)",
        "top_holdings": [
            {"name": "SpaceX (Private)", "ticker": "SPACEX", "weight": 21.5, "price": 350.00, "change": 2.5},
            {"name": "Tesla Inc", "ticker": "TSLA", "weight": 13.77, "price": 290.40, "change": -7.8},
            {"name": "xAI Corp (Private)", "ticker": "XAI", "weight": 5.4, "price": 200.00, "change": 3.2},
            {"name": "MSCI Inc", "ticker": "MSCI", "weight": 5.03, "price": 540.20, "change": -1.0},
            {"name": "Spotify Technology", "ticker": "SPOT", "weight": 4.56, "price": 585.30, "change": 2.4},
            {"name": "CoStar Group", "ticker": "CSGP", "weight": 4.32, "price": 78.90, "change": -0.8},
            {"name": "Shopify Inc", "ticker": "SHOP", "weight": 3.92, "price": 112.40, "change": -3.5},
            {"name": "Interactive Brokers", "ticker": "IBKR", "weight": 3.65, "price": 198.30, "change": 1.8},
            {"name": "Verisk Analytics", "ticker": "VRSK", "weight": 3.65, "price": 288.50, "change": 0.5},
            {"name": "Arch Capital Group", "ticker": "ACGL", "weight": 3.20, "price": 95.80, "change": -0.3},
        ],
        "aum_history": [{"month": "Sep", "aum": 0}, {"month": "Oct", "aum": 0}, {"month": "Nov", "aum": 0}, {"month": "Dec", "aum": 20}, {"month": "Jan", "aum": 45}, {"month": "Feb", "aum": 67}],
        "price_history": [{"date": "Sep", "price": 0}, {"date": "Oct", "price": 0}, {"date": "Nov", "price": 0}, {"date": "Dec", "price": 25.00}, {"date": "Jan", "price": 24.80}, {"date": "Feb", "price": 23.50}],
    },
]


def seed_database(db: Database | None = None):
    """Populate the database with all ETF data.

    Inserts: metadata, initial holdings, AUM history, price history.
    Safe to re-run -- uses upserts throughout.
    """
    close_db = db is None
    db = db or Database()

    logger.info("Seeding database at %s ...", db.path)

    # 1. ETF metadata
    db.upsert_etf_metadata(ETFS)

    # 2. Initial holdings (as a "seed" date)
    seed_date = "20260301"
    holdings_map = {}
    for etf in ETFS:
        if etf.get("top_holdings"):
            holdings_map[etf["ticker"]] = etf["top_holdings"]
    db.insert_holdings(seed_date, holdings_map)

    # 3. AUM history
    aum_records = []
    for etf in ETFS:
        for pt in etf.get("aum_history", []):
            if pt["aum"] > 0:
                date_str = MONTH_DATES.get(pt["month"], pt["month"])
                aum_records.append((date_str, etf["ticker"], pt["aum"]))
    db.insert_aum_bulk(aum_records)

    # 4. Price history
    price_records = []
    for etf in ETFS:
        for pt in etf.get("price_history", []):
            if pt["price"] > 0:
                date_str = MONTH_DATES.get(pt.get("date", pt.get("month", "")), "")
                if date_str:
                    price_records.append((date_str, etf["ticker"], pt["price"], None, None))
    db.insert_price_bulk(price_records)

    stats = db.table_stats()
    logger.info("Seed complete: %s", stats)

    if close_db:
        db.close()

    return stats


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    stats = seed_database()
    print(f"\nDatabase seeded successfully:")
    for table, count in stats.items():
        print(f"  {table}: {count} rows")


if __name__ == "__main__":
    main()
