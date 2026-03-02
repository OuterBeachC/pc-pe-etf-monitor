"""
Private Credit & Equity ETF Monitor
====================================
Standalone Streamlit dashboard tracking private credit and private equity ETFs.
Includes holdings analysis, AUM tracking, price movements, and automation configs.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import logging
import os
from datetime import datetime, date
from pathlib import Path

from backend.database import Database
from backend.parsers import load_parsed
from backend.alerts import load_alerts

# ─── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Private Credit & Equity ETF Monitor",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Source+Serif+4:wght@400;600;700&display=swap');

    /* Dark theme overrides */
    .stApp { background-color: #0c0a09; }
    section[data-testid="stSidebar"] { background-color: #1c1917; border-right: 1px solid #292524; }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1c1917 0%, #0c0a09 100%);
        border: 1px solid #292524;
        border-radius: 8px;
        padding: 12px 16px;
    }
    div[data-testid="stMetric"] label { color: #78716c !important; font-family: 'DM Mono', monospace !important; font-size: 11px !important; text-transform: uppercase; letter-spacing: 0.05em; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { color: #e7e5e4 !important; font-family: 'Source Serif 4', serif !important; }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] { font-family: 'DM Mono', monospace !important; }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; background-color: #1c1917; border-radius: 8px; padding: 4px; border: 1px solid #292524; }
    .stTabs [data-baseweb="tab"] { border-radius: 6px; color: #78716c; font-family: 'DM Mono', monospace; font-size: 13px; padding: 8px 16px; }
    .stTabs [aria-selected="true"] { background-color: #422006 !important; color: #fbbf24 !important; }

    /* Dataframe styling */
    .stDataFrame { border: 1px solid #292524; border-radius: 8px; }

    /* Headers */
    h1, h2, h3 { font-family: 'Source Serif 4', serif !important; color: #e7e5e4 !important; }
    h1 { font-size: 1.6rem !important; }

    /* Badge styling */
    .badge-pc { display: inline-block; background: #422006; color: #fbbf24; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-family: 'DM Mono', monospace; }
    .badge-pe { display: inline-block; background: #2e1065; color: #a78bfa; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-family: 'DM Mono', monospace; }
    .badge-active { display: inline-block; background: #422006; color: #fcd34d; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-family: 'DM Mono', monospace; }
    .badge-passive { display: inline-block; background: #1c1917; color: #78716c; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-family: 'DM Mono', monospace; border: 1px solid #292524; }

    /* Expander */
    .streamlit-expanderHeader { font-family: 'DM Mono', monospace !important; font-size: 13px !important; }

    /* Code blocks in automation tab */
    code { font-family: 'DM Mono', monospace !important; }

    /* Subtitle */
    .subtitle { color: #78716c; font-family: 'DM Mono', monospace; font-size: 12px; margin-top: -10px; }
</style>
""", unsafe_allow_html=True)


# ─── Data ───────────────────────────────────────────────────────────────────
def _merge_live_holdings(etfs: list[dict]) -> list[dict]:
    """Overlay live holdings onto the base ETF data when available.

    Priority:
      1. SQLite database (data/database.db) — most recent holdings
      2. Parsed JSON files (data/parsed/holdings_YYYYMMDD.json) — today's date
      3. Fall back to hardcoded mock data below
    """
    parsed = None

    # Try database first
    try:
        db = Database()
        parsed = db.get_latest_holdings_all()
        db.close()
        if parsed:
            source = "database"
    except Exception:
        parsed = None

    # Fall back to JSON files
    if not parsed:
        parsed = load_parsed()
        if parsed:
            source = "json"

    if not parsed:
        return etfs

    for etf in etfs:
        ticker = etf["ticker"]
        if ticker in parsed and parsed[ticker]:
            etf["top_holdings"] = parsed[ticker]
            etf["holdings_count"] = len(parsed[ticker])
            etf["_data_source"] = source

    return etfs


@st.cache_data
def load_etf_data():
    """Load ETF universe data, overlaying live downloaded holdings when available.

    Base data is hardcoded below. When the backend pipeline has been run
    (python -m backend), parsed holdings files in data/parsed/ are merged in
    automatically, replacing top_holdings with real provider data.
    """
    etfs = [
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
            "holdings_format": "CSV Download (partial — private sleeve opaque)",
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
    return _merge_live_holdings(etfs)


@st.cache_data
def load_price_movers():
    return [
        {"ticker": "TCPC", "name": "BlackRock TCP Capital", "change": -8.7, "reason": "19% NAV write-down in Q4 2025", "category": "PC"},
        {"ticker": "OXLC", "name": "Oxford Lane Capital", "change": -6.1, "reason": "CLO equity tranche losses", "category": "PC"},
        {"ticker": "APP", "name": "AppLovin Corp", "change": -8.2, "reason": "Short seller report; ad-tech revenue quality questions", "category": "PE"},
        {"ticker": "TSLA", "name": "Tesla Inc", "change": -7.8, "reason": "Delivery miss; margin compression fears", "category": "PE"},
        {"ticker": "PLTR", "name": "Palantir Technologies", "change": -5.6, "reason": "Valuation contraction; gov contract concerns", "category": "PE"},
        {"ticker": "PSEC", "name": "Prospect Capital Corp", "change": -5.2, "reason": "Deep NAV discount widening", "category": "PC"},
        {"ticker": "ECC", "name": "Eagle Point Credit", "change": -3.8, "reason": "CLO market stress", "category": "PC"},
        {"ticker": "OBDC", "name": "Blue Owl Capital Corp", "change": -3.1, "reason": "NII compression from rate cuts", "category": "PC"},
        {"ticker": "ANTHR", "name": "Anthropic PBC (Private)", "change": 5.0, "reason": "Revenue $14B annualized; Claude 4.5 launch", "category": "PE"},
        {"ticker": "XAI", "name": "xAI Corp (Private)", "change": 3.2, "reason": "Grok model traction; new funding round", "category": "PE"},
        {"ticker": "SPACEX", "name": "SpaceX (Private SPV)", "change": 2.5, "reason": "IPO speculation; Starlink revenue growth", "category": "PE"},
        {"ticker": "HTGC", "name": "Hercules Capital", "change": 1.2, "reason": "Strong tech lending pipeline", "category": "PC"},
        {"ticker": "BXSL", "name": "Blackstone Secured Lending", "change": 0.8, "reason": "First-lien focus outperforms", "category": "PC"},
    ]


@st.cache_data
def load_automation_configs():
    return [
        {"etf": "BIZD", "method": "CSV Download", "automation": "curl", "frequency": "Daily",
         "url": "https://www.vaneck.com/us/en/investments/bdc-income-etf-bizd/holdings/",
         "notes": "Direct CSV link available. Append date parameter.",
         "script": 'curl -o bizd_holdings_$(date +%Y%m%d).csv \\\n  "https://www.vaneck.com/.../bizd/holdings/?download=csv"'},
        {"etf": "PBDC", "method": "Selenium / Playwright", "automation": "Browser", "frequency": "Daily",
         "url": "https://www.franklintempleton.com/.../PBDC",
         "notes": "Holdings table rendered client-side. Use headless browser. CSV export available after JS loads.",
         "script": 'from playwright.sync_api import sync_playwright\n\nwith sync_playwright() as p:\n    browser = p.chromium.launch()\n    page = browser.new_page()\n    page.goto("https://www.franklintempleton.com/.../PBDC")\n    page.wait_for_selector(\'[data-testid="holdings-table"]\')\n    rows = page.query_selector_all("table tbody tr")\n    for row in rows:\n        print(row.inner_text())'},
        {"etf": "VPC", "method": "BeautifulSoup / Selenium", "automation": "Browser", "frequency": "Daily",
         "url": "https://www.virtus.com/products/virtus-private-credit-strategy-etf#holdings",
         "notes": "Paginated table. No direct CSV. Scrape with requests + BS4 or headless browser.",
         "script": 'import requests\nfrom bs4 import BeautifulSoup\n\nresp = requests.get("https://www.virtus.com/.../virtus-private-credit-strategy-etf")\nsoup = BeautifulSoup(resp.text, "html.parser")\ntable = soup.find("table", {"class": "holdings-table"})'},
        {"etf": "PRIV", "method": "CSV Download (Partial)", "automation": "curl + EDGAR", "frequency": "Daily",
         "url": "https://www.ssga.com/.../priv",
         "notes": "Public holdings via CSV. Private sleeve (~8%) opaque. Monitor N-PORT filings on SEC EDGAR quarterly.",
         "script": 'curl -o priv_holdings_$(date +%Y%m%d).xlsx \\\n  "https://www.ssga.com/.../holdings-daily-us-en-priv.xlsx"\n\n# SEC EDGAR quarterly\ncurl "https://efts.sec.gov/LATEST/search-index?q=PRIV&forms=NPORT-P"'},
        {"etf": "PCMM", "method": "CSV Download", "automation": "curl", "frequency": "Daily",
         "url": "https://bondbloxxetf.com/bondbloxx-private-credit-clo-etf/",
         "notes": "Holdings CSV with CUSIP, coupon, maturity.",
         "script": 'curl -o pcmm_holdings_$(date +%Y%m%d).csv \\\n  "https://bondbloxxetf.com/fund-data/pcmm-holdings.csv"'},
        {"etf": "PCR", "method": "Website Table + CSV", "automation": "curl / Selenium", "frequency": "Daily",
         "url": "https://www.simplify.us/etfs/pcr",
         "notes": "Simplify provides holdings with CSV export. Credit hedge positions (TRS) require manual monitoring.",
         "script": 'curl -o pcr_holdings_$(date +%Y%m%d).csv \\\n  "https://www.simplify.us/etfs/pcr/holdings?format=csv"'},
        {"etf": "HBDC", "method": "CSV Download", "automation": "curl", "frequency": "Daily",
         "url": "https://www.hiltoncapitalmanagement.com/hbdc",
         "notes": "BDC bond holdings with CUSIP, coupon, maturity, rating.",
         "script": 'curl -o hbdc_holdings_$(date +%Y%m%d).csv \\\n  "https://www.hiltoncapitalmanagement.com/hbdc/holdings/download"'},
        {"etf": "PRSD", "method": "CSV Download (Partial)", "automation": "curl + EDGAR", "frequency": "Daily",
         "url": "https://www.ssga.com/.../prsd",
         "notes": "Same as PRIV. Public sleeve transparent, private sleeve opaque.",
         "script": 'curl -o prsd_holdings_$(date +%Y%m%d).xlsx \\\n  "https://www.ssga.com/.../holdings-daily-us-en-prsd.xlsx"'},
        {"etf": "XOVR", "method": "Selenium + SEC EDGAR", "automation": "Browser + EDGAR", "frequency": "Daily / Quarterly",
         "url": "https://entrepreneurshares.com/",
         "notes": "SpaceX SPV fair-valued daily under SEC Rule 2a-5 but not independently verifiable. Morningstar flagged stale marks. Monitor N-PORT filings.",
         "script": 'from playwright.sync_api import sync_playwright\n\nwith sync_playwright() as p:\n    browser = p.chromium.launch()\n    page = browser.new_page()\n    page.goto("https://entrepreneurshares.com/")\n    page.wait_for_selector(\'[class*="holdings"]\')\n    data = page.evaluate("() => [...document.querySelectorAll(\'table tr\')].map(r => [...r.cells].map(c => c.textContent))")\n\n# SEC EDGAR\ncurl "https://efts.sec.gov/LATEST/search-index?q=ERShares+XOVR&forms=NPORT-P"'},
        {"etf": "AGIX", "method": "CSV Download + Website", "automation": "curl / Selenium", "frequency": "Daily",
         "url": "https://kraneshares.com/etf/agix/",
         "notes": "KraneShares provides holdings table on fund page. Private positions (SpaceX, Anthropic) held via SPVs. CSV export available.",
         "script": 'curl -o agix_holdings_$(date +%Y%m%d).csv \\\n  "https://kraneshares.com/etf/agix/holdings/?format=csv"'},
        {"etf": "RONB", "method": "CSV Download (Direct)", "automation": "curl", "frequency": "Daily",
         "url": "https://www.baroncapitalgroup.com/product-detail/baron-first-principles-etf-ronb",
         "notes": "Daily + quarterly holdings. SpaceX 21.5%, xAI 5.4% — ~27% private. Musk-linked = ~40%. Monitor 15% illiquidity rule.",
         "script": 'curl -o ronb_holdings_$(date +%Y%m%d).csv \\\n  "https://www.baroncapitalgroup.com/.../ronb/holdings/download"\n\n# Track private allocation\nimport csv\nprivate_tickers = ["SPACEX", "XAI"]\nwith open(f"ronb_holdings.csv") as f:\n    total = sum(float(r["weight"]) for r in csv.DictReader(f) if r["ticker"] in private_tickers)\n    print(f"Private allocation: {total:.1f}%")\n    if total > 15: print("WARNING: Exceeds 15% illiquidity threshold")'},
    ]


# ─── Helpers ────────────────────────────────────────────────────────────────
def fmt_aum(n):
    if n >= 1000:
        return f"${n/1000:.1f}B"
    return f"${n:.0f}M"

def color_change(val):
    if val > 0:
        return f"🟢 +{val:.1f}%"
    elif val < 0:
        return f"🔴 {val:.1f}%"
    return f"⚪ {val:.1f}%"

PC_COLOR = "#b45309"
PE_COLOR = "#7c3aed"
PLOTLY_TEMPLATE = "plotly_dark"
CHART_BG = "rgba(0,0,0,0)"
GRID_COLOR = "rgba(120,113,108,0.2)"


# ─── Load Data ──────────────────────────────────────────────────────────────
etfs = load_etf_data()
movers = load_price_movers()
auto_configs = load_automation_configs()

df_etfs = pd.DataFrame([{k: v for k, v in e.items() if k not in ("top_holdings", "aum_history", "price_history")} for e in etfs])
pc_etfs = [e for e in etfs if e["category"] == "Private Credit"]
pe_etfs = [e for e in etfs if e["category"] == "Private Equity"]
total_aum = sum(e["aum"] for e in etfs)
total_flows = sum(e["aum_change_3m"] for e in etfs)


# ─── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ◆ ETF Monitor")
    st.markdown(f'<p class="subtitle">{len(etfs)} ETFs · {fmt_aum(total_aum)} AUM</p>', unsafe_allow_html=True)

    st.divider()

    cat_filter = st.radio("Category", ["All", "Private Credit", "Private Equity"], horizontal=True)

    st.divider()

    # Quick stats
    pc_aum = sum(e["aum"] for e in pc_etfs)
    pe_aum = sum(e["aum"] for e in pe_etfs)
    st.metric("Private Credit AUM", fmt_aum(pc_aum), f"{len(pc_etfs)} ETFs")
    st.metric("Private Equity AUM", fmt_aum(pe_aum), f"{len(pe_etfs)} ETFs")

    st.divider()

    # Show data source status
    live_etfs = [e for e in etfs if e.get("_data_source")]
    if live_etfs:
        source = live_etfs[0].get("_data_source", "live")
        label = "DB" if source == "database" else "JSON"
        st.success(f"Live data ({label}): {len(live_etfs)}/{len(etfs)} ETFs")
    else:
        st.caption("Data is illustrative. Run `python -m backend` to fetch live holdings.")


# ─── Header ─────────────────────────────────────────────────────────────────
st.markdown("# Private Credit & Equity ETF Monitor")
st.markdown(f'<p class="subtitle">Tracking {len(pc_etfs)} private credit and {len(pe_etfs)} private equity ETFs · {fmt_aum(total_aum)} combined AUM · Updated Mar 2026</p>', unsafe_allow_html=True)


# ─── Tabs ───────────────────────────────────────────────────────────────────
tab_overview, tab_holdings, tab_aum, tab_movers, tab_auto = st.tabs(
    ["Overview", "Holdings", "AUM & Flows", "Price Movers", "Automation"]
)

# ═══ OVERVIEW TAB ═══════════════════════════════════════════════════════════
with tab_overview:
    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total AUM", fmt_aum(total_aum), f"{len(etfs)} ETFs tracked")
    c2.metric("3M Net Flows", f"+${total_flows:.0f}M", "8.2%")
    yield_etfs = [e for e in etfs if e["yield_30d"] > 0]
    avg_yield = sum(e["yield_30d"] for e in yield_etfs) / len(yield_etfs) if yield_etfs else 0
    c3.metric("Avg PC Yield", f"{avg_yield:.1f}%", "Private Credit only")
    c4.metric("Max PE Private Exp.", "~27%", "RONB (SpaceX 21.5%)")

    st.markdown("")

    # Filter data
    display_etfs = etfs if cat_filter == "All" else [e for e in etfs if e["category"] == cat_filter]

    # Main table
    table_data = []
    for e in display_etfs:
        cat_badge = "PC" if e["category"] == "Private Credit" else "PE"
        table_data.append({
            "Ticker": e["ticker"],
            "Name": e["name"],
            "Cat": cat_badge,
            "Type": e["type"],
            "AUM": e["aum"],
            "3M Flows ($M)": e["aum_change_3m"],
            "Yield": e["yield_30d"] if e["yield_30d"] > 0 else None,
            "Exp. Ratio": e["expense_ratio"],
            "Price Δ%": e["price_change"],
            "Prem/Disc%": e["prem_disc"],
        })

    df_table = pd.DataFrame(table_data)

    st.dataframe(
        df_table.style.format({
            "AUM": lambda x: fmt_aum(x),
            "3M Flows ($M)": lambda x: f"+${x:.1f}M" if x >= 0 else f"-${abs(x):.1f}M",
            "Yield": lambda x: f"{x:.1f}%" if x and x > 0 else "—",
            "Exp. Ratio": "{:.2f}%",
            "Price Δ%": lambda x: f"+{x:.1f}%" if x >= 0 else f"{x:.1f}%",
            "Prem/Disc%": lambda x: f"+{x:.2f}%" if x >= 0 else f"{x:.2f}%",
        }).applymap(
            lambda x: "color: #4ade80" if isinstance(x, (int, float)) and x > 0 else ("color: #f87171" if isinstance(x, (int, float)) and x < 0 else ""),
            subset=["Price Δ%", "3M Flows ($M)"]
        ),
        use_container_width=True,
        hide_index=True,
        height=min(len(display_etfs) * 38 + 38, 500),
    )

    # Charts row
    col_aum_chart, col_pie = st.columns(2)

    with col_aum_chart:
        st.markdown("#### AUM by ETF")
        sorted_etfs = sorted(display_etfs, key=lambda x: x["aum"])
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            y=[e["ticker"] for e in sorted_etfs],
            x=[e["aum"] for e in sorted_etfs],
            orientation="h",
            marker_color=[PE_COLOR if e["category"] == "Private Equity" else PC_COLOR for e in sorted_etfs],
            text=[fmt_aum(e["aum"]) for e in sorted_etfs],
            textposition="outside",
            textfont=dict(size=11),
        ))
        fig_bar.update_layout(
            template=PLOTLY_TEMPLATE, paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            height=350, margin=dict(l=10, r=60, t=10, b=10),
            xaxis=dict(showgrid=True, gridcolor=GRID_COLOR),
            yaxis=dict(tickfont=dict(family="DM Mono, monospace", size=11)),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_pie:
        st.markdown("#### Exposure Type Distribution")
        pie_data = pd.DataFrame([{"type": e["holdings_type"], "aum": e["aum"], "cat": e["category"]} for e in display_etfs])
        fig_pie = px.pie(
            pie_data, values="aum", names="type",
            color_discrete_sequence=["#b45309", "#a16207", "#78716c", "#57534e", "#44403c", "#7c3aed", "#6d28d9", "#5b21b6", "#d97706", "#fbbf24", "#92400e"],
            hole=0.45,
        )
        fig_pie.update_layout(
            template=PLOTLY_TEMPLATE, paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            height=350, margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(font=dict(size=10)),
        )
        st.plotly_chart(fig_pie, use_container_width=True)


# ═══ HOLDINGS TAB ═══════════════════════════════════════════════════════════
with tab_holdings:
    # ETF selector
    ticker_options = [f"{'◆ ' if e['category'] == 'Private Equity' else ''}{e['ticker']} — {e['name']}" for e in etfs]
    selected_label = st.selectbox("Select ETF", ticker_options, index=0)
    selected_ticker = selected_label.split(" — ")[0].replace("◆ ", "")
    etf = next(e for e in etfs if e["ticker"] == selected_ticker)

    # Header cards
    cat_class = "badge-pe" if etf["category"] == "Private Equity" else "badge-pc"
    type_class = "badge-active" if etf["type"] == "Active" else "badge-passive"
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">
        <span class="{cat_class}">{etf['category']}</span>
        <span class="{type_class}">{etf['type']}</span>
        <span class="badge-passive">{etf['holdings_type']}</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"**{etf['name']}** · {etf['issuer']} · Inception: {etf['inception']} · {etf['holdings_count']} holdings")

    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Price", f"${etf['price']:.2f}", f"{etf['price_change']:+.1f}%")
    mc2.metric("NAV", f"${etf['nav']:.2f}", f"{etf['prem_disc']:+.2f}% P/D")
    mc3.metric("AUM", fmt_aum(etf['aum']), f"+${etf['aum_change_3m']:.1f}M 3M")
    mc4.metric("Yield", f"{etf['yield_30d']:.1f}%" if etf["yield_30d"] > 0 else "—")
    mc5.metric("Expense", f"{etf['expense_ratio']:.2f}%", f"Total: {etf['total_expense']:.2f}%")

    st.markdown("#### Top Holdings")
    holdings_df = pd.DataFrame(etf["top_holdings"])
    holdings_df["impact"] = (holdings_df["weight"] / 100 * holdings_df["change"]).round(3)
    holdings_df.index = range(1, len(holdings_df) + 1)
    holdings_df.columns = ["Holding", "Ticker", "Weight%", "Price", "Change%", "Impact%"]

    st.dataframe(
        holdings_df.style.format({
            "Weight%": "{:.1f}%",
            "Price": "${:.2f}",
            "Change%": lambda x: f"+{x:.1f}%" if x >= 0 else f"{x:.1f}%",
            "Impact%": lambda x: f"+{x:.3f}%" if x >= 0 else f"{x:.3f}%",
        }).applymap(
            lambda x: "color: #4ade80" if isinstance(x, (int, float)) and x > 0 else ("color: #f87171" if isinstance(x, (int, float)) and x < 0 else ""),
            subset=["Change%", "Impact%"]
        ).bar(subset=["Weight%"], color="#44403c", vmin=0),
        use_container_width=True,
    )

    st.caption(f"Source: [{etf['holdings_source']}]({etf['holdings_source']}) · Format: {etf['holdings_format']}")

    # Holdings overlap
    st.markdown("#### Cross-ETF Holdings Overlap")
    overlap_tickers = ["ARCC", "OBDC", "BXSL", "FSK", "MAIN", "SPACEX", "NVDA", "META", "IBKR", "TSLA"]
    overlap_data = []
    for t in overlap_tickers:
        found_in = [e["ticker"] for e in etfs if any(h["ticker"] == t for h in e["top_holdings"])]
        overlap_data.append({"Holding": t, "Held By": ", ".join(found_in), "Count": len(found_in)})
    overlap_df = pd.DataFrame(overlap_data).sort_values("Count", ascending=False)
    st.dataframe(overlap_df, use_container_width=True, hide_index=True)


# ═══ AUM & FLOWS TAB ═══════════════════════════════════════════════════════
with tab_aum:
    col_aum_trend, col_price_trend = st.columns(2)

    with col_aum_trend:
        st.markdown("#### AUM Growth — 6 Month Trend")
        aum_records = []
        for e in etfs:
            for pt in e["aum_history"]:
                if pt["aum"] > 0:
                    aum_records.append({"month": pt["month"], "ticker": e["ticker"], "aum": pt["aum"], "category": e["category"]})
        aum_df = pd.DataFrame(aum_records)

        fig_aum = px.area(
            aum_df, x="month", y="aum", color="ticker",
            color_discrete_map={e["ticker"]: PE_COLOR if e["category"] == "Private Equity" else PC_COLOR for e in etfs},
        )
        fig_aum.update_layout(
            template=PLOTLY_TEMPLATE, paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            height=350, margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(gridcolor=GRID_COLOR), yaxis=dict(gridcolor=GRID_COLOR, title="AUM ($M)"),
            legend=dict(font=dict(size=10)),
        )
        st.plotly_chart(fig_aum, use_container_width=True)

    with col_price_trend:
        st.markdown("#### Price — 6 Month (Indexed to 100)")
        price_records = []
        for e in etfs:
            base = next((p["price"] for p in e["price_history"] if p["price"] > 0), None)
            if not base:
                continue
            for pt in e["price_history"]:
                if pt["price"] > 0:
                    price_records.append({
                        "date": pt["date"], "ticker": e["ticker"],
                        "indexed": round(pt["price"] / base * 100, 2), "category": e["category"],
                    })
        price_df = pd.DataFrame(price_records)

        fig_price = px.line(
            price_df, x="date", y="indexed", color="ticker",
            color_discrete_map={e["ticker"]: PE_COLOR if e["category"] == "Private Equity" else PC_COLOR for e in etfs},
        )
        fig_price.update_layout(
            template=PLOTLY_TEMPLATE, paper_bgcolor=CHART_BG, plot_bgcolor=CHART_BG,
            height=350, margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(gridcolor=GRID_COLOR), yaxis=dict(gridcolor=GRID_COLOR, title="Indexed Price"),
            legend=dict(font=dict(size=10)),
        )
        st.plotly_chart(fig_price, use_container_width=True)

    # Flow detail
    st.markdown("#### Net Flows Detail (3 Month)")
    sorted_flows = sorted(etfs, key=lambda x: x["aum_change_3m"], reverse=True)
    flow_cols = st.columns(4)
    for i, e in enumerate(sorted_flows[:8]):
        with flow_cols[i % 4]:
            growth_rate = (e["aum_change_3m"] / max(e["aum"] - e["aum_change_3m"], 1)) * 100
            st.metric(
                e["ticker"],
                f"+${e['aum_change_3m']:.1f}M",
                f"{growth_rate:.1f}% growth",
            )


# ═══ PRICE MOVERS TAB ══════════════════════════════════════════════════════
with tab_movers:
    st.markdown("#### Major Holdings Price Movements")
    st.caption("Significant moves across underlying BDC, CEF, CLO, and private equity positions held by tracked ETFs")

    sorted_movers = sorted(movers, key=lambda x: x["change"])
    for m in sorted_movers:
        cat_badge = "PE" if m["category"] == "PE" else "PC"
        cat_color = PE_COLOR if m["category"] == "PE" else PC_COLOR
        change_color = "#4ade80" if m["change"] >= 0 else "#f87171"
        arrow = "▲" if m["change"] >= 0 else "▼"

        st.markdown(f"""
        <div style="display:flex; align-items:center; gap:12px; padding:10px 14px; background:rgba(28,25,23,0.7); border:1px solid #292524; border-radius:8px; margin-bottom:6px;">
            <span style="color:{change_color}; font-size:18px; width:24px; text-align:center;">{arrow}</span>
            <span style="color:{cat_color}; font-family:'DM Mono',monospace; font-size:12px; min-width:60px; font-weight:600;">{m['ticker']}</span>
            <div style="flex:1;">
                <div style="color:#e7e5e4; font-size:14px;">{m['name']}</div>
                <div style="color:#78716c; font-size:12px;">{m['reason']}</div>
            </div>
            <span style="color:{change_color}; font-family:'DM Mono',monospace; font-size:18px; font-weight:600; min-width:70px; text-align:right;">
                {'+' if m['change'] >= 0 else ''}{m['change']}%
            </span>
            <span class="badge-{'pe' if m['category'] == 'PE' else 'pc'}">{cat_badge}</span>
        </div>
        """, unsafe_allow_html=True)

    # Risk alerts
    st.markdown("")
    st.markdown("#### ⚠️ Risk Alerts")

    with st.expander("🔴 Private Credit Alerts", expanded=True):
        st.warning("**TCPC NAV Write-down:** BlackRock TCP Capital reported a 19% NAV write-down in Q4 2025. Held by BIZD (4.2%), VPC (2.5%). Monitor for contagion.")
        st.warning("**PRIV Opacity:** Apollo-sourced private credit sleeve (~8%) remains opaque. Limited info on underlying asset structure. Quarterly N-PORT filings provide only basic detail.")
        st.info("**Rate Cut Impact:** Fed rate cuts compressing BDC net interest income. Floating-rate loan portfolios seeing reduced spreads. NII per share declining across sector.")

    with st.expander("🟣 Private Equity Alerts", expanded=True):
        st.error("**RONB Concentration Risk:** SpaceX 21.5% + xAI 5.4% = 26.9% private allocation. Combined with Tesla (13.8%), Musk-linked positions = ~40% of fund. Well above SEC 15% illiquidity guideline.")
        st.warning("**SpaceX Valuation Staleness:** Morningstar flagged XOVR's SpaceX marks hadn't moved since Dec 10, 2024. Fair valuation under Rule 2a-5 can lag. Monitor across all SPV holders (XOVR, AGIX, RONB).")
        st.warning("**XOVR AUM Reversal:** After quadrupling to $1.15B on SpaceX IPO speculation, XOVR shed ~$1B in one month. Rapid outflows concentrate illiquid private positions as public holdings redeemed first.")


# ═══ AUTOMATION TAB ═════════════════════════════════════════════════════════
with tab_auto:
    st.markdown("#### Holdings Data Retrieval — Automation Configs")
    st.caption("Scripts and methods for automated daily retrieval of holdings data from each ETF provider.")

    for config in auto_configs:
        etf_data = next((e for e in etfs if e["ticker"] == config["etf"]), None)
        cat = etf_data["category"] if etf_data else "Unknown"
        cat_icon = "🟣" if cat == "Private Equity" else "🟠"

        with st.expander(f"{cat_icon} **{config['etf']}** — {config['method']} ({config['frequency']})"):
            st.markdown(f"**Source:** [{config['url']}]({config['url']})")
            st.markdown(f"**Automation:** `{config['automation']}` · **Frequency:** {config['frequency']}")
            st.markdown(f"**Notes:** {config['notes']}")
            st.code(config["script"], language="bash" if config["script"].startswith("curl") else "python")

    # Master pipeline
    st.markdown("---")
    st.markdown("#### Master Pipeline — Daily Orchestration")
    st.code("""#!/bin/bash
# Private Credit & Equity ETF Holdings Daily Pipeline
# Run via cron: 0 18 * * 1-5 /opt/scripts/pc_pe_etf_pipeline.sh

DATE=$(date +%Y%m%d)
OUT_DIR="/data/holdings/$DATE"
mkdir -p "$OUT_DIR"

echo "[$DATE] Starting pipeline..."

# ═══ PRIVATE CREDIT ═══
for ETF in BIZD PCMM HBDC PRSD; do
  curl -sS -o "$OUT_DIR/${ETF}_holdings.csv" \\
    "https://provider-api.example.com/${ETF}/holdings?date=$DATE&format=csv"
done

# Browser-based scrapes
python3 /opt/scripts/scrape_holdings.py --etfs PBDC,VPC,PCR --output "$OUT_DIR"

# SSGA partial
curl -sS -o "$OUT_DIR/PRIV_public.xlsx" \\
  "https://www.ssga.com/.../holdings-daily-us-en-priv.xlsx"

# ═══ PRIVATE EQUITY ═══
python3 /opt/scripts/scrape_holdings.py --etfs XOVR --output "$OUT_DIR"
curl -sS -o "$OUT_DIR/AGIX_holdings.csv" \\
  "https://kraneshares.com/etf/agix/holdings/?format=csv"
curl -sS -o "$OUT_DIR/RONB_holdings.csv" \\
  "https://www.baroncapitalgroup.com/.../ronb/holdings/download"

# ═══ POST-PROCESSING ═══
python3 /opt/scripts/diff_holdings.py --today "$OUT_DIR" --yesterday "/data/holdings/$(date -d yesterday +%Y%m%d)"
python3 /opt/scripts/check_private_allocation.py --dir "$OUT_DIR" --etfs XOVR,AGIX,RONB --threshold 15.0
python3 /opt/scripts/generate_alerts.py --dir "$OUT_DIR" --threshold-price-move 5.0

echo "[$DATE] Pipeline complete."
""", language="bash")
