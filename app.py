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
from backend.seed import seed_database

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
@st.cache_data
def load_etf_data():
    """Load ETF universe data from the SQLite database.

    Requires the database to be seeded first:
        python -m backend.seed

    Live holdings from the pipeline (python -m backend) are overlaid
    automatically when available via parsed JSON files.
    """
    db = Database()
    etfs = db.load_etf_data()

    if not etfs:
        # Auto-seed on first run (e.g. fresh Streamlit Cloud deploy)
        seed_database(db)
        etfs = db.load_etf_data()

    db.close()

    if not etfs:
        st.error(
            "No ETF data found. Run `python -m backend.seed` to initialize the database."
        )
        return []

    # Overlay any live-parsed holdings from today's pipeline run
    parsed = load_parsed()
    if parsed:
        for etf in etfs:
            ticker = etf["ticker"]
            if ticker in parsed and parsed[ticker]:
                etf["top_holdings"] = parsed[ticker]
                etf["holdings_count"] = len(parsed[ticker])
                etf["_data_source"] = "live"

    return etfs




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
        if display_etfs:
            pie_data = pd.DataFrame([{"type": e["holdings_type"], "aum": e["aum"], "cat": e["category"]} for e in display_etfs])
            pie_data = pie_data.dropna(subset=["type", "aum"])
            if not pie_data.empty:
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
            else:
                st.caption("No data available for chart.")
        else:
            st.caption("No ETFs match the selected filter.")


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
