# Private Credit & Equity ETF Monitor

Streamlit dashboard tracking 8 private credit ETFs and 3 private equity ETFs.

## Quick Start (Local)

```bash
cd pc_pe_etf_monitor
pip install -r requirements.txt
streamlit run app.py
```

Opens at `http://localhost:8501`

## Deploy to Streamlit Cloud (Free)

1. Push this folder to a new GitHub repo:
   ```bash
   cd pc_pe_etf_monitor
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/OuterBeachC/pc-pe-etf-monitor.git
   git push -u origin main
   ```

2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select the repo and set main file to `app.py`
5. Click Deploy — live in ~60 seconds

## ETFs Tracked

### Private Credit (8)
| Ticker | Name | AUM |
|--------|------|-----|
| BIZD | VanEck BDC Income ETF | $1.6B |
| PBDC | Putnam BDC Income ETF | $236M |
| VPC | Virtus Private Credit Strategy ETF | $52M |
| PRIV | SPDR SSGA IG Public & Private Credit ETF | $152M |
| PCMM | BondBloxx Private Credit CLO ETF | $46M |
| PCR | Simplify VettaFi Private Credit Strategy ETF | $28M |
| HBDC | Hilton BDC Corporate Bond ETF | $82M |
| PRSD | State Street Short Duration IG Public Private Credit ETF | $32M |

### Private Equity (3)
| Ticker | Name | AUM |
|--------|------|-----|
| XOVR | ERShares Private-Public Crossover ETF | $1.1B |
| AGIX | KraneShares AI & Technology ETF | $181M |
| RONB | Baron First Principles ETF | $67M |

## Connecting Live Data

Replace the `load_etf_data()` function in `app.py` with reads from:
- Your existing SQLite database (`priv_data.db`)
- CSV files from the automated download scripts
- Or a REST API serving the data

The automation scripts in the "Automation" tab are ready to adapt to your existing
SSGA/Invesco download pipeline in `C:\Users\Conor\Documents\Daily PRIV Model\python\PRIV`.
