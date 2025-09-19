import streamlit as st
import re
import ast
import json
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import List, Tuple
from io import BytesIO

# ================== CONFIG ==================
TICKER_LIST_URL = "https://clientn.com/stocks/Shariaa.html"
LOOKBACK_DAYS = 90
INTERVAL = "1d"
YF_SUFFIX = ""  # Yahoo suffix for EGX tickers; set to "" if your list already includes it
COLS = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

# ================== HELPERS ==================
def get_egx_tickers(url: str) -> List[str]:
    txt = requests.get(url, timeout=20).text.strip()
    lst = []
    try:
        lst = ast.literal_eval(txt)
        if not isinstance(lst, list):
            lst = []
    except Exception:
        m = re.search(r"\[.*\]", txt, re.S)
        if m:
            lst = json.loads(m.group(0).replace("'", '"'))
    if not lst:
        raise RuntimeError("Ticker list parsing failed or returned empty.")
    return sorted({str(t).upper().strip() for t in lst if str(t).strip()})

def to_yf_symbol(t: str) -> str:
    return f"{t}{YF_SUFFIX}" if (YF_SUFFIX and not t.endswith(YF_SUFFIX)) else t

def sanitize_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/?*\[\]]', '_', name)
    return name[:31]

def _slice_ticker_from_download(df: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        lvl0 = df.columns.get_level_values(0)
        if ticker in lvl0:
            return df[ticker]
        try:
            lvl1 = df.columns.get_level_values(1)
            if ticker in lvl1:
                return df.xs(ticker, axis=1, level=1)
        except Exception:
            pass
        return None
    else:
        return df

def normalize_ohlcv(df_t: pd.DataFrame) -> pd.DataFrame:
    df_t = df_t.copy()
    if "Adj Close" not in df_t.columns and "Close" in df_t.columns:
        df_t["Adj Close"] = df_t["Close"]
    keep = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in df_t.columns]
    df_t = df_t[keep].dropna(how="any")
    df_t = df_t.reset_index()
    if df_t.columns[0].lower() != "date":
        df_t = df_t.rename(columns={df_t.columns[0]: "Date"})
    for col in COLS:
        if col not in df_t.columns:
            df_t[col] = pd.NA
    df_t = df_t[COLS]
    if "Date" in df_t.columns:
        df_t = df_t.sort_values("Date").reset_index(drop=True)
    return df_t

def download_and_write_excel(url: str) -> Tuple[BytesIO, pd.DataFrame, List[str], str]:
    original_tickers = get_egx_tickers(url)
    pairs = [(orig, to_yf_symbol(orig)) for orig in original_tickers]
    yf_syms = [y for _, y in pairs]
    if not yf_syms:
        raise RuntimeError("No tickers to download after mapping.")

    data = yf.download(
        yf_syms,
        period=f"{LOOKBACK_DAYS}d",
        interval=INTERVAL,
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    output_xlsx = f"egx_ohlcv_{LOOKBACK_DAYS}d_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        summary_rows = []
        skipped = []

        for orig, yf_sym in pairs:
            df_t = _slice_ticker_from_download(data, yf_sym)
            if df_t is None or df_t.empty:
                skipped.append(yf_sym)
                summary_rows.append({
                    "original_ticker": orig, "yf_symbol": yf_sym, "rows": 0,
                    "first_date": None, "last_date": None, "note": "No data"
                })
                continue

            df_norm = normalize_ohlcv(df_t)
            if df_norm.empty:
                skipped.append(yf_sym)
                summary_rows.append({
                    "original_ticker": orig, "yf_symbol": yf_sym, "rows": 0,
                    "first_date": None, "last_date": None, "note": "Empty after normalization"
                })
                continue

            sheet = sanitize_sheet_name(orig)
            df_norm.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            ws.freeze_panes = "A2"

            summary_rows.append({
                "original_ticker": orig,
                "yf_symbol": yf_sym,
                "rows": len(df_norm),
                "first_date": df_norm["Date"].iloc[0],
                "last_date": df_norm["Date"].iloc[-1],
                "note": ""
            })

        summary_df = pd.DataFrame(summary_rows)
        if not summary_df.empty:
            summary_df = summary_df[["original_ticker", "yf_symbol", "rows", "first_date", "last_date", "note"]]
        summary_df.to_excel(writer, sheet_name="SUMMARY", index=False)
        writer.sheets["SUMMARY"].freeze_panes = "A2"

    excel_buffer.seek(0)
    return excel_buffer, summary_df, skipped, output_xlsx

# ================== STREAMLIT UI ==================
st.set_page_config(page_title="EGX OHLCV Excel Downloader", layout="wide")
st.title("EGX OHLCV Excel Downloader")
st.write("Download last 90 days of daily OHLCV for all stocks in the list, one Excel file with a sheet per stock.")

if st.button("Download and Generate Excel"):
    with st.spinner("Processing..."):
        try:
            excel_buffer, summary_df, skipped, output_xlsx = download_and_write_excel(TICKER_LIST_URL)
            st.success(f"Excel generated: {output_xlsx}")
            st.download_button(
                label="Download Excel File",
                data=excel_buffer,
                file_name=output_xlsx,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.write("### Summary (first 20 rows):")
            st.dataframe(summary_df.head(20))
            if skipped:
                st.warning("No data for these tickers: " + ", ".join(skipped))
        except Exception as e:
            st.error(f"Error: {e}")
else:
    st.info("Press the button above to start.")
