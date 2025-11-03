import requests
import pandas as pd
from io import StringIO

nifty_freeze_qty=0
banknifty_freeze_qty=0

NSE_QTY_URLS = [
    "https://archives.nseindia.com/content/fo/qtyfreeze.csv",
    "https://www1.nseindia.com/content/fo/qtyfreeze.csv",
    "https://nsearchives.nseindia.com/content/fo/qtyfreeze.csv",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0 Safari/537.36",
    "Accept": "text/csv,application/vnd.ms-excel,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
}

def _find_header_index(lines):
    """Find the line index that contains the header (contains 'SYMBOL' or 'FREEZE' etc)."""
    for i, line in enumerate(lines):
        up = line.upper()
        if "SYMBOL" in up and ("FREEZE" in up or "QTY" in up or "QUANTITY" in up):
            return i
        if "SYMBOL" in up and i < 5:  # sometimes header just contains SYMBOL
            return i
    return None

def parse_qty_csv_text(text, target_symbol):
    """Return freeze qty for target_symbol or None."""
    # split into lines
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return None

    header_idx = _find_header_index(lines)
    # if header found, pass header index to read_csv
    try:
        if header_idx is not None:
            df = pd.read_csv(StringIO("\n".join(lines)), header=header_idx, engine="python")
        else:
            # try reading with header=0 and if fails fallback to header=None
            try:
                df = pd.read_csv(StringIO("\n".join(lines)), engine="python")
            except Exception:
                df = pd.read_csv(StringIO("\n".join(lines)), header=None, engine="python")
    except Exception as e:
        # parsing failed
        # as a last resort try simple manual parse by splitting commas
        for ln in lines:
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) >= 2 and parts[0].upper() == target_symbol.upper():
                try:
                    return int(parts[1])
                except Exception:
                    return None
        return None

    # normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    upper_cols = [c.upper() for c in df.columns]

    # find symbol column
    symbol_col = None
    freeze_col = None
    for i, uc in enumerate(upper_cols):
        if "SYMBOL" in uc or "CONTRACT" in uc or "INSTRUMENT" in uc:
            symbol_col = df.columns[i]
        if "FREEZE" in uc or "QTY" in uc or "QUANTITY" in uc:
            freeze_col = df.columns[i]

    # fallback heuristics
    if symbol_col is None:
        # try first column
        symbol_col = df.columns[0]
    if freeze_col is None:
        # try second column if exists
        if len(df.columns) >= 2:
            freeze_col = df.columns[1]
        else:
            # search for numeric column
            for c in df.columns:
                if pd.api.types.is_integer_dtype(df[c]) or pd.api.types.is_float_dtype(df[c]) or df[c].str.isnumeric().any():
                    freeze_col = c
                    break

    if symbol_col not in df.columns or freeze_col not in df.columns:
        return None

    # strip and compare
    df[symbol_col] = df[symbol_col].astype(str).str.strip()
    # sometimes symbols may have extra whitespace or suffixes, so use startswith or equal
    matched = df[df[symbol_col].str.upper() == target_symbol.upper()]

    if matched.empty:
        # fallback: try startswith
        matched = df[df[symbol_col].str.upper().str.startswith(target_symbol.upper())]

    if matched.empty:
        return None

    # get first match's freeze col, coerce to int if possible
    val = matched.iloc[0][freeze_col]
    try:
        return int(float(str(val).replace(",", "").strip()))
    except Exception:
        return None

def get_freeze_quantity_from_nse(symbol, debug=False, timeout=10):
    """
    Try multiple NSE URLs and parse the qtyfreeze CSV robustly.
    Returns integer freeze quantity or None.
    Set debug=True to #print diagnostics.
    """
    global nifty_freeze_qty
    global banknifty_freeze_qty
    #print(symbol,nifty_freeze_qty,banknifty_freeze_qty)
    if symbol=='NIFTY' and nifty_freeze_qty !=0:
        return nifty_freeze_qty
    if symbol=='BANKNIFTY' and banknifty_freeze_qty !=0:
        return banknifty_freeze_qty
    
    session = requests.Session()
    session.headers.update(HEADERS)

    # sometimes NSE blocks direct requests; hitting homepage can set cookies
    try:
        session.get("https://www.nseindia.com", timeout=timeout)
    except Exception:
        # ignore; we'll still try the CSV URLs
        pass

    for url in NSE_QTY_URLS:
        try:
            r = session.get(url, timeout=timeout)
            if debug:
                pass
                #print(f"GET {url} -> {r.status_code}")
            if r.status_code != 200:
                continue
            text = r.text
            # quick sanity check
            if len(text) < 50:
                if debug:
                    #print("Short response, skipping.")
                    pass
                continue
            qty = parse_qty_csv_text(text, symbol)
            if qty is not None:
                if symbol=='NIFTY':
                    nifty_freeze_qty=qty
                if symbol=='BANKNIFTY':
                    banknifty_freeze_qty=qty
                return qty
            else:
                if debug:
                    # #print small preview for debugging
                    #print("Parsing succeeded but no matching symbol. preview:")
                    #print("\n".join(text.splitlines()[:10]))
                    pass
        except requests.RequestException as e:
            if debug:
                pass
                #print(f"Request error for {url}: {e}")
            continue

    if debug:
        #print("All attempts failed.")
        pass
    return None





