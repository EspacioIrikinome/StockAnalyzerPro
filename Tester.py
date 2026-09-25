"""
Actualiza los CSVs del S&P 500 con los últimos días cotizados.

- Lee los CSVs existentes en sp500_csv/
- Descarga solo el rango reciente (default 30 días) desde Yahoo
- Merge por fecha: si la fecha ya existe se actualiza, si no se añade
- Guarda los CSVs actualizados preservando todo el histórico

Requiere:
    pip install yfinance pandas
"""

import os
import time
import sys
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# ─── Configuración ────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "sp500_csv")
DAYS_BACK = 30          # días de calendario hacia atrás
MAX_WORKERS = 6
MIN_BARS = 250
# ──────────────────────────────────────────────────────────────────────────


def yahoo_symbol(sym):
    return sym.replace(".", "-")


def load_existing(sym):
    """Lee el CSV existente del ticker. Devuelve DataFrame o None."""
    path = os.path.join(CSV_DIR, f"{sym}.csv")
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path)
        if "datetime" not in df.columns:
            return None
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")
        return df
    except Exception:
        return None


def download_recent(sym):
    """Descarga los últimos DAYS_BACK días. Devuelve DataFrame o None."""
    start = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    try:
        df = yf.download(
            yahoo_symbol(sym),
            start=start,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()

        date_col = None
        for c in df.columns:
            if str(c).lower() in ("date", "datetime"):
                date_col = c
                break
        if date_col is None:
            return None

        out = pd.DataFrame()
        out["datetime"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        out["open"]   = pd.to_numeric(df["Open"],  errors="coerce")
        out["high"]   = pd.to_numeric(df["High"],  errors="coerce")
        out["low"]    = pd.to_numeric(df["Low"],   errors="coerce")
        out["close"]  = pd.to_numeric(df["Close"], errors="coerce")
        out["volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")
        out = out.dropna(subset=["open", "high", "low", "close"])
        return out if len(out) else None
    except Exception:
        return None


def update_one(sym):
    """Actualiza un ticker. Devuelve (sym, status, added, total)."""
    existing = load_existing(sym)
    fresh = download_recent(sym)

    if fresh is None or fresh.empty:
        if existing is not None:
            return (sym, "SKIP_NO_FRESH", 0, len(existing))
        return (sym, "SKIP_NEW_NO_DATA", 0, 0)

    if existing is None:
        # CSV nuevo (no existía): guardar solo lo descargado
        if len(fresh) < MIN_BARS:
            return (sym, f"NEW_TOO_SHORT ({len(fresh)})", len(fresh), len(fresh))
        fresh.to_csv(os.path.join(CSV_DIR, f"{sym}.csv"), index=False)
        return (sym, f"NEW ({len(fresh)})", len(fresh), len(fresh))

    # Merge: concat + dedupe por datetime, quedándose con la fila nueva
    combined = pd.concat([existing, fresh], ignore_index=True)
    combined = combined.drop_duplicates(subset=["datetime"], keep="last")
    combined = combined.sort_values("datetime").reset_index(drop=True)

    added = len(combined) - len(existing)

    if len(combined) < MIN_BARS:
        return (sym, f"MERGE_TOO_SHORT ({len(combined)})", added, len(combined))

    combined.to_csv(os.path.join(CSV_DIR, f"{sym}.csv"), index=False)
    return (sym, f"OK (+{added})" if added else "OK (0)", added, len(combined))


def main():
    if not os.path.isdir(CSV_DIR):
        print(f"ERROR: no existe la carpeta {CSV_DIR}")
        print("Ejecuta primero download_sp500.py para crear los CSVs base.")
        sys.exit(1)

    # Detectar tickers desde los CSVs existentes
    symbols = []
    for f in os.listdir(CSV_DIR):
        if f.lower().endswith(".csv"):
            symbols.append(f[:-4])
    symbols = sorted(set(symbols))

    if not symbols:
        print(f"ERROR: no hay CSVs en {CSV_DIR}")
        sys.exit(1)

    print(f"Carpeta:  {CSV_DIR}")
    print(f"Tickers:  {len(symbols)}")
    print(f"Rango:    últimos {DAYS_BACK} días de calendario")
    print(f"Workers:  {MAX_WORKERS}")
    print("-" * 60)

    ok, skipped, errors = 0, 0, []
    total_added = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(update_one, sym): sym for sym in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            sym, status, added, total = fut.result()
            total_added += added
            if status.startswith("OK") or status.startswith("NEW"):
                ok += 1
                mark = "OK  "
            elif status.startswith("SKIP"):
                skipped += 1
                mark = "SKIP"
            else:
                errors.append((sym, status))
                mark = "ERR "
            print(f"[{i:>3}/{len(symbols)}] {mark} {sym:<8} {status}  (total {total})", flush=True)

    elapsed = time.time() - start
    print("-" * 60)
    print(f"Completado en {elapsed/60:.1f} min  ·  {ok} OK  ·  {skipped} sin datos frescos  ·  {len(errors)} errores")
    print(f"Barras nuevas añadidas: {total_added}")

    if errors:
        print("\nTickers con problemas:")
        for sym, status in errors[:30]:
            print(f"  {sym}: {status}")
        if len(errors) > 30:
            print(f"  ... y {len(errors) - 30} más")

    print(f"\nCSVs actualizados en: {CSV_DIR}")


if __name__ == "__main__":
    main()
