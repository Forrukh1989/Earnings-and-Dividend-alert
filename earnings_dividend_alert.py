#!/usr/bin/env python3
"""
Earnings & Dividend Alert  v1
================================================================
A completely free Telegram alerter that warns you a fixed number of
TRADING DAYS ahead of each watchlist stock's earnings date and
ex-dividend date. Default lead time is 3 trading days.

WHAT IT DOES IN ONE PASS
  1. Pulls upcoming EARNINGS dates for your watchlist
        - Finnhub free tier  (better dates + BMO/AMC timing + EPS estimate)
        - falls back to yfinance if no Finnhub key is set
  2. Pulls upcoming EX-DIVIDEND dates for your watchlist  (yfinance, no key)
  3. Counts the real NYSE trading days until each event
  4. Alerts only when an event is within the lead window (default 1..3
     trading days out), so a missed run still catches the event late
  5. De-duplicates against a local history file, so each event alerts once
  6. Pushes a clean briefing to Telegram  (prints to console if no token)

WHY "TRADING DAYS" AND NOT CALENDAR DAYS
  3 calendar days before a Monday event is Friday, but 3 *trading* days
  before is the previous Wednesday. Weekends and US market holidays are
  excluded. The 2026-2027 NYSE holiday calendar is hardcoded below.

COST
  Zero. Finnhub earnings calendar is free (60 calls/min). yfinance needs
  no key. The only optional registration is a free Finnhub key.

DEPENDENCIES
  pip install requests yfinance

USAGE
  python earnings_dividend_alert.py            # one real pass
  python earnings_dividend_alert.py --selftest # offline check of the
                                               # trading-day math, no network
================================================================
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, date, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # Python < 3.9; time display degrades gracefully

import concurrent.futures as _cf
import requests


# ==================================================================
# 1. CONFIG  -  edit this block
# ==================================================================

# ── Watchlist ─────────────────────────────────────────────────────
# Use dotted class shares (BRK.B) — converted to BRK-B for yfinance.
# Exchange-suffixed tickers (BNS.TO, MC.PA, RMS.PA) keep their suffix
# so yfinance can identify the right exchange. Finnhub will not return
# those on its free tier; the hybrid logic below falls back to yfinance.
#
# NOTE: LVMH was listed as LVMH.PA — correct yfinance ticker is MC.PA.
#       Hermès was listed as HRMS.PA — correct yfinance ticker is RMS.PA.
#
# Removed (no longer trading):
#   ATVI  — acquired by Microsoft, Jan 2023
#   CERN  — acquired by Oracle, Jun 2022
#   CTXS  — taken private, Sep 2022
WATCHLIST = [
    # ── Mega-cap tech ──────────────────────────────────────────────
    "AAPL", "MSFT", "NVDA", "AMD", "AMZN", "GOOGL", "META", "TSLA",

    # ── Semiconductor ──────────────────────────────────────────────
    "AVGO", "ASML", "TXN", "QCOM", "AMAT", "LRCX", "KLAC", "MU",
    "MCHP", "SWKS", "ADI", "NXPI", "STM", "SNPS", "CDNS", "IPGP", "MPWR",

    # ── Software / Cloud / SaaS ────────────────────────────────────
    "ORCL", "CRM", "ADBE", "INTU", "NOW", "ADSK", "TEAM", "VEEV",
    "SNOW", "PANW", "FTNT", "CRWD", "PLTR",

    # ── Internet / E-commerce / Fintech ────────────────────────────
    "NFLX", "BKNG", "EXPE", "SHOP", "MELI", "EBAY", "PYPL",
    "JD", "NTES", "BABA", "HOOD", "SFM", "CAR",

    # ── Enterprise tech / Hardware / Services ──────────────────────
    "CSCO", "ACN", "MSI", "TMUS", "AKAM", "CHKP", "ADP",
    "NTAP", "CTAS", "TTWO", "EA", "DELL", "GLW", "EQIX", "CHWY",

    # ── Financial services ─────────────────────────────────────────
    "JPM", "GS", "MS", "WFC", "BLK", "CME", "SCHW", "V", "MA", "SPGI",

    # ── Industrial / Defence / Transport ──────────────────────────
    "BA", "GD", "NOC", "LMT", "RTX", "DE", "CAT", "ETN",
    "MMM", "HON", "FDX", "UPS", "UNP", "NSC", "CSX", "TDG",

    # ── Energy ────────────────────────────────────────────────────
    "EOG", "CVX", "XOM",

    # ── Healthcare / Biotech ──────────────────────────────────────
    "UNH", "ELV", "HUM", "CI", "ABBV", "ABT", "TMO", "ILMN",
    "ISRG", "VRTX", "BIIB", "REGN", "LLY", "IDXX",

    # ── Consumer / Retail ─────────────────────────────────────────
    "HD", "WMT", "COST", "MCD", "YUM", "SBUX", "NKE", "DIS",
    "LULU", "ORLY", "AZO", "PG", "KO", "PEP", "PM", "MO", "MDLZ", "EL",

    # ── Indices / Data / Diversified ──────────────────────────────
    "MSCI", "BRK.B",  # BRK.B -> yfinance BRK-B

    # ── Other US ──────────────────────────────────────────────────
    "TSM",    # TSMC ADR (NYSE)
    "NSRGY",  # Nestlé OTC ADR (Pink Sheets)
    "WU",     # Western Union
    "AMTD",   # AMTD Digital ADR
    "XYZ",    # verify this ticker is correct for your use case
    "FLY",    # verify this ticker is correct for your use case

    # ── Non-US (exchange suffix kept for yfinance) ─────────────────
    "BNS.TO",  # Bank of Nova Scotia — Toronto Stock Exchange
    "MC.PA",   # LVMH — Paris (Euronext); user listed as LVMH.PA
    "RMS.PA",  # Hermès — Paris (Euronext); user listed as HRMS.PA
]

# Lead time. Alert when an event is exactly this many trading days out.
ALERT_TRADING_DAYS = 3

# Catch-up safety net. If a scheduled run is skipped, still alert when the
# event is anywhere from 1 up to ALERT_TRADING_DAYS sessions away. De-dup
# guarantees you still only get one alert per event.
MIN_TRADING_DAYS = 1

ENABLE_EARNINGS  = True
ENABLE_DIVIDENDS = True

# Parallel yfinance fetching — 138 tickers sequential = 9-14 min.
# 10 workers brings that down to ~30 seconds typical.
YFINANCE_WORKERS = 10   # parallel threads
YFINANCE_TIMEOUT = 90   # hard ceiling (seconds) for the full calendar batch


def _env(key, default=""):
    return os.environ.get(key, default).strip()

# --- Telegram delivery (free). Leave blank to just print to console. ----------
# Reuse the same bot from your news feed, or make a second bot via @BotFather
# if you want the alert stream kept separate. Same chat id works for both.
TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN", "")    # e.g. "7894561230:AAH...."
TELEGRAM_CHAT_ID   = _env("TELEGRAM_CHAT_ID", "")      # e.g. "5888283036"

# --- Free API key (optional). Empty = earnings come from yfinance instead. ----
FINNHUB_KEY = _env("FINNHUB_KEY", "")     # https://finnhub.io/register

# De-dup history lives next to this script.
SEEN_FILE = Path(__file__).resolve().parent / "earnings_div_seen.json"


# ==================================================================
# 2. TIME + TRADING-DAY HELPERS
# ==================================================================

ET    = ZoneInfo("America/New_York") if ZoneInfo else None
DHAKA = ZoneInfo("Asia/Dhaka")       if ZoneInfo else None

# NYSE market holidays (full closures). Half-days still count as trading days.
NYSE_HOLIDAYS = {
    # 2026
    date(2026, 1, 1),   date(2026, 1, 19),  date(2026, 2, 16),
    date(2026, 4, 3),   date(2026, 5, 25),  date(2026, 6, 19),
    date(2026, 7, 3),   date(2026, 9, 7),   date(2026, 11, 26),
    date(2026, 12, 25),
    # 2027
    date(2027, 1, 1),   date(2027, 1, 18),  date(2027, 2, 15),
    date(2027, 3, 26),  date(2027, 5, 31),  date(2027, 6, 18),
    date(2027, 7, 5),   date(2027, 9, 6),   date(2027, 11, 25),
    date(2027, 12, 24),
}


def now_et_date():
    """Today's date in US Eastern time (events are US-market events)."""
    if ET:
        return datetime.now(ET).date()
    return datetime.now(timezone.utc).date()


def is_trading_day(d):
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def trading_days_until(target, ref=None):
    """
    NYSE sessions strictly after `ref` up to and including `target`.
      target == ref            -> 0
      target is the next session -> 1
      target in the past        -> -1
    """
    ref = ref or now_et_date()
    if target < ref:
        return -1
    cnt, cur = 0, ref
    while cur < target:
        cur += timedelta(days=1)
        if is_trading_day(cur):
            cnt += 1
    return cnt


def fmt_now():
    u = datetime.now(timezone.utc)
    if ET and DHAKA:
        return (f"{u.astimezone(ET):%a %d %b %Y, %I:%M %p} ET"
                f"  /  {u.astimezone(DHAKA):%I:%M %p} Dhaka")
    return f"{u:%a %d %b %Y, %H:%M} UTC"


def fmt_event_date(d):
    return f"{d:%a %d %b %Y}"


def yf_symbol(sym):
    """
    Convert a watchlist symbol to the format yfinance expects.

    Class shares use a dash:  BRK.B  ->  BRK-B   (single-char suffix)
    Exchange suffixes keep the dot:  BNS.TO, MC.PA, RMS.PA  (multi-char suffix)
    Plain symbols pass through unchanged.
    """
    if "." in sym:
        base, suffix = sym.rsplit(".", 1)
        if len(suffix) == 1:          # A, B, C ... = class share
            return f"{base}-{suffix}"
    return sym                        # exchange suffix or no dot: unchanged


HOUR_LABEL = {"bmo": "before open", "amc": "after close",
              "dmh": "during hours", "": ""}


# ---- parallel yfinance calendar fetch ----------------------------

def _yf_fetch_calendars(symbols):
    """Fetch yfinance .calendar for every symbol in one parallel pass.

    Returns {sym: calendar_dict}.  Tickers that time out or fail return {}.
    A hard YFINANCE_TIMEOUT ceiling means no single slow or delisted ticker
    can block the whole run.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("[yfinance not installed - dividend/earnings fallback skipped]")
        return {s: {} for s in symbols}

    def _one(sym):
        return sym, yf.Ticker(yf_symbol(sym)).calendar or {}

    result = {}
    executor = _cf.ThreadPoolExecutor(max_workers=YFINANCE_WORKERS)
    try:
        fmap = {executor.submit(_one, sym): sym for sym in symbols}
        try:
            for fut in _cf.as_completed(fmap, timeout=YFINANCE_TIMEOUT):
                sym = fmap[fut]
                try:
                    s, cal = fut.result()
                    result[s] = cal
                except Exception as ex:
                    print(f"[yfinance: {sym} error: {ex}]")
                    result[sym] = {}
        except _cf.TimeoutError:
            # Cut off any tickers still pending after the total timeout
            for fut, sym in fmap.items():
                if sym not in result:
                    print(f"[yfinance: {sym} timed out — skipped]")
                    result[sym] = {}
    finally:
        executor.shutdown(wait=False)   # don't block on any still-hanging threads
    return result


# ==================================================================
# 3. EARNINGS
# ==================================================================

def fetch_earnings(cals=None):
    """Return {symbol: {'date': date, 'hour': str, 'eps': float|None}}.
    Pass pre-fetched cals dict to avoid duplicate yfinance calls."""
    if FINNHUB_KEY:
        found = _earnings_finnhub()
        # Supplement: Finnhub's free tier is US-focused and won't return
        # results for BNS.TO, MC.PA, RMS.PA or any US ticker it missed.
        # Fall back to yfinance for those so no symbol is silently skipped.
        missed = [s for s in WATCHLIST if s not in found]
        if missed:
            found.update(_earnings_yfinance(missed, cals))
        return found
    return _earnings_yfinance(WATCHLIST, cals)


def _earnings_finnhub():
    ref = now_et_date()
    frm = ref.isoformat()
    to  = (ref + timedelta(days=45)).isoformat()
    watch = set(WATCHLIST)
    out = {}
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": frm, "to": to, "token": FINNHUB_KEY},
            timeout=25,
        )
        rows = r.json().get("earningsCalendar", []) or []
    except Exception as ex:
        print(f"[earnings: Finnhub fetch failed: {ex}]")
        return _earnings_yfinance(WATCHLIST)

    for row in rows:
        sym = (row.get("symbol") or "").upper()
        if sym not in watch:
            continue
        try:
            d = datetime.strptime(row.get("date"), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if d < ref:
            continue
        # keep the earliest upcoming date per symbol
        if sym not in out or d < out[sym]["date"]:
            out[sym] = {"date": d,
                        "hour": (row.get("hour") or "").lower(),
                        "eps": row.get("epsEstimate")}
    return out


def _earnings_yfinance(symbols, cals=None):
    """Extract earnings dates from pre-fetched calendars (no extra network calls)."""
    if cals is None:
        cals = _yf_fetch_calendars(symbols)
    ref = now_et_date()
    out = {}
    for sym in symbols:
        cal = cals.get(sym) or {}
        ed = cal.get("Earnings Date")
        if isinstance(ed, (list, tuple)):
            ed = ed[0] if ed else None
        d = _as_date(ed)
        if d and d >= ref:
            out[sym] = {"date": d, "hour": "", "eps": None}
    return out


# ==================================================================
# 4. DIVIDENDS  (yfinance, no key)
# ==================================================================

def fetch_dividends(cals=None):
    """Return {symbol: {'exdate': date, 'amount': float|None, 'pay': date|None}}.
    Pass pre-fetched cals dict to avoid duplicate yfinance calls."""
    if cals is None:
        cals = _yf_fetch_calendars(WATCHLIST)
    ref = now_et_date()
    out = {}

    # Pass 1: extract ex-div dates from the already-fetched calendars (no network).
    for sym in WATCHLIST:
        cal = cals.get(sym) or {}
        exdate = _as_date(cal.get("Ex-Dividend Date"))
        pay    = _as_date(cal.get("Dividend Date"))
        if exdate and exdate >= ref:
            out[sym] = {"exdate": exdate, "amount": None, "pay": pay}

    # Pass 2: fetch .info only for the small subset with an upcoming ex-div
    # (usually 0-5 tickers) to get the dividend amount.
    if out:
        try:
            import yfinance as yf
        except ImportError:
            return out

        def _get_amount(sym):
            try:
                info = yf.Ticker(yf_symbol(sym)).info or {}
                return sym, (info.get("lastDividendValue") or info.get("dividendRate"))
            except Exception:
                return sym, None

        executor = _cf.ThreadPoolExecutor(max_workers=min(len(out), 5))
        try:
            fmap = {executor.submit(_get_amount, sym): sym for sym in out}
            try:
                for fut in _cf.as_completed(fmap, timeout=30):
                    s, amount = fut.result()
                    if s in out:
                        out[s]["amount"] = amount
            except _cf.TimeoutError:
                pass
        finally:
            executor.shutdown(wait=False)

    return out


# ---- small date coercers -----------------------------------------

def _as_date(v):
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(v[:len(fmt)], fmt).date()
            except ValueError:
                continue
    return None


def _ts_to_date(v):
    """Yahoo exDividendDate is a Unix timestamp in seconds."""
    try:
        if v is None:
            return None
        return datetime.fromtimestamp(int(v), tz=timezone.utc).date()
    except (ValueError, OverflowError, OSError, TypeError):
        return None


# ==================================================================
# 5. DE-DUP HISTORY
# ==================================================================

def load_seen():
    try:
        return json.loads(SEEN_FILE.read_text())
    except Exception:
        return {}


def save_seen(seen):
    # prune keys whose event date is in the past so the file stays small
    ref = now_et_date().isoformat()
    pruned = {k: v for k, v in seen.items() if k.split("|")[-1] >= ref}
    try:
        SEEN_FILE.write_text(json.dumps(pruned, indent=0))
    except Exception as ex:
        print(f"[seen: save failed: {ex}]")


# ==================================================================
# 6. TELEGRAM DELIVERY
# ==================================================================

def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print(text)
        print("\n[Telegram not configured - printed above instead]")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunk, length = [], 0
    for block in text.split("\n\n"):
        if length + len(block) > 3500 and chunk:
            _tg_post(url, "\n\n".join(chunk))
            chunk, length = [], 0
        chunk.append(block)
        length += len(block) + 2
    if chunk:
        _tg_post(url, "\n\n".join(chunk))


def _tg_post(url, body):
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": body,
                                 "disable_web_page_preview": True}, timeout=20)
    except Exception as ex:
        print(f"[Telegram send failed: {ex}]")


# ==================================================================
# 7. BRIEFING + MAIN
# ==================================================================

def _window_ok(n):
    return MIN_TRADING_DAYS <= n <= ALERT_TRADING_DAYS


def _td_phrase(n):
    return "in 1 trading day" if n == 1 else f"in {n} trading days"


def build_briefing():
    seen = load_seen()

    # One parallel yfinance pass for all 138 tickers — shared between
    # earnings fallback and dividends so the network is hit only once.
    cals = _yf_fetch_calendars(WATCHLIST) if (ENABLE_EARNINGS or ENABLE_DIVIDENDS) else {}

    earn_lines, div_lines = [], []

    if ENABLE_EARNINGS:
        for sym, info in sorted(fetch_earnings(cals).items()):
            n = trading_days_until(info["date"])
            if not _window_ok(n):
                continue
            key = f"EARN|{sym}|{info['date'].isoformat()}"
            if key in seen:
                continue
            seen[key] = now_et_date().isoformat()
            hour = HOUR_LABEL.get(info["hour"], "")
            bits = [f"{sym}", fmt_event_date(info["date"])]
            if hour:
                bits.append(f"({hour})")
            bits.append(_td_phrase(n))
            if info.get("eps") is not None:
                bits.append(f"EPS est {info['eps']}")
            earn_lines.append("- " + "  ".join(bits))

    if ENABLE_DIVIDENDS:
        for sym, info in sorted(fetch_dividends(cals).items()):
            n = trading_days_until(info["exdate"])
            if not _window_ok(n):
                continue
            key = f"DIV|{sym}|{info['exdate'].isoformat()}"
            if key in seen:
                continue
            seen[key] = now_et_date().isoformat()
            bits = [f"{sym}", "ex-div " + fmt_event_date(info["exdate"]),
                    _td_phrase(n)]
            if info.get("amount"):
                bits.append(f"${info['amount']:.2f}/sh")
            if info.get("pay"):
                bits.append(f"pay {fmt_event_date(info['pay'])}")
            div_lines.append("- " + "  ".join(bits))

    save_seen(seen)

    sections = []
    if earn_lines:
        sections.append("EARNINGS\n" + "\n".join(earn_lines))
    if div_lines:
        sections.append("EX-DIVIDEND\n" + "\n".join(div_lines))
    if not sections:
        return None

    header = (f"EARNINGS & DIVIDENDS  -  {ALERT_TRADING_DAYS} trading days out"
              f"\n{fmt_now()}")
    return header + "\n\n" + "\n\n".join(sections)


def main():
    briefing = build_briefing()
    if briefing:
        send_telegram(briefing)
    else:
        print(f"[{fmt_now()}] Nothing inside the {ALERT_TRADING_DAYS}-trading-day "
              f"window this pass.")


# ==================================================================
# 8. SELFTEST  (offline, no network, no keys)
# ==================================================================

def selftest():
    print("SELFTEST  (offline)\n" + "-" * 40)
    print("Today (ET):", now_et_date(), "\n")

    # Holiday detection
    assert is_trading_day(date(2026, 7, 3)) is False, "Jul 3 2026 should be closed"
    assert is_trading_day(date(2026, 11, 26)) is False, "Thanksgiving closed"
    assert is_trading_day(date(2026, 7, 6)) is True, "Jul 6 2026 should be open"
    print("Holiday calendar: OK")

    # Trading-day math, anchored to a known Monday (2026-06-01).
    mon = date(2026, 6, 1)   # Monday, not a holiday
    cases = {
        date(2026, 6, 1): 0,   # same day
        date(2026, 6, 2): 1,   # Tue
        date(2026, 6, 4): 3,   # Thu  -> 3 trading days out
        date(2026, 6, 5): 4,   # Fri
        date(2026, 6, 8): 5,   # next Mon (skips the weekend)
        date(2026, 5, 29): -1, # in the past
    }
    for d, expected in cases.items():
        got = trading_days_until(d, ref=mon)
        flag = "ok" if got == expected else "FAIL"
        print(f"  from {mon} to {d}: {got:>2}  (expected {expected})  {flag}")
        assert got == expected, f"trading-day math wrong for {d}"

    # A case that straddles a holiday: Memorial Day Mon 2026-05-25.
    fri = date(2026, 5, 22)              # Friday before Memorial Day
    got = trading_days_until(date(2026, 5, 28), ref=fri)  # Thu after
    # Sessions: Tue26, Wed27, Thu28 = 3 (Mon25 is the holiday, weekend skipped)
    print(f"  holiday straddle {fri} -> 2026-05-28: {got} (expected 3)",
          "ok" if got == 3 else "FAIL")
    assert got == 3, "holiday straddle math wrong"
    print("Trading-day math: OK")

    # De-dup window logic
    assert _window_ok(3) and _window_ok(1) and not _window_ok(0) \
        and not _window_ok(4), "window logic wrong"
    print("Lead-window logic: OK")

    print("-" * 40)
    print("All checks passed. Logic is sound; live run needs network + yfinance.")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
