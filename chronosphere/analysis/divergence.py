import json
import logging
import re
import time
import sys
import pandas as pd
import math

from ..models import Index, Watchlist_Index, Quote, Quote_CSI300
from .email import sendMail_Message
from ..utils.config import Config
from ..utils.utils import get_smarter_session
from stockstats import StockDataFrame as SDF
logger = logging.getLogger('main.screener')
pd.set_option('mode.chained_assignment', None)


def divergence_analysis(sdic):
    picks = []
    watch_tickers = {
        r.symbol
        for r in sdic['financials']
        .query(Watchlist_Index.symbol)
        .distinct()
    }
    # watch_tickers = ['000768.SZ', 'CNR.TO', 'RCI-B.TO', 'MSFT', 'DD'] ## TEST CHECKPOINT
    # watch_tickers = ['SYK']  ## TEST CHECKPOINT
    for dbname, session in sdic.items():
        if dbname == 'financials':
            continue

        for ticker in watch_tickers:
            # normalize ticker ONLY for tsxci
            search_ticker = (
                ticker.replace('.TO', '')
                if dbname == 'tsxci'
                else ticker
            )

            exists = (
                session.query(Index)
                .filter(Index.symbol == search_ticker)
                .first()
            )

            if exists:
                # print(ticker, '->', search_ticker, dbname)
                s = sdic[dbname]
                if dbname == 'csi300':
                    df = pd.read_sql(s.query(Quote_CSI300).\
                        filter(Quote_CSI300.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                elif dbname == 'tsxci':
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == search_ticker).\
                        statement, s.bind, index_col='date').sort_index()
                else:
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()

                # Check if weekly macd is bullish? Example output: True 15  -> bullish for 3 weeks (~15 trading days)
                is_bullish, days = _weekly_updown_trend(df.copy())

                # Add MACD in df and slice trend days
                df = _get_macd(df)
                df = _rows_in_trend(df, days)

                if is_bullish:
                    logger.info("Finding divergence - %s - %s for %s days" % (ticker, 'Bull', days))
                    is_ath = df.iloc[-1]['high'] == df['high'].max(skipna=True) # All Time High
                    is_macdh_down = len(df) >= 2 and (df['macdh'].iloc[-1] < df['macdh'].iloc[-2]) # Bar Lower than previous
                    lo, hi =  _find_shadow_range('Bull', df) # is High in previous upper shadow
                    is_in_shadow = lo < df.iloc[-1]['high'] <= hi and df.iloc[-1]['close'] <= lo
                    long_shadow = _has_long_shadow('Bull', df)
                    if (is_ath or is_in_shadow) and is_macdh_down:
                        if is_ath and long_shadow:
                            diff_ma = _get_ma5_ma10_diff_percent(df, price_col='close', decimals=2)
                            picks.append(ticker + " ↑" + str(days)+'New' + " \u0394" + diff_ma)
                            logger.info("Divergence found! - (%s)" % (ticker))
                        elif is_ath and not long_shadow:
                            pass
                        elif is_in_shadow:
                            diff_ma = _get_ma5_ma10_diff_percent(df, price_col='close', decimals=2)
                            picks.append(ticker + " ↑" + str(days) + " \u0394" + diff_ma)
                            logger.info("Divergence found! - (%s)" % (ticker))
                    #
                    # print('is ath', is_ath)
                    # print('is long shadow', long_shadow)
                    # print('is macdh_down', is_macdh_down)
                    # print('is in_shadow', is_in_shadow, lo, hi)
                    # print('today high', df.iloc[-1]['high'])

                # is bearish
                else:
                    logger.info("Finding divergence - %s - %s for %s days" % (ticker, 'Bear', days))
                    is_atl = df.iloc[-1]['low'] == df['low'].min(skipna=True) # All Time Low
                    is_macdh_up = len(df) >= 2 and (df['macdh'].iloc[-1] > df['macdh'].iloc[-2]) # Bar higher than previous
                    lo, hi = _find_shadow_range('Bear',df)  # is High in previous lower shadow
                    is_in_shadow = lo <= df.iloc[-1]['low'] < hi and df.iloc[-1]['close'] >= hi
                    long_shadow = _has_long_shadow('Bear', df)
                    if (is_atl or is_in_shadow) and is_macdh_up:
                        if is_atl and long_shadow:
                            diff_ma = _get_ma5_ma10_diff_percent(df, price_col='close', decimals=2)
                            picks.append(ticker + " ↓"  + str(days) + 'New' + " \u0394" + diff_ma)
                            logger.info("Divergence found！ - (%s)" % (ticker))
                        elif is_atl and not long_shadow:
                            pass
                        elif is_in_shadow:
                            diff_ma = _get_ma5_ma10_diff_percent(df, price_col='close', decimals=2)
                            picks.append(ticker + " ↓" + str(days) + " \u0394" + diff_ma)
                            logger.info("Divergence found！ - (%s)" % (ticker))
                    #
                    # print('is atl', is_atl)
                    # print('is long shadow', long_shadow)
                    # print('is is_macdh_up', is_macdh_up)
                    # print('is in_shadow', is_in_shadow, lo, hi)
                    # print('today low', df.iloc[-1]['low'])

    if len(picks) > 0:
        logger.info("All Divergence found: - (%s)" % (picks))
        sendMail_Message(Config, 'Divergence Found', picks)


# Assessories Functions ===================================


def _get_ma5_ma10_diff_percent(df, price_col='close', decimals=2):
    """
    Calculate (MA5 - MA10) / MA10 * 100 for the latest row.

    Returns:
        float or None: percentage difference (positive if MA5 > MA10)
        str         : formatted string like "+4.82%" or "-7.15%" or error message

    Example output:
        5.34    →  "+5.34%"
        -3.1    →  "-3.10%"
    """
    if len(df) < 10:
        return None, f"Not enough data — need ≥10 rows, got {len(df)}"

    if price_col not in df.columns:
        return None, f"Column '{price_col}' not found in DataFrame"

    df = df.copy()

    # Calculate simple moving averages
    df['ma5'] = df[price_col].rolling(window=5, min_periods=5).mean()
    df['ma10'] = df[price_col].rolling(window=10, min_periods=10).mean()

    latest = df.iloc[-1]

    if pd.isna(latest['ma5']) or pd.isna(latest['ma10']):
        return None, "Not enough valid data to compute both MA5 and MA10"

    diff_pct = (latest['ma5'] - latest['ma10']) / latest['ma10'] * 100
    rounded = round(diff_pct, decimals)

    # Format with sign
    sign = "+" if rounded > 0 else "" if rounded == 0 else ""
    formatted = f"{sign}{rounded:.{decimals}f}%"
    return formatted


def _has_long_shadow(direction, df, ratio=1.3):
    """
    Check if latest candle has long upper/lower shadow.
    Returns False if the candle is a doji (open == close).
    """
    if df is None or len(df) == 0:
        return False

    row = df.iloc[-1]

    # Handle flexible column names
    o = row['o'] if 'o' in row else row['open']
    h = row['h'] if 'h' in row else row['high']
    l = row['l'] if 'l' in row else row['low']
    c = row['c'] if 'c' in row else row['close']

    if any(v is None for v in (o, h, l, c)):
        return False

    body = abs(c - o)

    # STRICT DOJI CHECK: If no body, it's not a "long shadow" candle
    if body == 0:
        return False

    if direction == 'Bull':
        upper_shadow = h - max(o, c)
        return upper_shadow >= ratio * body
    else:  # Bear
        lower_shadow = min(o, c) - l
        return lower_shadow >= ratio * body


def _find_shadow_range(direction, df):

    if df is None or len(df) == 0:
        return (None, None)

    d = df.copy()

    # Tolerate mixed column naming
    def _pick(row, short, long_):
        return row[short] if short in row else row[long_]

    # Choose the anchor row based on direction
    if direction == 'Bull':
        # Bar with the maximum high (last occurrence if ties)
        # idxmax gives first occurrence; we reverse to find last
        try:
            anchor_idx = d['high'].iloc[::-1].idxmax()
        except KeyError:
            anchor_idx = d['h'].iloc[::-1].idxmax()
    elif direction == 'Bear':
        # Bar with the minimum low (last occurrence if ties)
        try:
            anchor_idx = d['low'].iloc[::-1].idxmin()
        except KeyError:
            anchor_idx = d['l'].iloc[::-1].idxmin()
    else:
        # Unknown direction
        return (None, None)

    # Extract the anchor row
    anchor = d.loc[anchor_idx]

    # Flexible OHLC access
    o = _pick(anchor, 'o', 'open')
    h = _pick(anchor, 'h', 'high')
    l = _pick(anchor, 'l', 'low')
    c = _pick(anchor, 'c', 'close')

    # Validate values
    if any(v is None for v in (o, h, l, c)):
        return (None, None)

    # Compute shadow per rules
    if direction == 'Bear':
        # LOWER shadow of the min-low bar
        # (c, l) if bearish else (o, l) if bullish/doji
        lower_start = c if c < o else o
        lo = min(lower_start, l)
        hi = max(lower_start, l)
        return (lo, hi)

    else:  # direction == 'Bull'
        # UPPER shadow of the max-high bar
        # (o, h) if c <= o (bearish/doji), else (c, h) if bullish
        upper_start = o if c <= o else c
        lo = min(upper_start, h)
        hi = max(upper_start, h)
        return (lo, hi)



def _rows_in_trend(df, n):
    """
    Returns the previous n rows immediately before the latest row.
    If there are fewer than n+1 rows, returns as many as available.
    """
    if len(df) <= 1:
        return df.iloc[0:0]   # empty
    start = max(0, len(df) - (n + 1))
    return df.iloc[start:]


# Calculate MACD
def _get_macd(df):
    """
    Compute daily MACD on a daily OHLCV DataFrame using StockStats.
    Returns a copy with ['macd', 'macds', 'macdh'] columns.
    """
    d = df.copy()

    # Ensure DatetimeIndex and sorted
    if not isinstance(d.index, pd.DatetimeIndex):
        d.index = pd.to_datetime(d.index)
    d = d.sort_index()

    # StockStats expects lowercase column names
    d.columns = [c.lower() for c in d.columns]

    s = SDF.retype(d)
    # Defaults are MACD (12,26,9)
    d['macd']  = s['macd']    # DIF
    d['macds'] = s['macds']   # DEA
    d['macdh'] = s['macdh']   # Histogram (DIF - DEA)

    return d



def _weekly_updown_trend(df):
    """
    Returns:
        is_bullish (bool): DIF > DEA on weekly MACD
        days (int): exact number of trading days in the current regime, based on your daily records
    """
    if df is None or df.empty:
        return None, 0

    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df.columns = [c.lower() for c in df.columns]

    # Build weekly OHLCV
    agg_map = {'open': 'first','high': 'max','low': 'min','close': 'last','volume': 'sum'}
    if 'adjusted' in df.columns:
        agg_map['adjusted'] = 'last'

    has_symbol = 'symbol' in df.columns
    n_symbols = df['symbol'].nunique() if has_symbol else 1

    if n_symbols > 1:
        # If a multi-symbol df slipped through, filter to one or adapt as needed
        raise ValueError("This helper expects a single-ticker DataFrame.")

    # Weekly bars (Mon–Fri grouped, labeled on Friday)
    df_weekly = (
        df.resample('W-FRI', label='right', closed='right')
          .agg(agg_map)
          .dropna(how='all')
    )
    if has_symbol:
        df_weekly.insert(0, 'symbol', df['symbol'].iloc[-1])

    # Compute weekly MACD
    stock = SDF.retype(df_weekly.copy())
    _ = stock['macd']; _ = stock['macds']; _ = stock['macdh']

    sig = stock[['macd','macds']].dropna()
    if sig.empty:
        return None, 0

    bull = sig['macd'] > sig['macds']
    run_id = bull.ne(bull.shift()).cumsum()
    # Identify current run (the last run_id)
    current_run_id = int(run_id.iloc[-1])
    current_run_weeks = sig.index[run_id == current_run_id]  # DatetimeIndex of weekly labels (Fridays)
    is_bullish = bool(bull.iloc[-1])

    # Count exact trading days in those weekly periods from the original daily df
    # Build a mapping of week-ending Friday -> count of daily rows that fall in that Sat–Fri window
    daily_counts = (
        df.assign(_one=1)
          .groupby(pd.Grouper(freq='W-FRI', label='right', closed='right'))['_one']
          .sum()
          .dropna()
    )
    # Sum only the weeks that are in the current run
    exact_trading_days = int(daily_counts.reindex(current_run_weeks).fillna(0).sum())

    return is_bullish, exact_trading_days




