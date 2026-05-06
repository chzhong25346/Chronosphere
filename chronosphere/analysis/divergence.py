import logging
import pandas as pd
from ..models import Index, Watchlist_Index, Quote, Quote_CSI300, Monitorlist_Index
from .email import sendMail_Message
from .ntfy import sendNtfy_Message
from ..utils.config import Config
from stockstats import StockDataFrame as SDF
logger = logging.getLogger('main.divergence')
pd.set_option('mode.chained_assignment', None)


def divergence_analysis(sdic, ticker=None, backtrace=None):
    backtrace_mode = False
    s_financials = sdic['financials']

    monitor_list= [{'symbol': row.symbol,
                  'latest_reached': row.latest_reached} for row in s_financials.query(Monitorlist_Index).distinct().all()]

    picks = []

    watch_tickers = {
        r.symbol
        for r in sdic['financials']
        .query(Watchlist_Index.symbol)
        .distinct()
    }

    # watch_tickers = ['NOC']

    if None not in (ticker, backtrace):
        watch_tickers = [ticker]
        backtrace_mode = True

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
                df_full = df.copy()
                df = _rows_in_trend(df, days)

                # Backtrace Mode
                if backtrace_mode:
                    n = int(backtrace)
                    if n > 0:
                        asof_index = df.index[-n:]
                    else:
                        asof_index = df.index
                    for asof in asof_index:
                        # Prefix snapshot up to and including 'asof'
                        df_asof = df.loc[:asof]
                        if df_asof is None or df_asof.empty:
                            continue
                        logger.info(f"Backtrace: {ticker} {pd.to_datetime(asof).date()}")

                        _decision_maker(
                                        s_financials,
                                        df_asof,
                                        df_full,
                                        monitor_list=monitor_list,
                                        picks=picks,
                                        ticker=ticker,
                                        is_bullish=is_bullish,
                                        days=days,
                                        backtrace_mode=True
                                    )

                # Normal mode watchliist @ financials
                else:
                    _decision_maker(
                        s_financials,
                        df,
                        df_full,
                        monitor_list=monitor_list,
                        picks=picks,
                        ticker=ticker,
                        is_bullish=is_bullish,
                        days=days
                        )

    if len(picks) > 0:
        if not backtrace_mode:
            logger.info("All Divergence found: - (%s)", picks)

            # try:
            #     sendMail_Message(Config, 'Divergence Found', picks)
            # except Exception as e:
            #     logger.error("Email failed, continuing with ntfy: %s", e)
            #
            # # ntfy always executes
            # try:
            #     sendNtfy_Message('Divergence Found', picks, 'Divergence_20260410')
            # except Exception as e:
            #     logger.error("ntfy failed: %s", e)


# Assessories Functions ===================================

def _decision_maker(
        s_financials,
        df,
        df_full,
        monitor_list,
        picks,
        ticker,
        is_bullish,
        days,
        backtrace_mode=False):


    # If ticker in monitor list already, check if volume of latest day is 1.2 times larger than the volume of
    # latest_reached date. if true, add ticker to picks.
    if any(d['symbol'] == ticker for d in monitor_list):
        latest_reached = next((item for item in monitor_list if item['symbol'] == ticker), None)['latest_reached']
        if _volume_surge(df, df_full, latest_reached):
            logger.info("Volume Surge! - (%s) - %s" % (ticker, df.index[-1].strftime('%b %d')))
            picks.append(ticker + ' Volume Surge')

    # is Bullish? ----------------------------------------
    if is_bullish:
        if backtrace_mode is False:
            logger.info("Finding divergence - %s - %s for %s days" % (ticker, 'Bull', days))
        is_ath = df.iloc[-1]['high'] == df['high'].max(skipna=True)  # All Time High
        is_macdh_down = len(df) >= 2 and (df['macdh'].iloc[-1] < df['macdh'].iloc[-2])  # Bar Lower than previous
        lo, hi = _find_shadow_range('Bull', df)  # is High in previous upper shadow
        is_in_shadow = lo < df.iloc[-1]['high'] <= hi and df.iloc[-1]['close'] <= lo
        long_shadow = _has_long_shadow('Bull', df)
        diff_ma = _get_ma5_ma10_diff_percent(df, df_full, price_col='close')
        if (is_ath or is_in_shadow) and is_macdh_down:
            if is_ath and long_shadow and diff_ma > 0:
                _update_monitor_table(s_financials, ticker, df.index[-1])
                picks.append(f"{ticker} ↑{days}NewΔ{float(diff_ma):+.2f}% - {df.index[-1].strftime('%b %d')}")
                logger.info("Divergence found! - (%s) - %s" % (ticker, df.index[-1].strftime('%b %d')) )
            elif is_ath and not long_shadow:
                pass
            elif is_in_shadow and diff_ma > 0:
                _update_monitor_table(s_financials, ticker, df.index[-1])
                picks.append(f"{ticker} ↑{days}Δ{float(diff_ma):+.2f}% - {df.index[-1].strftime('%b %d')}")
                logger.info("Divergence found! - (%s) - %s" % (ticker, df.index[-1].strftime('%b %d')) )

    # is Bearish? ----------------------------------------
    else:
        if backtrace_mode is False:
            logger.info("Finding divergence - %s - %s for %s days" % (ticker, 'Bear', days))
        is_atl = df.iloc[-1]['low'] == df['low'].min(skipna=True)  # All Time Low
        is_macdh_up = len(df) >= 2 and (df['macdh'].iloc[-1] > df['macdh'].iloc[-2])  # Bar higher than previous
        lo, hi = _find_shadow_range('Bear', df)  # is High in previous lower shadow
        is_in_shadow = lo <= df.iloc[-1]['low'] < hi and df.iloc[-1]['close'] >= hi
        long_shadow = _has_long_shadow('Bear', df)
        diff_ma = _get_ma5_ma10_diff_percent(df, df_full, price_col='close')
        if (is_atl or is_in_shadow) and is_macdh_up:
            if is_atl and long_shadow and diff_ma <  0:
                _update_monitor_table(s_financials, ticker, df.index[-1])
                picks.append(f"{ticker} ↓{days}NewΔ{float(diff_ma):+.2f}% - {df.index[-1].strftime('%b %d')}")
                logger.info("Divergence found! - (%s) - %s" % (ticker, df.index[-1].strftime('%b %d')) )
            elif is_atl and not long_shadow:
                pass
            elif is_in_shadow and diff_ma < 0:
                _update_monitor_table(s_financials, ticker, df.index[-1])
                picks.append(f"{ticker} ↓{days}Δ{float(diff_ma):+.2f}% - {df.index[-1].strftime('%b %d')}")
                logger.info("Divergence found! - (%s) - %s" % (ticker, df.index[-1].strftime('%b %d, %Y')) )


def _volume_surge(df, df_full, latest_reached, window=90, max_days=5, multiplier=1.2):
    """
    True if:
    1) latest day is within `max_days` trading days after latest_reached
    2) latest volume > multiplier × 90-day average volume (computed from df_full)
    """

    if df is None or df.empty or df_full is None or df_full.empty:
        return False

    if 'volume' not in df.columns or 'volume' not in df_full.columns:
        return False

    # Ensure DatetimeIndex
    for d in (df, df_full):
        if not isinstance(d.index, pd.DatetimeIndex):
            d.index = pd.to_datetime(d.index)

    df = df.sort_index()
    df_full = df_full.sort_index()

    latest_reached = pd.to_datetime(latest_reached)

    if latest_reached not in df.index:
        return False

    # ✅ Trading-day distance check (based on sliced df)
    latest_pos = len(df) - 1
    reached_pos = df.index.get_loc(latest_reached)
    days_after = latest_pos - reached_pos

    if days_after <= 0 or days_after > max_days:
        return False

    # ✅ 90-day average from FULL history
    avg_90 = df_full['volume'].rolling(window=window, min_periods=window).mean()

    latest_date = df.index[-1]

    if latest_date not in avg_90.index:
        return False

    latest_avg = avg_90.loc[latest_date]
    if pd.isna(latest_avg):
        return False

    latest_vol = df.loc[latest_date, 'volume']

    return latest_vol > (latest_avg * multiplier)




def _update_monitor_table(s, symbol, date):
    s.query(Monitorlist_Index).filter(Monitorlist_Index.symbol == symbol).delete()
    s.add(Monitorlist_Index(symbol=symbol,
                      latest_reached=date))
    s.commit()


def _get_ma5_ma10_diff_percent(df, df_full, price_col='close'):
    """
    Returns a string like '+4.82%' or 'N/A' across all code paths.
    """
    # Complement DF
    if df is None or df.empty or len(df) < 10 or price_col not in df.columns:
        latest_date = df.index.max()
        df_full_filtered = df_full[df_full.index <= latest_date]
        df_combined = pd.concat([df_full_filtered, df])
        df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
        df = df_combined.sort_index()

    df['ma5'] = df[price_col].rolling(window=5, min_periods=5).mean()
    df['ma10'] = df[price_col].rolling(window=10, min_periods=10).mean()

    latest = df.iloc[-1]
    if pd.isna(latest['ma5']) or pd.isna(latest['ma10']):
        return "N/A"

    diff_pct = (latest['ma5'] - latest['ma10']) / latest['ma10'] * 100

    return diff_pct


def _has_long_shadow(direction, df, ratio=1.05):
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




