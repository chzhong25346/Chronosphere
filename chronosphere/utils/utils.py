import hashlib
import logging
import math
import re

import pandas as pd
import requests_cache
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from stockstats import StockDataFrame
from datetime import datetime, time

logger = logging.getLogger('main.util')


def gen_id(string):
    return int(hashlib.md5(str.encode(string)).hexdigest(), 16)


def groupby_na_to_zero(df, ticker):
    df = df.groupby(ticker).first()
    df.fillna(0, inplace=True)
    return df


def missing_ticker(index):
    tickers = set()
    rx = re.compile("\((.+)\)")
    fh = open('log.log', 'r')
    for line in fh:
        if 'Found duplicate' not in line:
            strings = re.findall(rx, line)
            if strings and index in strings[0]:
                    tickers.add(strings[0].split(',')[1])
    logger.info('Found %d missing quotes in %s' % (len(tickers), index))
    fh.close()
    return list(tickers)


def find_dateByVolume(df, tdate, fvolume, direction):
    i = df.index.get_loc(tdate)
    sum = df.iloc()[i]['volume']
    while sum < fvolume:
        if direction == 'backward':
            sum = df.iloc()[i-1]['volume'] + sum
            i = i-1
        elif direction == 'forward':
            sum = df.iloc()[i+1]['volume'] + sum
            i = i+1
    return df.iloc()[i]


def distance_date(items, pivot, distance):
    if distance == "nearest":
        return min(items, key=lambda x: abs(x - pivot))
    elif distance == "farest":
        return max(items, key=lambda x: abs(x - pivot))


def latest_over_rsi70(df):
    # latest RSI >= 70 check
    pd.set_option('mode.chained_assignment',None)
    # Calculate RSI-14
    df = StockDataFrame.retype(df)
    df['rsi_14'] = df['rsi_14']
    # DF clearning
    df = df[(df != 0).all(1)]
    df.dropna(inplace=True)
    over_rsi70 = False
    for index, row in df[::-1].iterrows():
        if row['rsi_14'] >= 70:
            return True
        elif row['rsi_14'] <= 30:
            return False


def millify(n):
    millnames = ['',' Thousand',' Million',' Billion',' Trillion']
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])


# Yfinance Smarter scraping
def get_smarter_session():
    class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
        """ """
    session = CachedLimiterSession(
        per_second=0.9,
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache("yfinance.cache"),
    )
    session.headers['User-agent'] = 'my-program/1.0'
    # session = requests_cache.CachedSession('yfinance.cache')
    return session


def run_at_time(hour, minute, func):
    now = datetime.now().time()
    if now >= time(hour, minute):  # Checking if the current time is within the specified minute range
        logger.info(f"It's {now} AM. Running your code now.")
        func()  # Call the provided function
    else:
        logger.info(f"Not yet {hour}:{minute} AM. Waiting...")
