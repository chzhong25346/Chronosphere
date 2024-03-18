import json
import logging
import re
import time
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf

from ..models import Index, Monitorlist_Index
from .email import sendMail
from ..utils.config import Config
from ..utils.utils import get_smarter_session

logger = logging.getLogger('main.monitor')
pd.set_option('mode.chained_assignment', None)

# Set logging level to WARNING or ERROR for yfinance logger
yfinance_logger = logging.getLogger('yfinance')
yfinance_logger.setLevel(logging.WARNING)  # or logging.ERROR

def monitor_analysis(sdic):
    picks_dic = {}
    today = datetime.now().date()
    for dbname, s in sdic.items():
        if dbname in ('financials'):
            logger.info("Start to process: %s" % dbname)
            # Get Monitor List
            result_list = s.query(Monitorlist_Index).distinct().all()
            data_list = [{'symbol': row.symbol,
                          'low_price': row.low_price,
                          'high_price': row.high_price,
                          'latest_reached': row.latest_reached} for row in result_list]
            for monitor_data in data_list:
                current_price = get_current_price(monitor_data['symbol'])
                if current_price is not None:
                    if monitor_data['low_price'] <= current_price <= monitor_data['high_price']:
                        latest_reached_date = monitor_data['latest_reached'] if monitor_data['latest_reached'] else None
                        if latest_reached_date != today:
                            existing_entry = s.query(Monitorlist_Index).filter_by(symbol=monitor_data['symbol']).first()
                            if existing_entry:
                                existing_entry.low_price = monitor_data['low_price']
                                existing_entry.high_price = monitor_data['high_price']
                                existing_entry.latest_reached = today
                            else:
                                new_entry = Monitorlist_Index(symbol=monitor_data['symbol'],
                                                              low_price=monitor_data['low_price'],
                                                              high_price=monitor_data['high_price'],
                                                              latest_reached=today)
                                s.add(new_entry)
                            s.commit()
                            picks_dic[monitor_data['symbol']] = current_price

    if picks_dic:
        print(picks_dic)
        sendMail(Config, picks_dic)


def get_current_price(ticker):
    t = yf.Ticker(ticker)
    try:
        current_price = t.history(period='1d')['Close'].iloc[-1]
        return current_price
    except:
        tinfo = None