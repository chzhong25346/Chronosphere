import json
import socket
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
from .send_notice import send_event_to_server

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
                          'latest_reached': row.latest_reached,
                          'pct_current_to_high': row.pct_current_to_high} for row in result_list]
            for monitor_data in data_list:
                current_price = get_current_price(monitor_data['symbol'])
                if current_price is not None:
                    pct_current_to_high = calculate_pct_current_to_high(current_price, monitor_data['high_price'])
                    # print(monitor_data['symbol'], current_price, pct_current_to_high) # CHECKPOINT
                    if current_price <= monitor_data['high_price']:
                        latest_reached_date = monitor_data['latest_reached'] if monitor_data['latest_reached'] else None
                        if latest_reached_date != today:
                            s.query(Monitorlist_Index).filter(Monitorlist_Index.symbol == monitor_data['symbol']).delete()
                            new_entry = Monitorlist_Index(symbol=monitor_data['symbol'],
                                                          low_price=monitor_data['low_price'],
                                                          high_price=monitor_data['high_price'],
                                                          latest_reached=today,
                                                          pct_current_to_high=pct_current_to_high)
                            s.add(new_entry)
                            s.commit()
                            picks_dic[monitor_data['symbol']] = current_price
                    else:
                        s.query(Monitorlist_Index).filter(Monitorlist_Index.symbol == monitor_data['symbol']).delete()
                        new_entry = Monitorlist_Index(symbol=monitor_data['symbol'],
                                                      low_price=monitor_data['low_price'],
                                                      high_price=monitor_data['high_price'],
                                                      latest_reached=monitor_data['latest_reached'],
                                                      pct_current_to_high=pct_current_to_high)
                        s.add(new_entry)
                        s.commit()


    if picks_dic:
        logger.info("Price reached - (%s)" % (picks_dic))
        sendMail(Config, picks_dic)
        logger.info('email sent')
        send_event_to_server(Config)
        logger.info('notice sent')
    else:
        logger.info('Nothing reached')

def get_current_price(ticker):
    t = yf.Ticker(ticker)
    try:
        current_price = t.history(period='1d')['Close'].iloc[-1]
        return current_price
    except:
        tinfo = None


def calculate_pct_current_to_high(current_price, high_price, decimal_places=2):
    if high_price != 0:
        pct_current_to_high = ((current_price - high_price) / high_price) * 100
        pct_current_to_high = round(pct_current_to_high, decimal_places)
    else:
        pct_current_to_high = 0  # To handle the case when high_price is zero
    return pct_current_to_high