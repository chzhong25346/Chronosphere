import getopt
import logging
import logging.config
import math
import sys
import time
from .analysis.analysis_module import analysis_hub
from .db.db import Db
from .utils.config import Config

logging.config.fileConfig('chronosphere/log/logging.conf')
logger = logging.getLogger('main')

def main(argv):
    time_start = time.time()
    try:
        opts, args = getopt.getopt(
            argv,
            "ht:l:g:r:v:u:s:m:d:k:b:",
            [
                "help", "turnover=", "line=", "gap", "rsi", "hvlc=",
                "ublb=", "screener=", "monitor=", "divergence=",
                "ticker=", "backtrace="
            ]
        )
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    # Defer execution: remember the last requested task+market
    selected_market = None
    selected_module = None
    ticker = None
    backtrace = None

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()

        elif o in ("-t", "--turnover"):
            selected_market = a
            selected_module = 'turnover'

        elif o in ("-l", "--line"):
            selected_market = a
            selected_module = 'lines'

        elif o in ("-g", "--gap"):
            selected_market = a
            selected_module = 'gaps'

        elif o in ("-r", "--rsi"):
            selected_market = a
            selected_module = 'rsi'

        elif o in ("-v", "--hvlc"):
            selected_market = a
            selected_module = 'hvlc'

        elif o in ("-u", "--ublb"):
            selected_market = a
            selected_module = 'ublb'

        elif o in ("-s", "--screener"):
            selected_market = a
            selected_module = 'screener'

        elif o in ("-m", "--monitor"):
            selected_market = a
            selected_module = 'monitor'

        elif o in ("-d", "--divergence"):
            selected_market = a
            selected_module = 'divergence'

        elif o in ("-k", "--ticker"):
            ticker = a.strip()

        elif o in ("-b", "--backtrace"):
            backtrace = int(a)

        else:
            assert False, "unhandled option"

    # Execute once, after all options are known
    if selected_market and selected_module:
        analysis(selected_market, selected_module, ticker=ticker, backtrace=backtrace)
    else:
        usage()
        sys.exit(2)

def usage():
    helps = """
 1. -t/--turnover market(china/canada/usa) : Turnover Ratio Analysis
 2. -l/--line market(china/canada/usa)     : Support and Resistance Line Analysis
 3. -g/--gap market(china/canada/usa)      : Gaps created with range
 4. -r/--rsi market(china/canada/usa/eei/commodity) : RSI prediction
 5. -v/--hvlc market(china/canada/usa/eei/commodity): High Volume Low Change
 6. -u/--ublb market(china/canada/usa/eei/commodity): Up Band Lower Band Cross
 7. -s/--screener market(na/china/watchlist/commodity): Screener
 8. -m/--monitor market(financials)        : Price Monitor
 9. -d/--divergence market(all)            : Divergence in Watchlist[DB:financial]
10. -k/--ticker SYMBOL                     : Optional ticker filter, e.g., MSFT
11. -b/--backtrace N                       : Optional lookback window, e.g., 200
"""
    print(helps)

def analysis(market, module, ticker=None, backtrace=None):
    logger.info('Load module: [Analysis]')

    if market == 'china':
        db_name_list = ['csi300', 'financials', 'learning']
    elif market == 'canada':
        db_name_list = ['tsxci', 'financials', 'learning']
    elif market == 'usa':
        db_name_list = ['sp100', 'nasdaq100', 'financials', 'learning']
    elif market == 'eei':
        db_name_list = ['eei', 'financials', 'learning']
    elif market == 'na':
        db_name_list = ['tsxci', 'sp100', 'eei']
    elif market == 'commodity':
        db_name_list = ['commodity', 'financials', 'learning']
    elif market == 'watchlist':
        db_name_list = ['financials']
    elif market == 'market':
        db_name_list = ['market', 'financials', 'learning']
    elif market == 'financials':
        db_name_list = ['financials']
    elif market == 'all':
        db_name_list = ['financials', 'csi300', 'tsxci', 'eei', 'sp100', 'nasdaq100']
    elif market == 'testing':
        db_name_list = ['testing', 'learning']
    else:
        db_name_list = []

    sdic = {}
    for name in db_name_list:
        Config.DB_NAME = name
        db = Db(Config)
        s = db.session()
        sdic.update({name: s})

    analysis_hub(module, sdic=sdic, ticker=ticker, backtrace=backtrace)

    for name, s in sdic.items():
        try:
            s.close()
            logger.info("Session closed: '%s' " % s.bind.url.database)
        except:
            pass