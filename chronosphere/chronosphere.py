import getopt
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
        opts, args = getopt.getopt(sys.argv[1:], "ht:l:g:r:v:u:s:m:", ["help", "turnover=", "line=", "gap", "rsi", "hvlc=", "ublb=", "screener=", "monitor="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        output = None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-t", "--turnover"):
            market = a
            analysis(market, 'turnover')
        elif o in ("-l", "--line"):
            market = a
            analysis(market, 'lines')
        elif o in ("-g", "--gap"):
            market = a
            analysis(market, 'gaps')
        elif o in ("-r", "--rsi"):
            market = a
            analysis(market, 'rsi')
        elif o in ("-v", "--hvlc"):
            market = a
            analysis(market, 'hvlc')
        elif o in ("-u", "--ublb"):
            market = a
            analysis(market, 'ublb')
        elif o in ("-s", "--screener"):
            market = a
            analysis(market, 'screener')
        elif o in ("-m", "--monitor"):
            market = a
            analysis(market, 'monitor')
        else:
            assert False, "unhandled option"

    elapsed = math.ceil((time.time() - time_start)/60)
    logger.info("%s took %d minutes to run" % ( (',').join(argv), elapsed ) )


def usage():
    helps = """
    1.  -t/--turnover market(china/canada/usa) : Turnover Ratio Analysis
    2.  -l/--line market(china/canada/usa) : Support and Resistance Line Analysis
    3.  -g/--gap market(china/canada/usa) : Gaps created with range
    4.  -r/--rsi market(china/canada/usa/eei/commodity) : RSI prediction
    5.  -v/--hvlc market(china/canada/usa/eei/commodity) : High Volume Low Change
    6.  -u/--ublb market(china/canada/usa/eei/commodity) : Up Band Lower Band Cross
    7.  -s/--screener market(na/china/watchlist/commodity) : Screener
    8.  -m/--monitor market(financials): Price Monitor
    """
    print(helps)


def analysis(market, module):
    logger.info('Load module: [Analysis]')
    if market == 'china':
        db_name_list = ['csi300','financials','learning']
    elif market == 'canada':
        db_name_list = ['tsxci','financials','learning']
    elif market == 'usa':
        db_name_list = ['sp100','nasdaq100','financials','learning']
    elif market == 'eei':
        db_name_list = ['eei','financials','learning']
    elif market == 'na':
        db_name_list = ['tsxci','sp100','eei']
    elif market == 'commodity':
        db_name_list = ['commodity','financials','learning']
    elif market == 'watchlist':
        db_name_list = ['financials']
    elif market == 'market':
        db_name_list = ['market','financials','learning']
    elif market == 'financials':
        db_name_list = ['financials']
    elif market == 'testing':
        db_name_list = ['testing','learning']
    sdic = {}
    for name in db_name_list:
        Config.DB_NAME = name
        db = Db(Config)
        s = db.session()
        sdic.update({name:s})

    analysis_hub(module, sdic=sdic)

    for name, s in sdic.items():
        try:
            s.close()
            logger.info("Session closed: '%s' " % s.bind.url.database)
        except:
            pass
