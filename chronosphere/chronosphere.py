import getopt, sys, time, math, logging, logging.config
from .db.db import Db
from .utils.config import Config
from .utils.utils import gen_id
from .analysis.analysis_module import analysis_hub
logging.config.fileConfig('chronosphere/log/logging.conf')
logger = logging.getLogger('main')


def main(argv):
    time_start = time.time()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ht:l:g:r:v:", ["help", "turnover=","line=","gap","rsi","hvlc="])
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
        else:
            assert False, "unhandled option"

    elapsed = math.ceil((time.time() - time_start)/60)
    logger.info("%s took %d minutes to run" % ( (',').join(argv), elapsed ) )


def usage():
    helps = """
    1.  -t/--turnover market(china/canada/usa) : Turnover Ratio Analysis
    2.  -l/--line market(china/canada/usa) : Support and Resistance Line Analysis
    3.  -g/--gap market(china/canada/usa) : Gaps created with range
    4.  -r/--rsi market(china/canada/usa/eei) : RSI prediction
    5.  -v/--hvlc market(china/canada/usa/eei) : High Volume Low Change
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
        s.close()
        logger.info("Session closed: '%s' " % s.bind.url.database)
