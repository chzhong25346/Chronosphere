import getopt, sys, time, math, logging, logging.config
from .db.db import Db
from .utils.config import Config
from .analysis.analysis_module import analysis_hub
logging.config.fileConfig('chronosphere/log/logging.conf')
logger = logging.getLogger('main')


def main(argv):
    time_start = time.time()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ht:", ["help", "turnover="])
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
            to_analysis(market)
        else:
            assert False, "unhandled option"

    elapsed = math.ceil((time.time() - time_start)/60)
    logger.info("%s took %d minutes to run" % ( (',').join(argv), elapsed ) )


def usage():
    print('Turnover Ratio Analysis: -t/--turnover market(china/na)')


def to_analysis(market):
    logger.info('Run Task: [Turnover Analysis]')
    if market == 'china':
        db_name_list = ['csi300','financials','learning']
    elif market == 'na':
        db_name_list = ['nasdaq100','tsxci','sp100', 'financials','learning']
    sdic = {}
    for name in db_name_list:
        Config.DB_NAME = name
        db = Db(Config)
        s = db.session()
        sdic.update({name:s})

    analysis_hub('turnover', sdic=sdic)

    for name, s in sdic.items():
        s.close()
        logger.info("Session closed: '%s' " % s.bind.url.database)
