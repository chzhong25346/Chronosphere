from .utils.config import Config
from .utils.fetch import get_daily_adjusted, fetchError
from .utils.util import missing_ticker
from .db.db import Db
from .db.write import bulk_save, insert_onebyone, writeError, foundDup
from .db.mapping import map_index, map_quote, map_fix_quote, map_report
from .db.read import read_ticker, has_index
from .report.report import report
from .simulation.simulator import simulator
from .learning.updateEIA import updateEIA
import logging
import logging.config
import getopt
import time
import math
import os, sys
logging.config.fileConfig('chronosphere/log/logging.conf')
logger = logging.getLogger('main')


def main(argv):
    time_start = time.time()
    try:
        opts, args = getopt.getopt(argv,"u:rse",["update=", "report=", "simulate=", "eia="])
    except getopt.GetoptError:
        print('run.py -u <full|compact|fastfix|slowfix> <csi300>')
        print('run.py -r <csi300>')
        print('run.py -s <csi300>')
        print('run.py -e')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('run.py -u <full|compact|fastfix|slowfix>  <csi300>')
            print('run.py -r <csi300>')
            print('run.py -s <csi300>')
            print('run.py -e')
            sys.exit()

        elif opt in ("-e", "--eia"): # Update EIA data
            eia()

    elapsed = math.ceil((time.time() - time_start)/60)
    logger.info("%s took %d minutes to run" % ( (',').join(argv), elapsed ) )
