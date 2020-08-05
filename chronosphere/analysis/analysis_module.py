import logging
from .turnover import tor_analysis
logger = logging.getLogger('main.turnover')


def analysis_hub(type, sdic=None, s=None):
    if type == 'turnover':
        tor_analysis(sdic)
