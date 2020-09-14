import logging
from .turnover import tor_analysis
from .lines import line_analysis
logger = logging.getLogger('main.turnover')


def analysis_hub(type, sdic=None, s=None):
    if type == 'turnover':
        logger.info('Run Task: [Turnover Analysis]')
        tor_analysis(sdic)
    elif type == 'lines':
        logger.info('Run Task: [Line Analysis]')
        line_analysis(sdic)
