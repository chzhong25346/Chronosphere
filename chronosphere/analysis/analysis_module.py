import logging
from .turnover import tor_analysis
from .lines import line_analysis
from .gaps import gap_analysis
from .rsi import rsi_prediction
logger = logging.getLogger('main.turnover')


def analysis_hub(type, sdic=None, s=None):
    if type == 'turnover':
        logger.info('Run Task: [Turnover Analysis]')
        tor_analysis(sdic)
    elif type == 'lines':
        logger.info('Run Task: [Line Analysis]')
        line_analysis(sdic)
    elif type == 'gaps':
        logger.info('Run Task: [Gap Analysis]')
        gap_analysis(sdic)
    elif type == 'rsi':
        logger.info('Run Task: [RSI Prediction]')
        rsi_prediction(sdic)
