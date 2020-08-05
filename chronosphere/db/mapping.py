import pandas as pd
import datetime as dt
from ..utils.fetch import fetch_index#, get_daily_adjusted
from ..utils.util import gen_id
from ..models import Index, Quote, Report, Transaction, Holding, Eia_price, Eia_storage
import logging
logger = logging.getLogger('main.mapping')
