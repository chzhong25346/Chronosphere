import os


class Config():
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_PORT = os.environ.get('DB_PORT')
    DB_HOST = os.environ.get('DB_HOST')
    DB_NAME = None
    TS_KEY = os.environ.get('TS_KEY')
    EMAIL_USER = os.environ.get('EMAIL_USER')
    EMAIL_PASS = os.environ.get('EMAIL_PASS')
    EMAIL_TO = os.environ.get('EMAIL_TO')
