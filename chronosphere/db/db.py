import os
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import mysql
from sqlalchemy import *
import logging
logger = logging.getLogger('main.db')


class Db():
    def __init__(self, object, db_name=None):
        self.user = object.DB_USER
        self.pwd = object.DB_PASS
        self.port = object.DB_PORT
        self.host = object.DB_HOST
        self.db_name =  object.DB_NAME
        self.engine = create_engine('mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                        .format(self.user, self.pwd, self.host, self.port, self.db_name))

    Model = declarative_base()
    Column = Column
    String = String
    Integer = Integer
    Float = Float
    Boolean = Boolean
    DateTime = DateTime
    ForeignKey = ForeignKey
    relationship = relationship
    BIGINT = mysql.BIGINT


    def get_engine(self):
        return self.engine


    def create_all(self):
        self.Model.metadata.create_all(self.engine)


    def session(self):
        Session = sessionmaker(bind=self.engine)
        logger.info("Session into: '%s'" % self.db_name)
        return Session()
