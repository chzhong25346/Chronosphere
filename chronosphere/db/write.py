import pandas as pd
import logging
from sqlalchemy import exc
logger = logging.getLogger('main.write')

# def create_table(engine, model_list):
#     Obj = model_list[0]
#     Obj.__table__.create(engine)


def bulk_save(session, model_list):
    try:
        session.bulk_save_objects(model_list)
        session.commit()
    except exc.IntegrityError:
        session.rollback()
        raise foundDup('Found duplicate')
    except:
        session.rollback()
        raise writeError('Writing failed')


def insert_onebyone(session, model_list):
    for idx, model in enumerate(model_list):
        try:
            session.add(model)
            session.commit()
        except Exception as e:
            session.rollback()
            if idx > 0:
                raise foundDup('%s row(s) written' % str(idx))
            else:
                raise foundDup('Found duplicate')



class writeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class foundDup(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
