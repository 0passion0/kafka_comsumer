from typing import List

from application.db.mysql_manager import MySQLTupleModel
from application.settings import BATCH_CONFIG


class TestHandler:
    def __init__(self, table, fields):
        self.sql_model = MySQLTupleModel(table=table, fields=fields, connect_key='tlg')

    def batch_handle(self, data_batches: List) -> bool:
        try:

            buffer = [(r.title, r.viewCount, r.author) for r in data_batches]
            self.sql_model.insert_many(buffer)
            return True
        except Exception as e:
            return False
