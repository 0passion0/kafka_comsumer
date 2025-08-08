from typing import List

from application.db.mysql_manager import MySQLTupleModel
from application.settings import BATCH_CONFIG


class TestHandler:
    def __init__(self, router, topic, table, fields):
        self.router = router
        self.topic = topic
        self.sql_model = MySQLTupleModel(table=table, fields=fields,connect_key='tlg')
        self.register_agent()

    def register_agent(self):
        @self.router.agent(self.topic)
        async def process(stream):
            async for records in stream.take(BATCH_CONFIG['size'], within=BATCH_CONFIG['timeout']):
                self.batch_handle(records)

    def batch_handle(self, data_batches: List) -> bool:
        try:
            buffer = [(r.title, r.viewCount, r.author) for r in data_batches]
            self.sql_model.insert_many(buffer)
            return True
        except Exception as e:
            return False
