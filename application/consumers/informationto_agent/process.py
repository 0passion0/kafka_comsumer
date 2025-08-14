from typing import List, Any

from application.consumers.base_agents import BaseConsumer
from application.db.mysql_manager import MySQLTupleModel
from application.utils.decorators import log_execution


class InformationtoConsumer(BaseConsumer):
    sql_model = MySQLTupleModel("info", fields=(
        "uid",
        "topic",
        "name",
        "data_type",
        "menu_list",
        "info_date",
        "info_section",
        "info_author",
        "info_source",
        "affiliated_data",
    ))
    def insert_to_storage(self, records: List[Any]):
        self.sql_model.insert_many(records)
