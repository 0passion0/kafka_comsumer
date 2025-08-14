import json

from fasttransform import Transform

from application.db.mysql_manager import MySQLTupleModel
from pydantic import BaseModel, Field, model_validator


class InformationIntoPipeline(Transform):
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

    # def insert_to_storage(self, records: List[Any]):
    #     self.sql_model.insert_many(records)
    def apply(self, value):
        return (
            value.uid,  # str
            value.topic,  # str
            value.name,  # str
            value.data_type,  # str
            json.dumps(value.menu_list),  # list
            value.data['info_date'],  # str
            json.dumps(value.data['info_section']),  # dict
            value.data['info_author'],  # str
            value.data['info_source'],  # str
            json.dumps(value.affiliated_data),  # dict
        )

    def encodes(self, obj):
        for i in range(len(obj)):
            obj[i] = self.apply(obj[i])

        self.sql_model.insert_many(obj)
        return obj
