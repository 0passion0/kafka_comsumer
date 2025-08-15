import json

from application.db.mysql_manager import MysqlDBDataStream
from application.pipelines.base_pipeline import BasePipeline
from application.utils.decorators import log_execution


class InformationIntoPipeline(BasePipeline):
    """
    将信息对象转换为数据库可插入格式并批量写入 MySQL。
    """
    sql_model = MysqlDBDataStream(
        "info",
        fields=(
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
        ),
    )

    def apply(self, value):
        """
        将单个信息对象转换为数据库字段元组。
        """
        return (
            value.uid,  # str
            value.topic,  # str
            value.name,  # str
            value.data_type,  # str
            json.dumps(value.menu_list),  # list -> str
            value.data['info_date'],  # str
            json.dumps(value.data['info_section']),  # dict -> str
            value.data['info_author'],  # str
            value.data['info_source'],  # str
            json.dumps(value.affiliated_data),  # dict -> str
        )

    @log_execution
    def apply_batch(self, value):
        """
        批量插入数据库。
        """
        self.sql_model.insert_many(value)
        return value
