from typing import List

from application.db.mysql_manager import MySQLTupleModel
from application.utils import get_logger

# 创建日志记录器
logger = get_logger(__name__)


class TestHandler:
    def __init__(self, table, fields):
        self.sql_model = MySQLTupleModel(table=table, fields=fields)
        logger.info("TestHandler初始化完成，表名: %s", table)

    def batch_handle(self, data_batches: List) -> bool:
        try:
            buffer = [r.get() for r in data_batches]

            self.sql_model.insert_many(buffer)  # 批量写入数据库

            logger.info(f"批次首行数据展示 {data_batches[0].get()}", )
            logger.info("批量处理完成，共处理 %d 条数据", len(data_batches))
            return True
        except Exception as e:
            logger.error("批量处理失败: %s", str(e))
            return False
