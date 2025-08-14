from abc import ABC, abstractmethod
from typing import List, Any


class BaseConsumer(ABC):
    """
    批量处理基础代理类
    """
    batch_size: int = 10
    timeout_seconds: int = 10

    def process_batch(self, data_batches: List[Any]) -> bool:
        """
        批量处理数据
        :param data_batches: 数据批次列表
        :return: 处理是否成功
        """
        try:
            # 获取批次中每条记录的内容
            extracted_data = [record.process() for record in data_batches]
            self.insert_to_storage(extracted_data)
            return True
        except Exception as e:
            print("Error processing batch:", e)
            return False

    @abstractmethod
    def insert_to_storage(self, records: List[Any]):
        """
        将数据批量写入存储系统（数据库、消息队列等）
        :param records: 待写入的记录列表
        """
        pass

    async def __call__(self, stream) -> None:
        """
        消费并处理来自异步数据流（例如 Kafka）的消息
        :param stream: 异步消息流对象
        """
        print("start================")
        async for records in stream.take(self.batch_size, self.timeout_seconds):
            self.process_batch(records)
