from abc import ABC
from typing import List, Any
from fasttransform import Pipeline


class BaseConsumer(ABC):
    """
    批量处理基础代理类
    """
    batch_size: int = 10
    timeout_seconds: int = 10
    pip_list = []

    def __init__(self):
        self.pipeline = Pipeline(self.pip_list)

    def process_batch(self, data_batches: List[Any]) -> bool:
        """
        批量处理数据
        :param data_batches: 数据批次列表
        :return: 处理是否成功
        """
        try:
            # 获取批次中每条记录的内容
            # extracted_data = [record for record in data_batches]
            # print(data_batches)

            return self.pipeline(data_batches)
        except Exception as e:
            print("Error processing batch:", e)
            return False

    async def __call__(self, stream) -> None:
        """
        消费并处理来自异步数据流（例如 Kafka）的消息
        :param stream: 异步消息流对象
        """
        async for records in stream.take(self.batch_size, self.timeout_seconds):
            result = self.process_batch(records)

