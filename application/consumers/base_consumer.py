from abc import ABC
from fasttransform import Pipeline

from application.settings import BATCH_CONFIG
from application.utils import get_logger

logger = get_logger(__name__)

class BaseConsumer(ABC):
    """
    抽象基类，用于批量消费数据流并通过预定义的 Pipeline 处理数据。

    Attributes
    ----------
    batch_size : int
        每次批量处理的数据条数，从配置 BATCH_CONFIG 获取。
    timeout_seconds : int
        批量获取数据时的超时时间（秒），从配置 BATCH_CONFIG 获取。
    pip_list : list
        包含 Pipeline 步骤的列表，可在子类中定义具体处理流程。
    pipeline : Pipeline
        FastTransform Pipeline 实例，用于处理每批数据。
    """

    batch_size = BATCH_CONFIG['size']
    timeout_seconds = BATCH_CONFIG['timeout']
    pip_list = []

    def __init__(self):
        """
        初始化 BaseConsumer 实例，创建 Pipeline 对象。
        """
        self.pipeline = Pipeline(self.pip_list)

    async def __call__(self, stream):
        """
        异步调用方法，从给定的数据流中按批次读取数据并处理。

        Parameters
        ----------
        stream : AsyncIterable
            异步数据流对象，必须实现 `take(batch_size, timeout_seconds)` 方法，
            返回数据记录列表。

        Returns
        -------
        None
        """
        logger.info(f"开始消费数据流，批次大小={self.batch_size}，超时时间={self.timeout_seconds}秒")
        async for records in stream.take(self.batch_size, self.timeout_seconds):
            logger.info(f"接收到数据，共 {len(records)} 条记录")
            try:
                result = self.pipeline(records)
                logger.info(f"批次处理成功，处理结果条数: {len(result)}")
            except Exception as e:
                logger.error(f"批次处理出错: {e}", exc_info=True)