from application.consumers.base_consumer import BaseConsumer
from application.pipelines.information_into_pipline import InformationIntoPipeline
from application.pipelines.tranlate_data_pipline import TranlateDatePipeline


class InformationConsumer(BaseConsumer):
    """
    具体的数据消费者类，继承自 BaseConsumer。
    负责按批次处理信息数据流，并执行以下处理流程：

    1. 格式化日期 (TranlateDatePipeline)
    2. 数据入库 (InformationIntoPipeline)
    3. 可扩展其他后续处理步骤

    Attributes
    ----------
    pipe_list : list
        包含本消费者的处理 Pipeline 列表，按顺序执行。
    """

    # 定义具体处理流程的 Pipeline 顺序
    pipe_list = [TranlateDatePipeline(), InformationIntoPipeline()]
