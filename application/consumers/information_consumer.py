from application.consumers.base_consumer import BaseConsumer
from application.pipelines.information_pipeline import InformationTransformer
from application.consumers.write.batch_data_observer import BatchDataObserver
from application.models.kafka_pathway_schema import KafkaPathwaySchema
from application.settings import KAFKA_CONFIG
import pathway as pw


class InformationConsumer(BaseConsumer):
    """
    信息消费者类，用于消费和处理信息数据
    """
    pipelines = [InformationTransformer()]

    def __init__(self):
        """
        初始化信息消费者
        """
        input_data = pw.io.kafka.read(
            KAFKA_CONFIG['pathway'],
            topic="temp4",
            format="json",
            schema=KafkaPathwaySchema,  # 定义 schema
        )
        self.transform(input_data)

    def write(self, transform_data):
        """
        将处理后的数据写入观察者
        """
        pw.io.python.write(transform_data, BatchDataObserver(batch_size=10, time_window=5))
