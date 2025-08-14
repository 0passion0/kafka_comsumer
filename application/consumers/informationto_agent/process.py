from application.consumers.base_agents import BaseConsumer
from application.piplines.information_into_pipline import InformationIntoPipeline
from application.piplines.tranlate_data_pipline import TranlateDatePipeline


class InformationtoConsumer(BaseConsumer):
    pip_list = [TranlateDatePipeline(), InformationIntoPipeline()]
