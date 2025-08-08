from application.faust_app.test_topic.process import TestHandler
from application.settings import BATCH_CONFIG


def register_test_agent(app, topic):
    """
    注册测试主题的处理agent
    :param app: Faust应用实例
    :param topic: 主题对象
    """

    # 创建处理器实例
    handler = TestHandler("kafka_data", ("title", "viewCount", "author"))

    @app.agent(topic)
    async def process(stream):
        """
        处理来自Kafka主题的消息流
        :param stream: 消息流对象
        """
        async for records in stream.take(BATCH_CONFIG['size'], within=BATCH_CONFIG['timeout']):
            # 批量处理消息
            handler.batch_handle(records)

    return process
