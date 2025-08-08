import faust
from application.constant import KAFKA_CONFIG
from application.faust_app.test_topic.process import TestHandler
from application.models.test_model import TestData


def init_app():
    """初始化Faust应用"""
    try:
        root_api_router = faust.App(
            KAFKA_CONFIG['test_config']['app_name'],
            broker=KAFKA_CONFIG['test_config']['broker'],

        )
        return root_api_router
    except Exception as e:
        raise


root_router = init_app()
topic = root_router.topic(KAFKA_CONFIG['topic_test'], value_type=TestData)

# 注册处理器
handler = TestHandler(root_router, topic, "kafka_data", ("title", "viewCount", "author"))

if __name__ == '__main__':
    root_router.main()
