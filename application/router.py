import faust
from application.constant import KAFKA_CONFIG
from application.faust_app.agents import register_test_agent
from application.models.test_model import TestData
from application.utils import get_logger
from application.utils.logger import add_faust_handlers

# 创建日志记录器
logger = get_logger(__name__)


def init_app():
    """初始化Faust应用"""
    try:
        root_api_router = faust.App(
            id=KAFKA_CONFIG['test_config']['app_name'],
            broker=KAFKA_CONFIG['test_config']['broker'],
            # consumer_enable_auto_commit=True,
        )
        # 为Faust应用添加日志处理器
        add_faust_handlers()

        logger.info("Faust应用初始化完成，应用名: %s, broker地址: %s",
                    KAFKA_CONFIG['test_config']['app_name'],
                    KAFKA_CONFIG['test_config']['broker'])
        return root_api_router
    except Exception as e:
        logger.error("Faust应用初始化失败: %s", str(e))
        raise


root_router = init_app()

# 注册处理器
topic = root_router.topic(KAFKA_CONFIG['topic_test'], value_type=TestData)  # 创建主题绑定数据模型
test_agent = register_test_agent(root_router, topic)  # 注册测试主题的处理agent
logger.info("测试处理器已注册，主题: %s", KAFKA_CONFIG['topic_test'])

if __name__ == '__main__':
    logger.info("正在启动Faust应用")
    root_router.main()
