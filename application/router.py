import faust

from application.consumers.informationto_agent.process import InformationtoConsumer
from application.models.base_model import InformationtoData
from application.settings import KAFKA_CONFIG
from application.utils import get_logger
from application.utils.logger import add_faust_handlers

# 创建日志记录器
logger = get_logger(__name__)


class FaustAppManager:
    """Faust应用管理器，用于集中管理应用初始化和主题注册"""

    def __init__(self):
        self.app = None
        self._init_app()

    def _init_app(self):
        """初始化Faust应用"""
        try:
            self.app = faust.App(
                id=KAFKA_CONFIG['test_config']['app_name'],
                broker=KAFKA_CONFIG['test_config']['broker'],
            )
            # 为Faust应用添加日志处理器
            add_faust_handlers()
            logger.info("Faust应用初始化完成，应用名: %s, broker地址: %s",
                        KAFKA_CONFIG['test_config']['app_name'],
                        KAFKA_CONFIG['test_config']['broker'])
        except Exception as e:
            logger.error("Faust应用初始化失败: %s", str(e))
            raise

    def register_agent(self, topic_name, process_agent, value_type=None):
        """
        注册处理函数
        
        :param topic: 主题对象
        :param process_func: 处理函数
        """

        topic = self.app.topic(topic_name, value_type=value_type)
        logger.info("成功注册主题: %s", topic_name)

        self.app.agent(topic)(process_agent)
        logger.info("成功注册处理函数到主题: %s", topic.get_topic_name())

    def get_app(self):
        """获取Faust应用实例"""
        return self.app


# 初始化应用管理器
app_manager = FaustAppManager()
app_manager.register_agent('temp4', InformationtoConsumer(), value_type=InformationtoData)
root_router = app_manager.get_app()

if __name__ == '__main__':
    logger.info("正在启动Faust应用")
    root_router.main()
