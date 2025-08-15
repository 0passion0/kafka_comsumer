import faust

from application.consumers.information_consumer.process import InformationConsumer
from application.models.kafka_models.information_data_structure import InformationDataStructure
from application.settings import KAFKA_CONFIG, TOPIC_CONFIG
from application.utils import get_logger
from application.utils.logger import add_faust_handlers

# 创建日志记录器
logger = get_logger(__name__)


class FaustAppManager:
    """
    Faust 应用管理器，用于集中管理 Faust 应用的初始化、主题注册及代理函数绑定。

    Attributes
    ----------
    app : faust.App
        Faust 应用实例。
    """

    def __init__(self):
        """
        初始化 FaustAppManager 并创建 Faust 应用实例。
        """
        self.app = None
        self._init_app()

    def _init_app(self):
        """
        初始化 Faust 应用，设置应用名称和 broker 地址，并为应用添加日志处理器。

        Raises
        ------
        Exception
            初始化失败时抛出异常。
        """
        try:
            self.app = faust.App(
                id=KAFKA_CONFIG['default']['app_name'],
                broker=KAFKA_CONFIG['default']['broker'],
            )
            # 为 Faust 应用添加日志处理器
            add_faust_handlers()
            logger.info(
                "Faust应用初始化完成，应用名: %s, broker地址: %s",
                KAFKA_CONFIG['default']['app_name'],
                KAFKA_CONFIG['default']['broker']
            )
        except Exception as e:
            logger.error("Faust应用初始化失败: %s", str(e))
            raise

    def register_agent(self, topic_name, process_agent, value_type=None):
        """
        注册主题及对应的处理函数代理。

        Parameters
        ----------
        topic_name : str
            Kafka 主题名称。
        process_agent : callable
            处理函数或 Faust Agent，用于处理该主题消息。
        value_type : Optional[faust.Record]
            消息的数据结构类型，可选，用于序列化/反序列化。

        Notes
        -----
        会自动将处理函数绑定到 Faust 应用的 topic 上，并记录日志。
        """
        topic = self.app.topic(topic_name, value_type=value_type)
        logger.info("成功注册主题: %s", topic_name)

        self.app.agent(topic)(process_agent)
        logger.info("成功注册处理函数到主题: %s", topic.get_topic_name())

    def get_app(self):
        """
        获取 Faust 应用实例。

        Returns
        -------
        faust.App
            当前 Faust 应用实例。
        """
        return self.app


# ----------------- 应用启动 -----------------
# 初始化应用管理器
app_manager = FaustAppManager()

# 注册 Kafka 主题及处理函数
app_manager.register_agent(
    TOPIC_CONFIG['default']['topic'],
    InformationConsumer(),  # 订阅主题消费类
    value_type=InformationDataStructure  # 接受消息的数据结构类型
)

# 获取应用实例作为根路由
root_router = app_manager.get_app()

if __name__ == '__main__':
    # 注意得现有生产者才能消费
    logger.info("正在启动Faust应用")
    root_router.main()
