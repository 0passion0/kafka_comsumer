import pathway as pw

from application.consumers.information_consumer import InformationConsumer
from application.utils import get_logger

# 创建日志记录器
logger = get_logger(__name__)
InformationConsumer()
if __name__ == '__main__':
    # 注意得现有生产者才能消费
    logger.info("正在启动pathway应用")
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
