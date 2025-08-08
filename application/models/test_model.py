import faust


class TestData(faust.Record):
    """
    定义 Kafka 消息结构，与 JSON 格式一致。
    """
    title: str
    author: str
    viewCount: int