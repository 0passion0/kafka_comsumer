import faust


class TestData(faust.Record, serializer='json'):
    """
    定义 Kafka 消息结构，与 JSON 格式一致。
    """
    title: str
    author: str
    viewCount: int

    def get(self):
        return tuple([self.title, self.author, self.viewCount])
