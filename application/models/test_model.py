import faust


class TestData(faust.Record, serializer='json'):
    """
    定义 Kafka 消息结构，与 JSON 格式一致。
    """
    title: str
    author: str
    viewCount: int

    def get(self):
        """
        将对象属性转换为元组格式返回
        
        Returns:
            tuple: 包含title, author, viewCount的元组
        """
        return tuple([self.title, self.author, self.viewCount])