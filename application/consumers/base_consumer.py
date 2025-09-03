class BaseConsumer:
    """
    消费者基类，定义消费者的基本结构和处理流程
    """
    pipelines = []

    def transform(self, transform_data):
        """
        对数据应用管道处理
        """
        for pipeline in self.pipelines:
            transform_data = pipeline.apply(transform_data)
        self.write(transform_data)

    def write(self, transform_data):
        """
        写入处理后的数据
        """
        return transform_data
