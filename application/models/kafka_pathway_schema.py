import pathway as pw


class KafkaPathwaySchema(pw.Schema):
    '''
    Kafka消息的Pathway数据模式定义
    
    该类定义了从Kafka消费的数据在Pathway中的结构化表示，
    包含消息的基本信息、数据内容和相关元数据。
    '''
    uid: str = pw.column_definition(primary_key=True)  # 消息唯一标识符，主键
    topic: str  # 消息所属的Kafka主题
    name: str  # 数据模式名称
    created_at: str  # 消息创建时间戳
    data_type: str  # 数据类型标识
    tag_values: str = pw.column_definition(default_value='')  # 标签值集合，以字符串形式存储
    data: dict  # 消息主体数据内容
    metadata: dict = pw.column_definition(default_value={})  # 消息元数据信息
    affiliated_data: list = pw.column_definition(default_value=[])  # 附加数据列表
