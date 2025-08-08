import json
from kafka import KafkaConsumer


class KafkaMessageConsumer:
    """
    Kafka 消息消费者封装类，用于从指定主题消费 JSON 消息。
    """

    def __init__(self, bootstrap_servers, topic, group_id='default_group', auto_offset_reset='earliest'):
        """
        初始化 Kafka 消费者。

        :param bootstrap_servers: Kafka 服务地址列表，例如 ['localhost:9092']
        :param topic: 要订阅的 Kafka 主题名称
        :param group_id: 消费组 ID
        :param auto_offset_reset: 无初始偏移时的行为，可选值为 'earliest' 或 'latest'
        """
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            group_id=group_id,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )


    def consume_messages(self):
        """
        持续消费 Kafka 消息并处理。
        """
        print(f"[Kafka] 开始监听主题：{self.topic}")
        try:
            for message in self.consumer:
                # 获取消息内容
                msg_value = message.value
                print(f"[Kafka] 接收到消息: {msg_value}")
                # 可在此添加自定义处理逻辑
        except KeyboardInterrupt:
            print("[Kafka] 接收到中断信号，停止消费。")
        except Exception as e:
            print(f"[Kafka] 消费过程中发生异常: {e}")
        finally:
            self.close()

    def close(self):
        """
        关闭消费者连接。
        """
        self.consumer.close()
        print("[Kafka] 消费者连接已关闭。")


def main():
    """
    Kafka 消息消费主函数。
    """
    # Kafka 消费配置
    kafka_config = {
        'bootstrap_servers': ['180.76.250.147:19092'],
        'topic': 'temp4',

        'auto_offset_reset': 'latest'  # 或 'latest'
    }

    consumer = KafkaMessageConsumer(**kafka_config)
    consumer.consume_messages()


if __name__ == '__main__':
    main()
