# Kafka Consumer 项目（考虑是否可以将生产者也使用faust一起管理 ，但是单个机器会影响性能）

基于 Faust 框架的 Kafka 消费者项目，用于从 Kafka 主题中消费消息并批量处理后存储到目标库。


## 项目结构
### 为什么使用faust

- 1、Faust 提供了 agent + stream + table 等机制，直接支持批量收集和异步处理，开发成本低。kafka-python 实现同样的批处理逻辑需要手动管理缓冲区、定时器和异步逻辑，代码会复杂很多。
- 2、Faust 支持类似 路由机制和 topic 分组，可以轻松按类型分发到不同 agent。使用 kafka-python，必须自己维护 topic/消息类型的映射，代码量大且容易出错
- 3、Faust 内部封装了 Kafka 的连接逻辑，只需配置 broker 地址，框架会自动管理消费者组、分区订阅和再平衡。kafka-python 需要自己手动管理 KafkaConsumer 或 KafkaProducer，还要处理重连、心跳和分区再平衡等复杂逻辑

```
.
├── application/                    # 应用核心代码目录
│   ├── consumers/                 # 消息消费者模块
│   │   ├── base_consumer.py       # 消费者基类
│   │   ├── information_consumer/  # 资讯类消息消费者
│   │   │   ├── __init__.py
│   │   │   └── process.py         # 资讯类消息处理逻辑
│   │   └── __init__.py
│   ├── db/                        # 数据库操作模块
│   │   ├── __init__.py
│   │   └── mysql_manager.py       # MySQL数据库管理
│   ├── models/                    # 数据模型定义
│   │   ├── __init__.py
│   │   └── kafka_models/          # Kafka消息数据模型
│   │       ├── base_data_structure.py           # 基础数据结构
│   │       ├── information_data_structure.py     # 资讯类数据结构
│   │       └── __init__.py
│   ├── piplines/                  # 数据处理管道模块
│   │   ├── base_pipline.py        # 管道基类
│   │   ├── information_into_pipline.py    # 资讯数据入库管道
│   │   ├── tranlate_data_pipline.py       # 数据转换管道
│   │   └── __init__.py
│   ├── utils/                     # 工具类模块
│   │   ├── __init__.py
│   │   ├── decorators.py          # 装饰器工具
│   │   └── logger.py              # 日志管理模块
│   ├── __init__.py
│   ├── constant.py                # 项目常量配置
│   ├── router.py                  # 路由配置
│   └── settings.py                # 项目设置
├── runtime/                       # 运行时目录
│   └── log/                       # 日志文件目录
├── test/                          # 测试目录
│   └── main.py                    # 测试入口文件
├── requirements.txt               # 项目依赖
└── README.md                     # 项目说明文档
```

## 功能介绍

本项目主要实现以下功能：

1. 从 Kafka 主题中消费消息
2. 对消息进行批量处理（默认批量大小为10条，超时时间为10秒）
3. 将处理后的数据批量写入 MySQL 数据库

## 技术栈

- Python 3.12
- Faust - Python流处理库
- Kafka - 消息队列
- MySQL - 关系型数据库

流程说明：

1. Kafka生产者将消息发送到指定主题
2. Faust应用作为消费者从Kafka主题订阅消息
3. 根据消息类型路由到对应的消费者（如InformationConsumer）
4. 消费者使用批处理机制收集消息（默认每10条或超时10秒）
5. 批量数据通过Pipeline管道依次处理：
   - TranlateDatePipeline：处理日期格式
   - InformationIntoPipeline：将数据转换为数据库格式并批量插入
6. 处理后的数据批量写入MySQL数据库

## 运行方式

```bash
# 安装依赖
pip install -r requirements.txt

# 启动Faust应用
faust --debug -A application.router:root_router worker -l info
```

## 扩展说明

项目采用模块化设计，支持灵活扩展：

1. 添加新的消息类型：
   - 在 [application/models/kafka_models/](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/models/kafka_models) 下创建新的数据结构模型
   - 在 [application/consumers/](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/consumers/) 下创建对应的消费者
   - 在 [application/piplines/](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/piplines/) 下创建对应的处理管道

2. 添加新的数据库支持：
   - 在 [application/db/](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/db/) 下创建新的数据库管理模块
   - 修改 [application/settings.py](file:///D:/company_project/kafka_comsumer/kakfa_comsumer/application/settings.py) 添加数据库配置

3. 自定义处理流程：
   - 继承 [BasePipeline](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/piplines/base_pipline.py) 创建新的处理管道
   - 在消费者中配置管道执行顺序
```
