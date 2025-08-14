# Kafka Consumer 项目

基于 Faust 框架的 Kafka 消费者项目，用于从 Kafka 主题中消费消息并批量处理后存储到目标库（目前只有 MySQL 数据库）。

## 项目结构

```
.
├── application/                 # 应用核心代码目录
│   ├── db/                     # 数据库操作模块
│   │   ├── __init__.py
│   │   ├── mongodb_manager.py  # MongoDB数据库管理
│   │   ├── mysql_manager.py    # MySQL数据库管理
│   │   └── redis_manager.py    # Redis数据库管理
│   ├── faust_app/              # Faust应用相关代码
│   │   ├── test_topic/         # 特定主题处理逻辑
│   │   │   └── process.py      # 消息处理实现
│   │   ├── __init__.py
│   │   └── agents.py           # Faust agents定义
│   ├── models/                 # 数据模型定义
│   │   ├── __init__.py
│   │   └── test_model.py       # 测试数据模型
│   ├── utils/                  # 工具类模块
│   │   ├── __init__.py
│   │   └── logger.py           # 日志管理模块
│   ├── __init__.py
│   ├── constant.py             # 项目常量配置
│   ├── router.py               # 路由配置
│   └── settings.py             # 项目设置
├── extend/                     # 扩展目录
├── faust_mysql_batch-data/     # Faust数据目录
├── runtime/                    # 运行时目录
│   └── log/                    # 日志文件目录
├── requirements.txt            # 项目依赖
└── README.md                   # 项目说明文档
```

## 功能介绍

本项目主要实现以下功能：

1. 从 Kafka 主题中消费消息
2. 对消息进行批量处理（默认批量大小为10000条，超时时间为10秒）
3. 将处理后的数据批量写入 MySQL 数据库

## 技术栈

- Python 3.12 Faust - Python流处理库
- Kafka - 消息队列
- MySQL - 关系型数据库
- Redis - 内存数据库
- MongoDB - 文档数据库

## 配置说明

### Kafka 配置

在 [constant.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/constant.py) 文件中配置 Kafka
相关信息：

```python
KAFKA_CONFIG = {
    "test_config": {
        'broker': 'kafka_models://127.0.0.1:19092',
        'app_name': 'faust_mysql_batch'
    },
    "topic_test": "temp4"
}
```

### 数据库配置

在 [constant.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/constant.py) 文件中配置各类数据库连接信息：

1. MySQL 配置：

```python
MYSQL_DATABASES = {
    "default": {
        "type": "mysql",
        'user': '',
        'password': '',
        'host': '192.168.1.245',
        'port': 3306,
        'database': 'raw_data',
        "charset": "utf8mb4"
    },
    "tlg": {
        "type": "mysql",
        'user': 'root',
        'password': '',
        'host': '127.0.0.1',
        'port': 3306,
        'database': 'mydata',
        "charset": "utf8mb4"
    },
}
```

2. MongoDB 配置
3. Redis 配置

### 批处理配置

在 [settings.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/settings.py) 文件中配置批处理参数：

```python
BATCH_CONFIG = {
    'size': 10000,  # 批处理大小
    'timeout': 10.0  # 超时时间(秒)
}
```

## 核心模块说明

### Faust 应用 (faust_app/)

Faust 应用模块包含消息处理的逻辑：

- [agents.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/faust_app/agents.py): 定义 Faust
  agents，负责注册消息处理器
- [test_topic/process.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/faust_app/test_topic/process.py):
  实现具体的消息处理逻辑

### 数据库操作 (db/)

数据库操作模块封装了各种数据库的连接和操作：

- [mysql_manager.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/db/mysql_manager.py): MySQL
  数据库连接池管理和操作封装
- [mongodb_manager.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/db/mongodb_manager.py):
  MongoDB 数据库操作封装
- [redis_manager.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/db/redis_manager.py): Redis
  数据库操作封装

### 数据模型 (models/)

数据模型模块定义了 Kafka 消息的数据结构：

- [test_model.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/models/test_model.py): 定义测试数据模型

### 工具类 (utils/)

工具类模块提供通用功能：

- [logger.py](file:///D:/company_project/kafka_comsumer/kafka_comsumer/application/utils/logger.py): 日志管理模块，统一处理项目中的日志记录

## 运行方式

1. 安装依赖：

```bash
pip install -r requirements.txt
```

2. 启动 Faust 应用：

```bash
faust --debug -A application.router:root_router worker -l info
```

## 日志管理

项目使用统一的日志管理模块，所有日志将记录到 [runtime/log/](file:///D:/company_project/kafka_comsumer/kafka_comsumer/runtime/log)
目录下，按日期分割日志文件。日志格式如下：

```
[时间戳] [日志级别] [模块名称] [函数名:行号] 消息内容
```
