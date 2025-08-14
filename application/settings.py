# 批处理配置
BATCH_CONFIG = {
    'size': 1,
    'timeout': 1.0
}

# Kafka配置
KAFKA_CONFIG = {
    "test_config": {
        'broker': 'kafka://127.0.0.1:19092,kafka://127.0.0.1:19096,kafka://127.0.0.1:19100',
        'app_name': 'faust_mysql_batch'
    },
    "topic_test": "temp4"
}

# 数据库配置
MYSQL_DATABASES = {
    "xxx": {
        "type": "mysql",
        'user': 'medpeer',
        'password': 'medpeer',
        'host': '192.168.1.245',
        'port': 3306,
        'database': 'raw_data',
        "charset": "utf8mb4"
    },
    "default": {
        "type": "mysql",
        'user': 'root',
        'password': 'Btlg2002',
        'host': '127.0.0.1',
        'port': 3306,
        'database': 'mydata',
        "charset": "utf8mb4"
    },
}

MONGODB_DATABASES = {
    "default": {
        "type": "mongodb",
        'user': 'medpeer',
        'password': 'medpeer',
        'auth_source': 'admin',  # 认证数据库（必须与用户创建库一致）
        'host': '192.168.1.245',
        'port': 27017,
        'database': 'raw_data',
        "charset": "utf8mb4"
    },
}
REDIS_DATABASES = {
    "default": {
        "type": "redis",
        'password': 'medpeer',
        'host': '101.200.62.36',
        'port': 6379,
        'database': 1,
    }
}
