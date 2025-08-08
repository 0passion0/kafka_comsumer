# -*- coding: utf-8 -*-

"""
# @Time    : 2025/2/25 13:58
# @User  : Mabin
# @Description  :MySQL数据库操作工具类（单例、连接池）
"""
from typing import Tuple, List

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB
from threading import Lock

from application.constant import MYSQL_DATABASES


class MySQLManager:
    """
    MySQL数据库管理类
    :author Mabin
    使用示例(查询)：
    test_model = MySQLManager()
    with test_model.mysql_pool.connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users")
        print(cursor.fetchall())

    使用示例(插入)：
    test_model = MySQLManager()
    with test_model.mysql_pool.connection() as conn, conn.cursor() as cursor:
        cursor.execute("UPDATE users SET login_count = login_count + 1 WHERE id = %s",(1,))
        # 手动提交事务
        conn.commit()
    """
    _instances = {}  # 存储不同URI的连接实例
    _lock = Lock()  # 线程安全锁

    def __new__(cls, connect_key="default"):
        """
        单例核心逻辑
        :author Mabin
        :param str connect_key:数据库连接标识
        """
        if connect_key not in cls._instances:
            # 不存在对应连接标识的实例
            with cls._lock:
                # 实例化
                instance = super().__new__(cls)

                # 初始化数据库连接
                instance._init_connection(connect_key=connect_key)

                # 存储
                cls._instances[connect_key] = instance

        # 返回实例
        return cls._instances[connect_key]

    def _init_connection(self, connect_key="default"):
        """
        初始化新连接(连接池)
        :author Mabin
        :param str connect_key:数据库连接标识
        :return:
        """
        # 获取数据库链接配置
        connect_config = MYSQL_DATABASES.get(connect_key, None)
        if not connect_config:
            raise Exception(f"创建MySQL数据库连接时，未查询到数据库链接配置！{connect_key}")

        # 创建连接池（自动管理连接池）
        self.mysql_pool = PooledDB(
            creator=pymysql,  # 使用 pymysql 驱动
            maxconnections=10,  # 连接池最大连接数
            mincached=3,  # 初始化时预创建的连接数
            blocking=True,  # 无可用连接时阻塞等待（默认False会抛错）
            host=connect_config["host"],
            user=connect_config["user"],
            password=connect_config["password"],
            database=connect_config["database"],
            charset=connect_config["charset"],
            autocommit=False,  # 关闭自动提交（手动控制事务）
            cursorclass=DictCursor  # 返回字典格式的游标
        )


class MySQLTupleModel:
    """
    极简元组插入器
    fields 是有序字段名元组，insert 时按同顺序传入值即可。
    """
    db = MySQLManager('tlg')

    def __init__(self, table: str, fields: Tuple[str, ...]):
        self.table = table
        self.fields = fields

        # 预生成 SQL：INSERT INTO `t` (`f1`,`f2`,...) VALUES (%s,%s,...)
        columns = ",".join(f"`{f}`" for f in fields)
        placeholders = ",".join(["%s"] * len(fields))
        self._sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"

    # ------------- API -------------
    def insert_one(self, record: Tuple) -> int:
        """返回新自增 id"""
        conn = self.db.mysql_pool.connection()
        try:
            with conn.cursor() as cur:
                cur.execute(self._sql, record)
                new_id = cur.lastrowid
            conn.commit()
            return new_id
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_many(self, records: List[Tuple]) -> int:
        """返回影响行数"""
        if not records:
            return 0
        conn = self.db.mysql_pool.connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(self._sql, records)
                rows = cur.rowcount
            conn.commit()
            return rows
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ---------------------------------
if __name__ == "__main__":
    # 假设表 user(id, name, age, email)
    user = MySQLTupleModel(
        table="kafka_data",
        fields=("title", "viewCount", "author")
    )

    # 单条
    uid = user.insert_one(("Alice", 18, "a@test.com"))
    print(uid)

    # 批量
    rows = user.insert_many([
        ("Bob", 20, "b@test.com"),
        ("Carol", 22, "c@test.com")
    ])
    print(rows)
