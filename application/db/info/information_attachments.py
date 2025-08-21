from peewee import (CharField, IntegerField,
                    AutoField, SQL
                    )
from playhouse.mysql_ext import JSONField

from application.db import get_database_connection
from application.db.base_model import BaseModel


class InformationAttachments(BaseModel):
    """资讯附件表"""
    list_id = AutoField()  # 自增主键
    attachment_id = CharField(index=True)  # 资讯附件ID，可为空
    information_id = CharField(index=True, null=True)  # 资讯ID，外键
    attachment_name = CharField()  # 附件名称
    attachment_address = JSONField()  # 附件存储地址（OSS地址）
    display_order = IntegerField()  # 展示顺序
    is_deleted = IntegerField(constraints=[SQL("DEFAULT 0")])  # 是否删除：0-否，1-是

    class Meta:
        table_name = 'information_attachments'
        database = get_database_connection('default')  # 使用默认数据库