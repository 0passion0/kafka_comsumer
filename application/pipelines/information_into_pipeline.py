import json
import random
import time
from typing import List
from urllib.parse import urlparse

from application.db import get_database_connection
from application.db.info.InformationAttachments import InformationAttachments
from application.db.info.InformationList import InformationList
from application.db.info.InformationSectionList import InformationSectionList
from application.db.info.InformationTagsRelationship import InformationTagsRelationship
from application.db.info.ResourceSource import ResourceSource

from application.models.kafka_models.information_data_structure import InformationDataStructure
from application.pipelines.base_pipeline import BasePipeline
from application.utils.decorators import log_execution


class InformationIntoPipeline(BasePipeline):
    """
    将信息对象转换为数据库可插入格式并批量写入 MySQL。
    """
    change_data_structure = False  # 不改变数据
    # 临时
    tag_code = {
        "information_nsfc": "info_nsfc",  # 标签代码
    }

    def apply(self, value: InformationDataStructure):
        """
        将单个信息对象转换为数据库字段元组。
        """
        return {
            "information_list": {
                "information_id": value.uid,  # 信息唯一标识
                "information_name": {'zh': value.name},  # 信息名称
                "information_description": {'zh': value.data.description},  # 信息描述
                "original_link": value.metadata.details_page,  # 原始链接
                "original_language": value.metadata.marc_code,  # 原始语言
                "publish_date": value.data.info_date,  # 发布时间
                "metadata": {"info_author": value.data.info_author},  # 元数据
                "source_id": self.find_source_id(value.metadata.details_page),  # 来源id
            },
            'information_tagging_relationships': {
                'information_id': value.uid,  # 信息唯一标识
                'tag_code': self.tag_code.get(value.data_type),  # 标签code
                'tag_value': value.tag_values,  # 标签值
            },
            "information_attachment": [
                {
                    "information_id": value.uid,  # 信息唯一标识
                    "attachment_name": link.get("accessory_name", ''),  # 附件名称
                    "attachment_address": link.get("accessory_url", ''),  # 附件存储地址（OSS地址）
                    "display_order": index + 1  # 展示顺序（从1开始）
                }
                for index, link in enumerate(value.link_data)
            ],
            'information_section': [
                {
                    "section_id": f"{value.uid}_{int(time.time())}_{random.randint(1000, 9999)}",
                    "information_id": value.uid,  # 信息唯一标识
                    "section_order": index + 1,  # 展示顺序（从1开始）
                    "title_level": item.get('title_level', 0) or 0,
                    "marc_code": item.get("marc_code"),
                    "src_text": item.get("text_info"),
                    "dst_text": item.get("dst_text"),
                    "media_info": item.get("media_info"),
                    "md5_encode": f"{value.uid}_{int(time.time())}_{random.randint(1000, 9999)}",
                }
                for index, item in enumerate(value.data.info_section)
            ],
        }

    @log_execution
    @get_database_connection().atomic()  # 保证事务
    def apply_batch(self, value: List) -> List:
        """
        批量插入数据库。
        """
        into_information_list = []
        into_information_tagging_relationships = []
        into_information_attachment = []
        into_information_section = []
        for item in value:
            into_information_list.append(item['information_list'])
            into_information_tagging_relationships.append(item['information_tagging_relationships'])
            into_information_attachment.extend(item['information_attachment'])
            into_information_section.extend(item['information_section'])
        InformationList.insert_many(into_information_list).execute()  # 资讯列表
        InformationTagsRelationship.insert_many(into_information_tagging_relationships).execute()  # 资讯标签关系
        InformationAttachments.insert_many(into_information_attachment).execute()  # 资讯附件
        InformationSectionList.insert_many(into_information_section).execute()  # 资讯段落
        return value

    @staticmethod
    def find_source_id(url):
        """
        根据链接查找资源id
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc  # 结果: "www.nsfc.gov.cn"
        record = ResourceSource.get_or_none(ResourceSource.source_main_link == domain)

        return record.source_id
