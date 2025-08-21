from typing import List

from application.db import get_database_connection
from application.db.info.information_attachments import InformationAttachments
from application.db.info.information_list import InformationList
from application.db.info.information_tags_relationship import InformationTagsRelationship
from application.models.kafka_models.information_data_structure import InformationDataStructure
from application.pipelines.base_pipeline import BasePipeline
from application.utils.decorators import log_execution


class InformationIntoPipeline(BasePipeline):
    """
    将信息对象转换为数据库可插入格式并批量写入 MySQL。
    """
    change_data_structure = False

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
                "metadata": {"info_author": value.data.info_author, "info_source": value.data.info_source},
                # 信息元数据
                # "source_id": find_source_id(value.metadata['details_page']),
            },
            'information_tagging_relationships': {
                'information_id': value.uid,  # 信息唯一标识
                'tag_code': value.tag_code,  # 标签code
                'tag_value': value.tag_values,  # 标签值
            },
            "information_attachment": [
                {
                    "information_id": value.uid,  # 信息唯一标识
                    "attachment_name": link.get("accessory_name", ''),  # 附件名称
                    "attachment_address": link.get("accessory_url", ''),  # 附件存储地址（OSS地址）
                    "display_order": index + 1  # 展示顺序（从1开始）
                }
                for index, link in enumerate(value.affiliated_data.link_data)
            ],
            'information_section':[

            ]
        }

    @log_execution
    @get_database_connection().atomic() #保证事务
    def apply_batch(self, value: List) -> List:
        """
        批量插入数据库。
        """
        into_information_list = []
        into_information_tagging_relationships = []
        into_information_attachment = []

        for item in value:
            into_information_list.append(item['information_list'])
            into_information_tagging_relationships.append(item['information_tagging_relationships'])
            into_information_attachment.extend(item['information_attachment'])

        InformationList.insert_many(into_information_list).execute()  # 信息列表
        InformationTagsRelationship.insert_many(into_information_tagging_relationships).execute()  # 信息标签关系
        InformationAttachments.insert_many(into_information_attachment).execute()  # 信息附件

        return value
