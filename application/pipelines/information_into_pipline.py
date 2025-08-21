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

    def apply(self, value: InformationDataStructure):
        """
        将单个信息对象转换为数据库字段元组。
        """
        return {
            "information_list": {
                "information_id": value.uid,
                "information_name": {'zh': value.name},
                "information_description": {'zh': value.data['description']},
                "original_link": value.metadata['details_page'],
                "original_language": value.metadata['marc_code'],
                "publish_date": value.data['info_date'],
                "metadata": {"info_author": value.data['info_author'], "info_source": value.data['info_source']},
                "source_id": '',
            },
            'information_tagging_relationships': {
                'information_id': value.uid,
                'tag_code': value.tag_code,
                'tag_value': value.tag_values,
            },
            "information_attachment": [
                {
                    "information_id": value.uid,  # 信息唯一标识
                    "attachment_name": link.get("accessory_name", ''),  # 附件名称
                    "attachment_address": link.get("accessory_url", ''),  # 附件存储地址（OSS地址）
                    "display_order": index + 1  # 展示顺序（从1开始）
                }
                for index, link in enumerate(value.affiliated_data["link_data"])
            ]
        }

    @log_execution
    @get_database_connection().atomic()
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

        InformationList.insert_many(into_information_list).execute()
        InformationTagsRelationship.insert_many(into_information_tagging_relationships).execute()
        InformationAttachments.insert_many(into_information_attachment).execute()

        return value
