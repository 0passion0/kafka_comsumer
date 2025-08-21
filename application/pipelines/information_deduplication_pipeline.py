from typing import List

from application.db.info.information_list import InformationList
from application.pipelines.base_pipeline import BasePipeline


class informationDeduplicationPipeline(BasePipeline):

    def apply_batch(self, value: List) -> List:
        # 查询所有 information_id
        all_ids = InformationList.select(InformationList.information_id)
        # 转成 set
        id_set = {record.information_id for record in all_ids}
        return [item for item in value if item.uid not in id_set]

