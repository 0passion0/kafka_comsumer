import copy
from abc import abstractmethod
from typing import List, Any
from fasttransform import Transform

from application.models.kafka_models.base_data_structure import DataStructure


class BasePipeline(Transform):
    """
    抽象基类，定义数据管道的基础处理逻辑。
    子类必须实现 apply 和 apply_batch 方法。
    """
    change_data_structure = True  # 是否在通过管道后变更数据（默认是，在入库或者一些操作是会变更数据结构但是后续还要用到原结构时使用）

    def encodes(self, obj: List[DataStructure]):
        """
        异步处理输入序列，将每个元素应用 apply 方法，
        然后批量应用 apply_batch 方法。
        """
        copy_obj = []
        if not self.change_data_structure:
            # 深拷贝，确保源数据不被修改
            copy_obj = copy.deepcopy(obj)

        # 对每个元素单独处理
        for i in range(len(obj)):
            obj[i] = self.apply(obj[i])

        # 批量处理
        obj = self.apply_batch(obj)

        if not self.change_data_structure:
            return copy_obj
        return obj

    @abstractmethod
    def apply(self, value: DataStructure):
        """
        对单个元素进行处理。
        子类必须实现。
        """
        return value

    @abstractmethod
    def apply_batch(self, value: List) -> List:
        """
        对整个序列进行批量处理。
        子类必须实现。
        """
        return value
