from abc import abstractmethod
from typing import List
from fasttransform import Transform


class BasePipeline(Transform):
    """
    抽象基类，定义数据管道的基础处理逻辑。
    子类必须实现 apply 和 apply_batch 方法。
    """

    def encodes(self, obj):
        """
        异步处理输入序列，将每个元素应用 apply 方法，
        然后批量应用 apply_batch 方法。
        """
        # 对每个元素单独处理
        for i in range(len(obj)):
            obj[i] = self.apply(obj[i])

        # 批量处理
        self.apply_batch(obj)
        return obj

    @abstractmethod
    def apply(self, value):
        """
        对单个元素进行处理。
        子类必须实现。
        """
        pass

    @abstractmethod
    def apply_batch(self, value: List) -> List:
        """
        对整个序列进行批量处理。
        子类必须实现。
        """
        return value
