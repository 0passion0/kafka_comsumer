import faust
from abc import ABC, abstractmethod
from typing import ClassVar, Dict, Any, Type, Iterable, Tuple, Union

# ---------------------------------------------------------------------
# 处理器基类（使用 process 替代 __call__）
# ---------------------------------------------------------------------


class Processor(ABC):
    """
    抽象处理器基类，所有自定义处理器应继承此类并实现 ``process``。

    :param record: 将被处理的记录对象（实例）
    :returns: None
    """
    @abstractmethod
    def process(self, record: Any) -> None:
        """
        对 record 就地修改或处理。

        :param record: 待处理的记录对象
        :returns: None
        """
        raise NotImplementedError


class StripTitle(Processor):
    """
    去除 title 字段首尾空白。
    """

    def process(self, record: Any) -> None:
        if getattr(record, "title", None) is not None:
            record.title = record.title.strip()


class NormalizeAuthor(Processor):
    """
    将 author 名称规范化（首字母大写）。
    """

    def process(self, record: Any) -> None:
        if getattr(record, "author", None) is not None:
            record.author = record.author.title()


class ClampViewCount(Processor):
    """
    限制 viewCount 不为负数（将负数修正为 0）。
    """

    def process(self, record: Any) -> None:
        if getattr(record, "viewCount", None) is not None:
            try:
                record.viewCount = max(0, int(record.viewCount))
            except Exception:
                record.viewCount = 0

# ---------------------------------------------------------------------
# 抽象数据基类
# ---------------------------------------------------------------------


class BaseData(faust.Record, ABC, serializer='json'):
    """
    抽象数据基类，提供静态配置型数据处理管道（类似 Scrapy 的 ITEM_PIPELINES 思路），
    并要求子类实现 get() 以返回最终数据。

    :cvar custom_pipeline: 类级别管道配置，格式为 {priority: ProcessorClass | Processor | callable}
                          使用 ClassVar 避免被 Faust 当成字段解析。
    """
    custom_pipeline: ClassVar[Dict[int,
                                   Union[Type[Processor], Processor, callable]]] = {}

    def __post_init__(self) -> None:
        """
        在对象初始化后自动运行合并后的管道。

        :returns: None
        """
        self._run_pipeline()

    def _collect_pipelines(self) -> Iterable[Tuple[int, Union[Type[Processor], Processor, callable]]]:
        """
        从继承链（MRO）中收集所有类的 custom_pipeline 配置，并按类定义顺序合并为列表。

        :returns: List of (priority, processor) 元组
        """
        items = []
        # reversed MRO: 从基类到当前类遍历（这样父类的配置先收集）
        for cls in reversed(self.__class__.mro()):
            cp = getattr(cls, "custom_pipeline", None)
            if isinstance(cp, dict):
                for priority, processor in cp.items():
                    items.append((int(priority), processor))
        return items

    def _run_pipeline(self) -> None:
        """
        按优先级执行收集到的处理器：
        - 如果配置项是类（type），实例化后使用（无参构造）。
        - 如果配置项是实例或函数，按以下顺序调用：
            1. 优先调用 ``process(record)``（若存在）。
            2. 回退到直接可调用（callable），即调用 ``processor(record)``（支持函数或实现 __call__ 的对象）。
        - 抛出 TypeError 当配置项不合规时。

        :returns: None
        """
        items = self._collect_pipelines()
        for _, processor in sorted(items, key=lambda x: x[0]):
            # 避免修改配置对象，局部创建实例（若传入的是类）
            if isinstance(processor, type):
                proc_obj = processor()
            else:
                proc_obj = processor

            # 优先使用显式的 process 方法（更语义化）
            if hasattr(proc_obj, "process") and callable(getattr(proc_obj, "process")):
                proc_obj.process(self)
            # 回退到可调用对象 —— 支持函数或实现 __call__ 的实例
            elif callable(proc_obj):
                proc_obj(self)
            else:
                raise TypeError(f"Invalid processor configured: {processor!r}")

    @abstractmethod
    def get(self) -> Any:
        """
        抽象方法：返回最终处理后的数据。子类必须实现。

        :returns: 子类定义的最终数据格式
        """
        raise NotImplementedError

# ---------------------------------------------------------------------
# 示例子类：TestData（使用 process，兼容函数或实例）
# ---------------------------------------------------------------------


class TestData(BaseData):
    """
    示例数据模型：在类静态变量 custom_pipeline 中以类引用或实例引用的方式配置处理器，
    所有处理在 get() 之前完成。
    """
    title: str
    author: str
    viewCount: int

    custom_pipeline = {
        10: StripTitle,          # 传类，会在执行时实例化
        20: NormalizeAuthor(),   # 传实例，直接可调用（优先调用 process）
        30: ClampViewCount       # 传类
    }

    def get(self) -> tuple:
        """
        返回最终处理后的元组数据。

        :returns: (title, author, viewCount)
        """
        return (self.title, self.author, self.viewCount)


# ---------------------------------------------------------------------
# 运行示例
# ---------------------------------------------------------------------
if __name__ == "__main__":
    raw = TestData(title="  hello world  ", author="john doe", viewCount=-5)
    print(raw.get())  # 期望: ('hello world', 'John Doe', 0)
