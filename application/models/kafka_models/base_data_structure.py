import faust
import json


class DataStructure(faust.Record):
    """
    :param uid: 唯一标识
    :param topic: 主题
    :param name: 名称
    :param created_at: 创建时间
    :param data_type: 数据类型标识
    :param tag_code: 标签代码
    :param tag_values: 标签值
    """
    uid: str
    topic: str
    name: str
    created_at: str
    data_type: str
    tag_code: str
    tag_values: str

    def to_json(self, **kwargs) -> str:
        """
        将模型序列化为 JSON 字符串。

        修复：原实现返回 bytes，这里保证返回 str 并默认禁止 ASCII 转义以保留 Unicode 可读性。

        :return: JSON bytes
        """
        return json.dumps(self.asdict(), ensure_ascii=False, **kwargs).encode("utf-8")

