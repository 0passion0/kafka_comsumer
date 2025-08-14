from typing import Type, Union, Sequence
from fasttransform import Transform


class BasePipeline(Transform):
    """
    根据目标类型自动处理对象中匹配的字段。
    可匹配多个类型，对匹配的字段值执行 apply 操作。
    """
    target_types: Sequence[Type]

    def encodes(self, obj):
        """
        遍历对象注解的字段，若字段值类型符合 target_types 列表中的任意一种，
        则调用 apply 进行处理，并支持返回多个值。
        """
        annotations = getattr(obj, "__annotations__", {})
        for attr_name in annotations:
            value = getattr(obj, attr_name, None)
            if value is not None and isinstance(value, tuple(self.target_types)):
                new_value = self.apply(value)
                # 支持 apply 返回单值或 (name, value) 对列表
                if isinstance(new_value, tuple) and len(new_value) > 1:
                    # 返回多个值时，假设为 (attr_name1, value1), (attr_name2, value2) 形式
                    for sub_attr, sub_val in new_value:
                        setattr(obj, sub_attr, sub_val)
                else:
                    setattr(obj, attr_name, new_value)
        return obj

    def apply(self, value):
        """
        子类必须实现的处理方法。

        :param value: 匹配类型的字段值
        :return: 单值或多个 (字段名, 新值) 的元组
        """
        raise NotImplementedError
