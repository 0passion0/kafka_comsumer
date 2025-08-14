import json
from typing import Any, List, Type, Optional
from abc import ABC, abstractmethod
import faust
from fasttransform import Transform, Pipeline

from application.models.kafka_models.information_data_structure import InformationDataStructure
from pydantic import BaseModel


class BasePipline(Transform):
    """
    根据目标类型自动处理对象中匹配的字段
    """
    target_type: Type

    def encodes(self, obj):
        for attr_name in getattr(obj, "__annotations__", {}):
            value = getattr(obj, attr_name, None)
            if value is not None and isinstance(value, self.target_type):
                setattr(obj, attr_name, self.apply(value))
        return obj

    def apply(self, value):
        """子类必须实现"""
        raise NotImplementedError


class BaseData(ABC):
    pip_list: Optional[List[Transform]] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    def get(self) -> Any:
        pass

    def process(self) -> Any:
        if self.pip_list:
            pipeline = Pipeline(self.pip_list)
            pipeline(self)

        return self.get()


class InformationtoData(InformationDataStructure, BaseData):

    def get(self):
        """
        获取数据元组，用于后续处理或存储
        Returns:
            tuple: 包含所有信息字段的元组
        """
        return (
            self.uid,  # str
            self.topic,  # str
            self.name,  # str
            self.data_type,  # str
            json.dumps(self.menu_list),  # list
            self.data['info_date'],  # str
            json.dumps(self.data['info_section']),  # dict
            self.data['info_author'],  # str
            self.data['info_source'],  # str
            json.dumps(self.affiliated_data)  # dict
        )
