from __future__ import annotations  # 允许在类型注解中使用前向引用

from dataclasses import Field

import faust
from typing import List, Dict, Any, Optional

from application.models.kafka_models.base_data_structure import DataStructure


# ---------- 子 Record：data ----------
class DataPayload(faust.Record):
    """
    核心信息数据结构

    Attributes:
        info_date (str): 信息日期，格式如 'YYYY-MM-DD'
        info_section (List[str]): 资讯正文段落
        info_author (str): 信息作者
        description (str): 信息的详细描述或摘要
    """
    info_date: Optional[str] = None
    info_section: List[Any] = []
    info_author: Optional[str] = None
    description: Optional[str] = None


# ---------- 子 Record：metadata ----------
class MetaPayload(faust.Record):
    """
    元数据结构

    Attributes:
        marc_code (str): 语言代码
        details_page (str): 详细页面URL
    """
    marc_code: str
    details_page: str


# ---------- 主数据结构：InformationDataStructure ----------
class InformationDataStructure(DataStructure):
    """
    信息数据结构，用于在Kafka中传递结构化的信息数据。

    Attributes:
        data (DataPayload): 核心信息数据
        metadata (MetaPayload): 元数据，包含信息来源和页面信息
    """
    data: DataPayload
    metadata: MetaPayload
