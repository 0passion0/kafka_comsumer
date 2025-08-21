from __future__ import annotations  # 允许在类型注解中使用前向引用
import faust
from typing import List, Dict, Any

from application.models.kafka_models.base_data_structure import DataStructure




# ---------- 子 Record：data ----------
class DataPayload(faust.Record):
    """
    核心信息数据结构

    Attributes:
        info_date (str): 信息日期，格式如 'YYYY-MM-DD'
        info_section (List[str]): 资讯正文段落
        info_source (str): 信息来源
        info_author (str): 信息作者
        description (str): 信息的详细描述或摘要
    """
    info_date: str
    info_section: List[str]
    info_source: str
    info_author: str
    description: str


# ---------- 子 Record：metadata ----------
class MetaPayload(faust.Record):
    """
    元数据结构

    Attributes:
        marc_code (str): MARC（Machine-Readable Cataloging）代码
        main_site (str): 主站点或来源网站
        details_page (str): 详细页面URL
    """
    marc_code: str
    main_site: str
    details_page: str


# ---------- 子 Record：affiliated_data ----------
class AffiliatedPayload(faust.Record):
    """
    关联数据结构，用于存储与主信息相关的链接和文件

    Attributes:
        link_data (List[Dict[str, Any]]): 关联链接数据列表，每个字典包含链接相关信息
        files (List[Dict[str, Any]]): 关联文件数据列表，每个字典包含文件相关信息
    """
    link_data: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []


# ---------- 主数据结构：InformationDataStructure ----------
class InformationDataStructure(DataStructure):
    """
    信息数据结构，用于在Kafka中传递结构化的信息数据。

    Attributes:
        data (DataPayload): 核心信息数据
        metadata (MetaPayload): 元数据，包含信息来源和页面信息
        affiliated_data (AffiliatedPayload): 关联数据，如链接和附件文件
    """
    data: DataPayload
    metadata: MetaPayload
    affiliated_data: AffiliatedPayload