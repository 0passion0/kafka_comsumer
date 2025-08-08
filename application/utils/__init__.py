"""
工具包初始化文件
"""

from .logger import get_logger, LoggerManager

# 创建全局日志管理器实例
logger_manager = LoggerManager()

__all__ = ['get_logger', 'LoggerManager', 'logger_manager']