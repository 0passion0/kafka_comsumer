"""
日志管理模块
提供统一的日志记录功能，支持文件和控制台输出
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional


class LoggerManager:
    """
    日志管理器，统一管理系统中的日志处理
    """

    def __init__(self):
        self._loggers = {}

    def get_logger(self, name: str, level: int = logging.INFO) -> logging.Logger:
        """
        获取指定名称的日志记录器

        Args:
            name: 日志记录器名称
            level: 日志级别，默认为INFO

        Returns:
            logging.Logger: 日志记录器实例
        """
        if name in self._loggers:
            return self._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # 避免重复添加处理器
        if logger.handlers:
            self._loggers[name] = logger
            return logger

        # 创建日志目录
        log_dir = os.path.join('runtime', 'log')
        os.makedirs(log_dir, exist_ok=True)

        # 创建文件处理器，以日期命名日志文件
        log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
        file_handler = logging.FileHandler(
            os.path.join(log_dir, log_filename), 
            encoding='utf-8'
        )
        file_handler.setLevel(level)

        # 创建控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # 创建格式器并添加到处理器
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s:%(lineno)d] %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 添加处理器到日志记录器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        self._loggers[name] = logger
        return logger


# 全局日志管理实例
logger_manager = LoggerManager()


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    获取日志记录器的便捷函数

    Args:
        name: 日志记录器名称
        level: 日志级别

    Returns:
        logging.Logger: 日志记录器实例
    """
    return logger_manager.get_logger(name, level)