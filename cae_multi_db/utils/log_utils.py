# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 操作日志工具
会话级存储操作日志，用于前端展示，程序关闭后清空
"""
import time

def init_logger():
    """
    初始化日志列表（会话级）
    :return: list - 日志列表，每个元素为字典：{"time": 时间, "content": 内容}
    """
    return []

def add_log(logger, content):
    """
    添加操作日志
    :param logger: 日志列表（由init_logger初始化）
    :param content: 日志内容
    """
    log_item = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "content": content
    }
    logger.append(log_item)
    # 限制日志数量，最多保留50条
    if len(logger) > 50:
        logger.pop(0)

def clear_log(logger):
    """
    清空日志
    :param logger: 日志列表
    """
    logger.clear()