# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 数据库适配器模块
采用适配器模式统一不同数据库的检索接口
包版本：v1.0.1
"""
__version__ = "1.0.1"

# 导出所有适配器类，方便核心模块调用
from .base_adapter import BaseDBAdapter
from .mysql_adapter import MySQLAdapter
from .pg_adapter import PGAdapter
from .qdrant_adapter import QdrantAdapter