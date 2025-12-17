# -*- coding: utf-8 -*-
"""适配器基类（多线程安全）"""
from abc import ABC, abstractmethod

class BaseDBAdapter(ABC):
    """所有数据库适配器的基类（抽象类）"""
    def __init__(self, db_id, db_info, user_auth):
        self.db_id = db_id
        self.db_info = db_info  # 深拷贝后的配置，多线程安全
        self.user_auth = user_auth  # 深拷贝后的权限，多线程安全
        self.conn = None

    @abstractmethod
    def connect(self):
        """建立连接，返回(bool, msg)"""
        pass

    @abstractmethod
    def get_all_tables(self):
        """获取所有表名"""
        pass

    @abstractmethod
    def get_table_meta(self, table_name, preview_rows=5):
        """获取表元信息"""
        pass

    @abstractmethod
    def search(self, keyword, enabled_tables):
        """执行检索"""
        pass

    @abstractmethod
    def close(self):
        """关闭连接"""
        pass