# -*- coding: utf-8 -*-
"""
检索引擎核心（使用单线程避免SessionState访问问题）
"""
import pandas as pd
from cae_multi_db.adapters.mysql_adapter import MySQLAdapter
from cae_multi_db.adapters.pg_adapter import PGAdapter
from cae_multi_db.config.user_config import get_db_info_by_id, get_db_auth_by_id, get_enabled_tables


class CAESearchEngine:
    """多数据库检索引擎（单线程版本）"""

    def __init__(self, st_session):
        self.st_session = st_session
        self.adapter_map = {
            "mysql": MySQLAdapter,
            "postgresql": PGAdapter
        }

    def _get_adapter_instance(self, db_id):
        """获取数据库适配器实例（单线程安全）"""
        db_info = get_db_info_by_id(self.st_session, db_id)
        if not db_info:
            return None

        user_auth = get_db_auth_by_id(self.st_session, db_id)
        if not user_auth or not user_auth.get("is_verified", False):
            return None

        db_type = db_info["db_type"]
        adapter_class = self.adapter_map.get(db_type)
        if not adapter_class:
            return None

        return adapter_class(db_id, db_info, user_auth)

    def _single_db_search(self, db_id, keyword):
        """单个数据库检索（单线程执行）"""
        adapter = self._get_adapter_instance(db_id)
        if not adapter:
            return pd.DataFrame()

        try:
            # 获取启用的表
            enabled_tables = get_enabled_tables(self.st_session, db_id)
            if not enabled_tables:
                return pd.DataFrame()

            # 执行检索
            result_df = adapter.search(keyword, enabled_tables)
            return result_df
        except Exception as e:
            print(f"数据库{db_id}检索异常：{str(e)}")
            return pd.DataFrame()
        finally:
            if adapter:
                adapter.close()

    def search_all_enabled_dbs(self, keyword):
        """检索所有启用且验证通过的数据库（单线程版本）"""
        # 获取所有验证通过的数据库
        verified_dbs = [
            db["db_id"] for db in self.st_session.get("dynamic_dbs", [])
            if db.get("enable_search", False)
               and self.st_session.get("user_auth", {}).get(db["db_id"], {}).get("is_verified", False)
        ]

        if not verified_dbs:
            return pd.DataFrame()

        # 单线程依次检索每个数据库
        all_results = []
        for db_id in verified_dbs:
            result_df = self._single_db_search(db_id, keyword)
            if not result_df.empty:
                all_results.append(result_df)

        # 合并结果
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            return combined_df
        return pd.DataFrame()