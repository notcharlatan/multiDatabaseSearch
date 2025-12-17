# -*- coding: utf-8 -*-
"""
检索引擎核心（修复多线程SessionState问题）
"""
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from cae_multi_db.adapters.mysql_adapter import MySQLAdapter
from cae_multi_db.adapters.pg_adapter import PGAdapter
from cae_multi_db.config.user_config import (
    get_db_info_by_id, get_db_auth_by_id, get_enabled_tables
)


class CAESearchEngine:
    """多数据库检索引擎（多线程SessionState安全）"""

    def __init__(self, st_session):
        self.st_session = st_session
        self.adapter_map = {
            "mysql": MySQLAdapter,
            "postgresql": PGAdapter
        }

    def _get_adapter_instance(self, db_id):
        """
        获取适配器实例（主线程提取配置，避免子线程访问SessionState）
        :return: 适配器实例 / None
        """
        # 主线程提取配置（深拷贝，多线程安全）
        db_info = get_db_info_by_id(self.st_session, db_id)
        user_auth = get_db_auth_by_id(self.st_session, db_id)

        if not db_info or not user_auth or not user_auth["is_verified"]:
            return None

        # 获取启用的表列表
        enabled_tables = get_enabled_tables(self.st_session, db_id)
        if not enabled_tables:
            return None

        # 创建适配器实例（传递配置，不依赖SessionState）
        db_type = db_info["db_type"]
        adapter_class = self.adapter_map.get(db_type)
        if not adapter_class:
            return None

        adapter = adapter_class(db_id, db_info, user_auth)
        return (adapter, enabled_tables)

    def _single_db_search(self, db_id, keyword):
        """
        单数据库检索（子线程执行，无SessionState依赖）
        :param db_id: 数据库ID
        :param keyword: 检索关键词
        :return: DataFrame
        """
        # 主线程提前准备好配置，子线程直接使用
        adapter_info = self._get_adapter_instance(db_id)
        if not adapter_info:
            return pd.DataFrame()

        adapter, enabled_tables = adapter_info
        try:
            result_df = adapter.search(keyword, enabled_tables)
            return result_df
        except Exception as e:
            print(f"数据库{db_id}检索异常：{str(e)}")
            return pd.DataFrame()

    def search_all_enabled_dbs(self, keyword):
        """检索所有启用且验证通过的数据库"""
        # 主线程获取所有启用的数据库ID（避免子线程访问SessionState）
        from cae_multi_db.config.user_config import get_verified_dbs
        enabled_db_ids = get_verified_dbs(self.st_session)
        if not enabled_db_ids:
            return pd.DataFrame()

        # 多线程并行检索（子线程无SessionState访问）
        all_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_db = {
                executor.submit(self._single_db_search, db_id, keyword): db_id
                for db_id in enabled_db_ids
            }
            # 收集结果
            for future in future_to_db:
                result_df = future.result()
                if not result_df.empty:
                    all_results.append(result_df)

        # 合并结果
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            return combined_df
        return pd.DataFrame()