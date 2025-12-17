# -*- coding: utf-8 -*-
"""
检索引擎核心（彻底解决子线程SessionState访问问题）
"""
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from cae_multi_db.adapters.mysql_adapter import MySQLAdapter
from cae_multi_db.adapters.pg_adapter import PGAdapter


class CAESearchEngine:
    """多数据库检索引擎（子线程完全不访问SessionState）"""

    def __init__(self, st_session):
        self.st_session = st_session
        self.adapter_map = {
            "mysql": MySQLAdapter,
            "postgresql": PGAdapter
        }

    def _extract_db_configs(self):
        """
        主线程提取所有启用的数据库配置（打包传递给子线程）
        :return: list - [(db_id, db_info, user_auth, enabled_tables), ...]
        """
        db_configs = []
        # 主线程遍历SessionState，提取所有配置（深拷贝）
        for db in self.st_session.get("dynamic_dbs", []):
            db_id = db["db_id"]
            # 过滤：仅启用检索且验证通过的数据库
            if not db.get("enable_search", False):
                continue
            user_auth = self.st_session["user_auth"].get(db_id, {})
            if not user_auth.get("is_verified", False):
                continue
            # 提取启用的表
            enabled_tables = []
            table_meta = db.get("table_meta", {})
            for table_name, meta in table_meta.items():
                if meta.get("enable_search", True):
                    enabled_tables.append(table_name)
            if not enabled_tables:
                continue
            # 深拷贝配置，避免子线程修改主线程数据
            import copy
            db_configs.append({
                "db_id": db_id,
                "db_info": copy.deepcopy(db),
                "user_auth": copy.deepcopy(user_auth),
                "enabled_tables": copy.deepcopy(enabled_tables)
            })
        return db_configs

    def _single_db_search(self, db_config, keyword):
        """
        子线程检索函数（完全不访问SessionState）
        :param db_config: 主线程打包的配置
        :param keyword: 检索关键词
        :return: DataFrame
        """
        db_id = db_config["db_id"]
        db_info = db_config["db_info"]
        user_auth = db_config["user_auth"]
        enabled_tables = db_config["enabled_tables"]

        # 创建适配器实例
        db_type = db_info["db_type"]
        adapter_class = self.adapter_map.get(db_type)
        if not adapter_class:
            return pd.DataFrame()

        adapter = adapter_class(db_id, db_info, user_auth)
        try:
            # 执行检索
            result_df = adapter.search(keyword, enabled_tables)
            return result_df
        except Exception as e:
            print(f"数据库{db_id}检索异常：{str(e)}")
            return pd.DataFrame()
        finally:
            adapter.close()

    def search_all_enabled_dbs(self, keyword):
        """检索所有启用且验证通过的数据库（主线程统一处理SessionState）"""
        # 1. 主线程提取所有配置（关键：子线程不碰SessionState）
        db_configs = self._extract_db_configs()
        if not db_configs:
            return pd.DataFrame()

        # 2. 多线程并行检索
        all_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交任务：仅传递打包的配置和关键词
            future_to_db = {
                executor.submit(self._single_db_search, config, keyword): config["db_id"]
                for config in db_configs
            }
            # 收集结果
            for future in future_to_db:
                result_df = future.result()
                if not result_df.empty:
                    all_results.append(result_df)

        # 3. 合并结果
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            return combined_df
        return pd.DataFrame()