# -*- coding: utf-8 -*-
"""MySQL适配器（支持表/列注释读取）"""
import pymysql
import pandas as pd
from cae_multi_db.adapters.base_adapter import BaseDBAdapter

class MySQLAdapter(BaseDBAdapter):
    """MySQL适配器（多线程安全+注释读取）"""
    def __init__(self, db_id, db_info, user_auth):
        self.db_id = db_id
        self.db_info = db_info
        self.user_auth = user_auth
        self.conn = None

    def connect(self):
        """建立MySQL连接"""
        try:
            self.conn = pymysql.connect(
                host=self.db_info["host"],
                user=self.user_auth["user"],
                password=self.user_auth["password"],
                port=int(self.user_auth["port"]),
                database=self.db_info["database"],
                charset="utf8mb4",
                connect_timeout=5
            )
            return (True, "连接成功")
        except Exception as e:
            error_msg = f"MySQL连接失败：{str(e)}"
            print(error_msg)
            self.close()
            return (False, error_msg)

    def get_table_comment(self, table_name):
        """获取表注释"""
        if not self.conn:
            if not self.connect()[0]:
                return ""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                SELECT TABLE_COMMENT 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (self.db_info["database"], table_name))
            comment = cursor.fetchone()[0] if cursor.rowcount > 0 else ""
            cursor.close()
            return comment or f"表 {table_name}"
        except Exception as e:
            print(f"获取{table_name}注释失败：{str(e)}")
            return f"表 {table_name}"

    def get_table_meta(self, table_name, preview_rows=5):
        """获取表的列名/注释/预览数据"""
        if not self.conn:
            if not self.connect()[0]:
                return {"columns": [], "columns_comment": [], "preview_data": []}
        try:
            cursor = self.conn.cursor()
            # 获取列名和注释
            cursor.execute(f"DESCRIBE {table_name}")
            columns_info = cursor.fetchall()
            # 列名 + 注释（无注释显示列名）
            columns = [col[0] for col in columns_info]
            columns_comment = [col[8] if col[8] else col[0] for col in columns_info]
            # 获取预览数据
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {preview_rows}")
            preview_data = cursor.fetchall()
            cursor.close()
            return {
                "columns": columns,          # 原始列名
                "columns_comment": columns_comment,  # 列注释（优先）
                "preview_data": preview_data
            }
        except Exception as e:
            print(f"获取{table_name}元信息失败：{str(e)}")
            return {"columns": [], "columns_comment": [], "preview_data": []}

    def get_all_tables(self):
        """获取数据库中所有表名+注释"""
        if not self.connect()[0]:
            return []
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME, TABLE_COMMENT 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s
            """, (self.db_info["database"],))
            tables = [{"name": t[0], "comment": t[1] or f"表 {t[0]}"} for t in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception as e:
            print(f"获取表列表失败：{str(e)}")
            return []

    def _search_single_table(self, table_name, keyword):
        """检索单个表的所有列"""
        if not self.conn:
            if not self.connect()[0]:
                return pd.DataFrame()
        # 获取表的所有列名
        meta = self.get_table_meta(table_name)
        columns = meta["columns"]
        if not columns:
            return pd.DataFrame()
        # 构建全列模糊检索SQL
        col_str = ", ".join([f"IFNULL({col}, '')" for col in columns])
        sql = f"""
            SELECT * FROM {table_name} 
            WHERE CONCAT_WS(' ', {col_str}) LIKE %s
        """
        search_pattern = f"%{keyword}%"
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (search_pattern,))
            raw_data = cursor.fetchall()
            df = pd.DataFrame(raw_data, columns=columns)
            # 添加元信息
            df["_db_id"] = self.db_id
            df["_db_alias"] = self.db_info.get("db_alias", self.db_id)
            df["_table"] = table_name
            return df
        except Exception as e:
            print(f"检索{table_name}失败：{str(e)}")
            return pd.DataFrame()

    def search(self, keyword, enabled_tables):
        """执行全表检索"""
        if not self.connect()[0]:
            return pd.DataFrame()
        all_results = []
        for table in enabled_tables:
            df = self._search_single_table(table, keyword)
            if not df.empty:
                all_results.append(df)
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            self.close()
            return combined_df
        self.close()
        return pd.DataFrame()

    def close(self):
        """关闭连接"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            finally:
                self.conn = None