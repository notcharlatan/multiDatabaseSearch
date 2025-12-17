# -*- coding: utf-8 -*-
"""PostgreSQL适配器（多线程安全+元信息读取）"""
import psycopg2
import pandas as pd
from cae_multi_db.adapters.base_adapter import BaseDBAdapter

class PGAdapter(BaseDBAdapter):
    """PostgreSQL适配器（多线程安全）"""
    def __init__(self, db_id, db_info, user_auth):
        self.db_id = db_id
        self.db_info = db_info
        self.user_auth = user_auth
        self.conn = None

    def connect(self):
        """建立连接"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_info["host"],
                user=self.user_auth["user"],
                password=self.user_auth["password"],
                port=int(self.user_auth["port"]),
                dbname=self.db_info["database"],
                connect_timeout=5
            )
            return (True, "连接成功")
        except Exception as e:
            error_msg = f"PostgreSQL连接失败：{str(e)}"
            print(error_msg)
            self.close()
            return (False, error_msg)

    def get_all_tables(self):
        """获取所有表名"""
        if not self.connect()[0]:
            return []
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [t[0] for t in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception as e:
            print(f"获取表列表失败：{str(e)}")
            return []

    def get_table_meta(self, table_name, preview_rows=5):
        """获取表元信息"""
        if not self.connect()[0]:
            return {"columns": [], "preview_data": []}
        try:
            cursor = self.conn.cursor()
            # 获取列名
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table_name,))
            columns = [col[0] for col in cursor.fetchall()]
            # 获取预览数据
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {preview_rows}")
            preview_data = cursor.fetchall()
            cursor.close()
            return {
                "columns": columns,
                "preview_data": preview_data
            }
        except Exception as e:
            print(f"获取{table_name}元信息失败：{str(e)}")
            return {"columns": [], "preview_data": []}

    def _search_single_table(self, table_name, keyword):
        """检索单个表"""
        if not self.conn:
            if not self.connect()[0]:
                return pd.DataFrame()
        meta = self.get_table_meta(table_name)
        columns = meta["columns"]
        if not columns:
            return pd.DataFrame()
        # 构建SQL
        col_str = ", ".join([f"COALESCE({col}::text, '')" for col in columns])
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
            df["_db_id"] = self.db_id
            df["_db_alias"] = self.db_info.get("db_alias", self.db_id)
            df["_table"] = table_name
            return df
        except Exception as e:
            print(f"检索{table_name}失败：{str(e)}")
            return pd.DataFrame()

    def search(self, keyword, enabled_tables):
        """执行检索"""
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