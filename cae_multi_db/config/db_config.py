# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 数据库配置模板+动态管理基础
支持MySQL/PostgreSQL/Qdrant（计划支持），适配任意表名/列名
"""

# 数据库类型配置模板（定义支持的数据库类型及默认值）
DB_TYPE_TEMPLATES = {
    "mysql": {
        "db_type": "mysql",
        "db_alias": "MySQL数据库",
        "host": "localhost",
        "port": 3306,
        "database": "preprae",  # 默认适配你的测试库
        "tables": "material",   # 默认适配你的测试表（多表用逗号分隔）
        "description": "支持全表全列模糊检索",
        "is_extend": False
    },
    "postgresql": {
        "db_type": "postgresql",
        "db_alias": "PostgreSQL数据库",
        "host": "localhost",
        "port": 5432,
        "database": "preprae",
        "tables": "material",
        "description": "支持全表全列模糊检索",
        "is_extend": False
    },
    "qdrant": {
        "db_type": "qdrant",
        "db_alias": "Qdrant向量数据库（计划支持）",
        "host": "localhost",
        "port": 6333,
        "database": "cae_vector",
        "collection": "cae_model_feature",
        "description": "存储CAE模型特征向量（计划支持）",
        "is_extend": True
    }
}

# 初始化动态数据库列表的默认值（适配你的测试库）
DEFAULT_DBS = [
    {
        "db_id": "mysql_default",
        **DB_TYPE_TEMPLATES["mysql"]
    },
    {
        "db_id": "pg_default",
        **DB_TYPE_TEMPLATES["postgresql"]
    }
]