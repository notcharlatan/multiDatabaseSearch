# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 数据库权限验证工具
适配动态数据库类型，支持MySQL/PostgreSQL/Qdrant（计划支持）
"""
import pymysql
import psycopg2


def verify_mysql_connection(host, user, password, port, database):
    """验证MySQL数据库连接，返回（是否成功，错误信息）"""
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=int(port),
            database=database,
            charset="utf8mb4",
            connect_timeout=5
        )
        conn.close()
        return (True, "连接成功")
    except Exception as e:
        error_msg = f"MySQL连接失败：{str(e)}"
        print(error_msg)
        return (False, error_msg)


def verify_postgresql_connection(host, user, password, port, database):
    """验证PostgreSQL数据库连接，返回（是否成功，错误信息）"""
    try:
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            port=int(port),
            dbname=database,
            connect_timeout=5
        )
        conn.close()
        return (True, "连接成功")
    except Exception as e:
        error_msg = f"PostgreSQL连接失败：{str(e)}"
        print(error_msg)
        return (False, error_msg)


def verify_qdrant_connection(host, user, password, port, database):
    """验证Qdrant连接（计划支持，暂返回True）"""
    print(f"Qdrant连接验证：暂未实现，默认返回成功")
    return (True, "Qdrant为计划支持功能，暂不验证连接")


def verify_db_auth(db_id, user, password, port, db_info):
    """统一验证数据库权限（适配动态数据库），返回（是否成功，错误信息）"""
    host = db_info["host"]
    database = db_info["database"]
    db_type = db_info["db_type"]

    if db_type == "mysql":
        return verify_mysql_connection(host, user, password, port, database)
    elif db_type == "postgresql":
        return verify_postgresql_connection(host, user, password, port, database)
    elif db_type == "qdrant":
        return verify_qdrant_connection(host, user, password, port, database)
    else:
        error_msg = f"不支持的数据库类型：{db_type}"
        return (False, error_msg)