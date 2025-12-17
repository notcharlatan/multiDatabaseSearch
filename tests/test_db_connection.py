# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 数据库连接测试用例
用于验证数据库适配器和权限验证功能的基础可用性
"""
import sys
import os

# 添加项目根目录到Python路径（解决导入问题）
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cae_multi_db.core.auth_manager import DBAuthManager
from cae_multi_db.core.search_engine import CAESearchEngine
from cae_multi_db.utils.auth_utils import verify_mysql_connection, verify_postgresql_connection


def test_mysql_connection():
    """测试MySQL基础连接（需手动修改测试参数）"""
    print("=== 测试MySQL连接 ===")
    # 请根据本地环境修改以下参数
    host = "localhost"
    user = "root"
    password = "2003.guo"
    port = 3306
    database = "preprae"

    result = verify_mysql_connection(host, user, password, port, database)
    if result:
        print("✅ MySQL连接测试成功")
    else:
        print("❌ MySQL连接测试失败")
    return result


def test_postgresql_connection():
    """测试PostgreSQL基础连接（需手动修改测试参数）"""
    print("=== 测试PostgreSQL连接 ===")
    # 请根据本地环境修改以下参数
    host = "localhost"
    user = "postgres"
    password = "123456"
    port = 5432
    database = "preprae"

    result = verify_postgresql_connection(host, user, password, port, database)
    if result:
        print("✅ PostgreSQL连接测试成功")
    else:
        print("❌ PostgreSQL连接测试失败")
    return result


def test_auth_manager():
    """测试权限验证管理器"""
    print("=== 测试权限验证管理器 ===")
    auth_manager = DBAuthManager()
    # 测试获取验证通过的数据库列表（初始为空）
    verified_dbs = auth_manager.get_verified_db_list()
    print(f"当前验证通过的数据库：{verified_dbs}")
    print("✅ 权限验证管理器初始化成功")
    return True


def test_search_engine():
    """测试检索引擎初始化"""
    print("=== 测试检索引擎初始化 ===")
    search_engine = CAESearchEngine()
    print("✅ 检索引擎初始化成功")
    return True


if __name__ == "__main__":
    """执行所有基础测试用例"""
    print("===== 开始执行CAE多数据库检索工具测试用例 =====")
    test_results = []
    test_results.append(test_mysql_connection())
#    test_results.append(test_postgresql_connection())
    test_results.append(test_auth_manager())
    test_results.append(test_search_engine())

    # 统计测试结果
    success_count = sum(test_results)
    total_count = len(test_results)
    print(f"\n===== 测试完成：成功{success_count}/{total_count} =====")
    if success_count == total_count:
        print("🎉 所有测试用例执行成功！")
    else:
        print("⚠️ 部分测试用例执行失败，请检查数据库配置！")