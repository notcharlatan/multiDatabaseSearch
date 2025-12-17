# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 权限验证管理核心
适配动态数据库增删，统一管理权限验证流程
"""
from cae_multi_db.config.user_config import (
    update_user_db_auth, get_verified_dbs, get_db_info_by_id, get_db_auth_by_id
)
from cae_multi_db.utils.auth_utils import verify_db_auth


class DBAuthManager:
    """数据库权限验证管理器（适配动态数据库）"""

    def __init__(self, st_session):
        self.st_session = st_session  # 接收Streamlit会话状态

    def verify_db_auth(self, db_id, user, password, port):
        """
        验证指定数据库的权限，并更新用户配置
        :param db_id: 数据库唯一ID
        :param user: 用户名
        :param password: 密码
        :param port: 端口
        :return: (bool, str) - (是否验证通过，错误/成功信息)
        """
        # 1. 基础校验：数据库是否存在
        db_info = get_db_info_by_id(self.st_session, db_id)
        if not db_info:
            error_msg = f"错误：数据库ID{db_id}不存在"
            print(error_msg)
            return (False, error_msg)

        # 2. 调用工具类验证连接
        is_verified, msg = verify_db_auth(db_id, user, password, port, db_info)

        # 3. 更新用户权限配置（仅验证通过时更新密码，避免存储错误密码）
        if is_verified:
            update_user_db_auth(self.st_session, db_id, user, password, port, is_verified)

        return (is_verified, msg)

    def get_verified_db_list(self):
        """获取所有验证通过的数据库列表"""
        return get_verified_dbs(self.st_session)

    def get_db_auth_config(self, db_id):
        """获取指定数据库的验证配置"""
        return get_db_auth_by_id(self.st_session, db_id)