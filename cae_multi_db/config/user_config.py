# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 用户动态配置（支持元信息缓存+检索启用状态）
"""
import copy

def init_dynamic_dbs():
    """初始化动态数据库列表（新增启用检索/元信息缓存字段）"""
    from cae_multi_db.config.db_config import DEFAULT_DBS
    default_dbs = copy.deepcopy(DEFAULT_DBS)
    for db in default_dbs:
        db["enable_search"] = True  # 默认启用检索
        db["table_meta"] = {}       # 表元信息缓存：{table_name: {"columns": [], "preview_data": [], "enable_search": True}}
    return default_dbs

def init_user_auth():
    """初始化用户权限配置"""
    auth_dict = {}
    from cae_multi_db.config.db_config import DEFAULT_DBS
    for db in DEFAULT_DBS:
        auth_dict[db["db_id"]] = {
            "user": "",
            "password": "",
            "port": db["port"],
            "is_verified": False
        }
    return auth_dict

def add_db_to_list(st_session, db_info):
    """新增数据库到动态列表（初始化新增字段）"""
    import time
    db_id = f"{db_info['db_type']}_{int(time.time())}"
    db_info["db_id"] = db_id
    db_info["enable_search"] = True  # 默认启用检索
    db_info["table_meta"] = {}       # 初始化表元信息
    st_session["dynamic_dbs"].append(db_info)
    # 初始化权限配置
    st_session["user_auth"][db_id] = {
        "user": "",
        "password": "",
        "port": db_info["port"],
        "is_verified": False
    }
    return db_id

def delete_db_from_list(st_session, db_id):
    """从动态列表删除数据库"""
    st_session["dynamic_dbs"] = [db for db in st_session["dynamic_dbs"] if db["db_id"] != db_id]
    if db_id in st_session["user_auth"]:
        del st_session["user_auth"][db_id]

def update_user_db_auth(st_session, db_id, user, password, port, is_verified):
    """更新指定数据库的权限配置"""
    if db_id in st_session["user_auth"]:
        st_session["user_auth"][db_id].update({
            "user": user,
            "password": password,
            "port": port,
            "is_verified": is_verified
        })

def update_db_enable_search(st_session, db_id, enable):
    """更新数据库的检索启用状态"""
    for idx, db in enumerate(st_session["dynamic_dbs"]):
        if db["db_id"] == db_id:
            st_session["dynamic_dbs"][idx]["enable_search"] = enable
            break

def update_table_enable_search(st_session, db_id, table_name, enable):
    """更新表的检索启用状态"""
    for idx, db in enumerate(st_session["dynamic_dbs"]):
        if db["db_id"] == db_id:
            if table_name in db["table_meta"]:
                db["table_meta"][table_name]["enable_search"] = enable
            break

def save_table_meta(st_session, db_id, table_meta):
    """保存数据库的表元信息"""
    for idx, db in enumerate(st_session["dynamic_dbs"]):
        if db["db_id"] == db_id:
            db["table_meta"] = table_meta
            break

def get_verified_dbs(st_session):
    """获取所有验证通过且启用检索的数据库ID"""
    verified = []
    for db in st_session["dynamic_dbs"]:
        db_id = db["db_id"]
        if db["enable_search"] and st_session["user_auth"].get(db_id, {}).get("is_verified", False):
            verified.append(db_id)
    return verified

def get_db_info_by_id(st_session, db_id):
    """根据ID获取数据库基础信息（深拷贝，避免子线程修改）"""
    for db in st_session["dynamic_dbs"]:
        if db["db_id"] == db_id:
            return copy.deepcopy(db)
    return None

def get_db_auth_by_id(st_session, db_id):
    """根据ID获取数据库权限信息（深拷贝）"""
    auth = st_session["user_auth"].get(db_id, None)
    if auth:
        return copy.deepcopy(auth)
    return None

def get_enabled_tables(st_session, db_id):
    """获取数据库中启用检索的表列表"""
    enabled_tables = []
    db_info = get_db_info_by_id(st_session, db_id)
    if db_info and "table_meta" in db_info:
        for table_name, meta in db_info["table_meta"].items():
            if meta.get("enable_search", True):
                enabled_tables.append(table_name)
    return enabled_tables