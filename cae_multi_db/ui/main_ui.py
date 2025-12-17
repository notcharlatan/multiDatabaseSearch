# -*- coding: utf-8 -*-
"""
多数据库全列检索系统前端（元信息展示+检索启用配置+多线程安全）
适配Streamlit 1.52.1
"""
import streamlit as st
import pandas as pd
import time
from cae_multi_db.core.auth_manager import DBAuthManager
from cae_multi_db.core.search_engine import CAESearchEngine
from cae_multi_db.config.db_config import DB_TYPE_TEMPLATES
from cae_multi_db.config.user_config import (
    init_dynamic_dbs, init_user_auth, add_db_to_list, delete_db_from_list,
    update_db_enable_search, update_table_enable_search, save_table_meta,
    get_enabled_tables
)
from cae_multi_db.adapters.mysql_adapter import MySQLAdapter
from cae_multi_db.adapters.pg_adapter import PGAdapter
from cae_multi_db.utils.export_utils import export_to_csv, export_to_excel
from cae_multi_db.utils.log_utils import init_logger, add_log, clear_log

# ====================== 初始化会话状态 ======================
if "dynamic_dbs" not in st.session_state:
    st.session_state.dynamic_dbs = init_dynamic_dbs()
if "user_auth" not in st.session_state:
    st.session_state.user_auth = init_user_auth()
if "logger" not in st.session_state:
    st.session_state.logger = init_logger()
if "search_result" not in st.session_state:
    st.session_state.search_result = pd.DataFrame()
if "search_history" not in st.session_state:
    st.session_state.search_history = []

# ====================== 初始化核心业务类 ======================
auth_manager = DBAuthManager(st.session_state)
search_engine = CAESearchEngine(st.session_state)
logger = st.session_state.logger


# ====================== 工具函数（读取表元信息） ======================
def load_db_table_meta(db_id):
    """加载数据库的表元信息（主线程执行，避免子线程访问SessionState）"""
    db_info = None
    for db in st.session_state.dynamic_dbs:
        if db["db_id"] == db_id:
            db_info = db
            break
    if not db_info:
        return

    user_auth = st.session_state.user_auth.get(db_id, {})
    if not user_auth.get("is_verified", False):
        return

    # 创建适配器实例（主线程）
    adapter = None
    if db_info["db_type"] == "mysql":
        adapter = MySQLAdapter(db_id, db_info, user_auth)
    elif db_info["db_type"] == "postgresql":
        adapter = PGAdapter(db_id, db_info, user_auth)

    if not adapter:
        return

    # 获取所有表名
    tables = adapter.get_all_tables()
    table_meta = {}
    for table in tables:
        # 获取表元信息
        meta = adapter.get_table_meta(table)
        table_meta[table] = {
            "columns": meta["columns"],
            "preview_data": meta["preview_data"],
            "enable_search": True  # 默认启用表检索
        }
    # 保存元信息到SessionState
    save_table_meta(st.session_state, db_id, table_meta)
    adapter.close()


# ====================== 页面基础配置 ======================
st.set_page_config(
    page_title="多数据库全列检索系统",
    layout="wide",
    page_icon="🔍",
    initial_sidebar_state="collapsed"
)
st.title("🔍 多数据库全列检索系统")
st.divider()

# ====================== 顶部标签页 ======================
tab1, tab2, tab3 = st.tabs(["数据库管理", "一键检索", "操作日志"])

# ====================== 标签页1：数据库管理（核心改造） ======================
with tab1:
    st.subheader("⚙️ 数据库管理（元信息+检索配置）")

    # 新增数据库按钮
    with st.expander("➕ 新增数据库", expanded=False):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            db_type = st.selectbox("数据库类型", options=list(DB_TYPE_TEMPLATES.keys()),
                                   format_func=lambda x: DB_TYPE_TEMPLATES[x]["db_alias"],
                                   key="new_db_type")
            db_alias = st.text_input("数据库别名", value=DB_TYPE_TEMPLATES[db_type]["db_alias"],
                                     key="new_db_alias")
        with col2:
            host = st.text_input("主机地址", value=DB_TYPE_TEMPLATES[db_type]["host"],
                                 key="new_db_host")
            port = st.number_input("端口", value=DB_TYPE_TEMPLATES[db_type]["port"],
                                   min_value=1, max_value=65535, key="new_db_port")
            database = st.text_input("数据库名", value=DB_TYPE_TEMPLATES[db_type]["database"],
                                     key="new_db_name")
            tables = st.text_input("检索表名（留空自动读取所有表）", value=DB_TYPE_TEMPLATES[db_type].get("tables", ""),
                                   key="new_db_tables")
        with col3:
            description = st.text_input("描述", value=DB_TYPE_TEMPLATES[db_type]["description"],
                                        key="new_db_desc")
            if st.button("添加数据库", type="primary", use_container_width=True, key="add_db_btn"):
                new_db = {
                    "db_alias": db_alias,
                    "db_type": db_type,
                    "host": host,
                    "port": port,
                    "database": database,
                    "tables": tables,
                    "description": description,
                    "is_extend": DB_TYPE_TEMPLATES[db_type]["is_extend"]
                }
                db_id = add_db_to_list(st.session_state, new_db)
                st.success(f"✅ {db_alias} 添加成功！ID：{db_id}")
                add_log(logger, f"新增数据库：{db_alias}（{db_type}），ID：{db_id}")
                st.rerun()

    st.divider()

    # 已添加数据库列表（核心改造：一个数据库一个独立展开框）
    st.markdown("### 📦 已添加数据库")
    if st.session_state.dynamic_dbs:
        for db_idx, db in enumerate(st.session_state.dynamic_dbs):
            db_id = db["db_id"]
            auth = st.session_state.user_auth.get(db_id, {})

            # 每个数据库一个独立的展开框（核心修改①）
            with st.expander(f"📦 {db['db_alias']}（{db['db_type']}）", expanded=False):
                # 数据库基本信息 + 操作按钮
                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    st.caption(f"ID：{db_id} | 主机：{db['host']}:{db['port']} | 库名：{db['database']}")
                    st.caption(f"描述：{db['description']}")
                with col2:
                    # 启用检索勾选框
                    enable_search = st.checkbox(
                        "启用检索",
                        value=db.get("enable_search", True),
                        key=f"db_enable_{db_id}",
                        help="勾选后，该数据库会参与跨库检索"
                    )
                    if enable_search != db.get("enable_search", True):
                        update_db_enable_search(st.session_state, db_id, enable_search)
                        st.rerun()
                with col3:
                    if st.button("删除", type="secondary", key=f"del_db_{db_id}", use_container_width=True):
                        delete_db_from_list(st.session_state, db_id)
                        st.success(f"✅ {db['db_alias']} 已删除")
                        add_log(logger, f"删除数据库：{db['db_alias']}（{db_id}）")
                        st.rerun()

                # 1. 连接配置与测试（展开项）
                with st.expander("🔌 连接配置与测试", expanded=False):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    with col1:
                        user = st.text_input("用户名", value=auth.get("user", ""), key=f"user_{db_id}")
                    with col2:
                        pwd = st.text_input("密码", type="password", value=auth.get("password", ""), key=f"pwd_{db_id}")
                    with col3:
                        port = st.number_input("端口", value=auth.get("port", db["port"]),
                                               min_value=1, max_value=65535, key=f"port_{db_id}")
                        if st.button("测试连接", key=f"verify_{db_id}", use_container_width=True):
                            with st.spinner("验证连接中..."):
                                is_valid, msg = auth_manager.verify_db_auth(db_id, user, pwd, port)
                                if is_valid:
                                    st.success(f"✅ {msg}")
                                    add_log(logger, f"验证数据库{db['db_alias']}连接成功：{msg}")
                                    # 加载表元信息
                                    with st.spinner("加载表元信息..."):
                                        load_db_table_meta(db_id)
                                        st.success("✅ 表元信息加载完成")
                                else:
                                    st.error(f"❌ {msg}")
                                    add_log(logger, f"验证数据库{db['db_alias']}连接失败：{msg}")

                # 2. 表结构与数据预览（仅验证通过后显示）
                if auth.get("is_verified", False):
                    with st.expander("📋 表结构与数据预览", expanded=False):
                        table_meta = db.get("table_meta", {})
                        if not table_meta:
                            st.info("点击上方「测试连接」加载表元信息")
                        else:
                            enabled_tables = get_enabled_tables(st.session_state, db_id)
                            st.caption(f"共{len(table_meta)}张表，其中{len(enabled_tables)}张启用检索")

                            for table_name, meta in table_meta.items():
                                # 表名大写+加黑（核心修改②）
                                st.markdown(f"**{table_name.upper()}**")
                                # 列名：大写+加黑展示（核心修改②）
                                st.markdown(f"**列名：** {' | '.join([col.upper() for col in meta['columns']])}")

                                # 前5条数据预览：去掉外层框、列名大写（核心修改②）
                                st.markdown("**前5条数据预览：**")
                                if meta["preview_data"]:
                                    preview_df = pd.DataFrame(meta["preview_data"], columns=meta["columns"])
                                    # 列名转为大写
                                    preview_df.columns = [col.upper() for col in preview_df.columns]
                                    # 展示数据：仅保留基础样式，移除额外框体（适配1.52.1版本）
                                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                                else:
                                    st.info("该表暂无数据")

                                # 检索该表的勾选框
                                table_enable = st.checkbox(
                                    "检索该表",
                                    value=meta.get("enable_search", True),
                                    key=f"table_enable_{db_id}_{table_name}",
                                    help="勾选后，该表会参与检索"
                                )
                                if table_enable != meta.get("enable_search", True):
                                    update_table_enable_search(st.session_state, db_id, table_name, table_enable)
                                    st.rerun()
                                st.divider()
                else:
                    st.info("🔒 请先完成数据库连接验证，查看表结构与数据预览")
        st.divider()
    else:
        st.info("暂无添加的数据库，点击上方「新增数据库」添加")

# ====================== 标签页2：一键检索（核心修改③：删除检索关键词文字） ======================
with tab2:
    st.subheader("🎯 跨库全列检索（所有启用的数据库）")

    # 检索配置
    st.markdown("### ⚙️ 检索配置")
    col1, col2 = st.columns([3, 1])
    with col1:
        keyword = st.text_input(
            label="",  # 核心修改③：删除"输入检索关键词"文字
            placeholder="支持全列模糊检索，例如：材料、Q355B、2003.guo",
            key="search_keyword",
            help="检索所有启用的数据库中所有启用的表"
        )
    with col2:
        search_btn = st.button(
            label="🚀 一键检索所有启用的数据库",
            use_container_width=True,
            type="primary",
            disabled=not keyword
        )

    # 清空结果
    if st.button("🗑️ 清空检索结果", key="clear_result"):
        st.session_state.search_result = pd.DataFrame()
        st.rerun()

    # 执行检索
    if search_btn and keyword:
        add_log(logger, f"用户发起一键检索，关键词：{keyword}")
        with st.spinner("正在检索所有启用的数据库，请稍候..."):
            start_time = time.time()
            # 执行检索（多线程安全）
            result_df = search_engine.search_all_enabled_dbs(keyword)
            end_time = time.time()
            cost_time = round(end_time - start_time, 2)

            # 更新结果
            st.session_state.search_result = result_df
            # 记录历史
            st.session_state.search_history.append({
                "keyword": keyword,
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "count": len(result_df),
                "cost": cost_time
            })
            if len(st.session_state.search_history) > 10:
                st.session_state.search_history.pop(0)
            # 日志
            add_log(logger, f"检索完成：关键词{keyword}，返回{len(result_df)}条结果，耗时{cost_time}秒")

    # 结果展示
    st.markdown("### 📊 检索结果")
    if not st.session_state.search_result.empty:
        result_df = st.session_state.search_result
        # 结果概览
        col_stats1, col_stats2, col_stats3 = st.columns(3)
        with col_stats1:
            st.metric("总结果数", value=len(result_df))
        with col_stats2:
            st.metric("涉及数据库数", value=len(result_df["_db_alias"].unique()))
        with col_stats3:
            st.metric("耗时（秒）", value=st.session_state.search_history[-1]["cost"])

        # 分页
        page_size = st.slider("每页显示条数", 5, 50, 10, key="page_size")
        total_pages = max(1, (len(result_df) - 1) // page_size + 1)
        current_page = st.number_input("页码", 1, total_pages, 1, key="current_page")
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, len(result_df))
        display_df = result_df.iloc[start_idx:end_idx].copy()

        # 关键词高亮
        def highlight_keyword(text, kw):
            if pd.isna(text) or not kw:
                return text
            return str(text).replace(kw, f"**{kw}**")

        for col in display_df.columns:
            if display_df[col].dtype == "object":
                display_df[col] = display_df[col].apply(lambda x: highlight_keyword(x, keyword))

        # 展示表格
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.caption(f"显示第{start_idx + 1}-{end_idx}条，共{len(result_df)}条（第{current_page}/{total_pages}页）")

        # 导出
        st.markdown("### 💾 结果导出")
        col1, col2 = st.columns(2)
        with col1:
            csv_data = export_to_csv(result_df)
            st.download_button(
                "导出CSV", csv_data,
                f"检索结果_{keyword}_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                use_container_width=True
            )
        with col2:
            excel_data = export_to_excel(result_df)
            st.download_button(
                "导出Excel", excel_data,
                f"检索结果_{keyword}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx",
                use_container_width=True
            )
    else:
        if search_btn:
            st.info(f"❌ 未检索到包含「{keyword}」的记录")
        else:
            st.info("请输入关键词，一键检索所有启用的数据库")

# ====================== 标签页3：操作日志 ======================
with tab3:
    st.subheader("📋 操作日志")
    log_content = "\n".join([f"{log['time']} - {log['content']}" for log in logger])
    st.text_area(
        label="日志详情",
        value=log_content if log_content else "暂无操作日志",
        height=400,
        disabled=True
    )
    if st.button("清空日志", key="clear_log"):
        clear_log(logger)
        st.rerun()