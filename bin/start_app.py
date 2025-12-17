#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CAE多数据库联合检索工具 - 启动脚本
软件工程实践课程交付件
"""
import subprocess
import sys
import os

#避免streamlit包的欢迎和隐私提示
os.environ['STREAMLIT_GATHER_USAGE_STATS'] = 'false'
os.environ['STREAMLIT_SHOW_WARNING_ON_DIRECT_EXECUTION'] = 'false'

# 切换到项目根目录（解决路径问题）
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT_DIR)

def main():
    """一键启动前端界面"""
    try:
        print("=== CAE多数据库联合检索工具启动中 ===")
        print(f"项目根目录：{ROOT_DIR}")
        # 启动Streamlit前端（指定端口，避免冲突）
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", "cae_multi_db/ui/main_ui.py", "--server.port", "8501"],
            cwd=ROOT_DIR,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"启动失败：{e}")
        sys.exit(1)
    except Exception as e:
        print(f"未知错误：{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()