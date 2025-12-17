#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CAE多数据库联合检索工具 - 桌面版启动脚本
无需浏览器，直接弹出桌面窗口运行
"""
import subprocess
import sys
import os
import threading
import time
import webview  # 桌面内嵌浏览器库

# ====================== 保留原有环境变量（避免Streamlit提示） ======================
os.environ['STREAMLIT_GATHER_USAGE_STATS'] = 'false'
os.environ['STREAMLIT_SHOW_WARNING_ON_DIRECT_EXECUTION'] = 'false'

# ====================== 保留原有路径逻辑（解决项目根目录问题） ======================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT_DIR)

# ====================== 配置项（无需修改） ======================
STREAMLIT_PORT = 8501  # Streamlit服务端口
STREAMLIT_APP_PATH = "cae_multi_db/ui/main_ui.py"  # 你的前端文件路径
WINDOW_TITLE = "CAE多数据库联合检索工具"  # 桌面窗口标题
WINDOW_WIDTH = 1400  # 窗口宽度
WINDOW_HEIGHT = 900  # 窗口高度

def start_streamlit_server():
    """后台启动Streamlit服务（不阻塞主线程）"""
    try:
        # 启动Streamlit
        subprocess.run(
            [
                sys.executable, "-m", "streamlit", "run", STREAMLIT_APP_PATH,
                "--server.port", str(STREAMLIT_PORT),
                "--server.headless=true",  # 关键：不自动打开浏览器
                "--server.enableCORS=false",  # 允许桌面窗口访问
                "--server.enableXsrfProtection=false"
            ],
            cwd=ROOT_DIR,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Streamlit服务启动失败：{e}")
        input("按回车键退出...")  # 让窗口停留，方便看错误
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Streamlit服务未知错误：{e}")
        input("按回车键退出...")
        sys.exit(1)

def main():
    """一键启动：后台启动服务 + 弹出桌面窗口"""
    try:
        print("=== CAE多数据库联合检索工具（桌面版）启动中 ===")
        print(f"项目根目录：{ROOT_DIR}")
        print(f"Streamlit服务端口：{STREAMLIT_PORT}")
        print("⚠️  首次启动可能需要2-3秒，请稍候...")

        # 1. 检查前端文件是否存在（避免启动失败）
        app_full_path = os.path.join(ROOT_DIR, STREAMLIT_APP_PATH)
        if not os.path.exists(app_full_path):
            print(f"\n❌ 找不到前端文件：{app_full_path}")
            print("请检查文件路径是否正确！")
            input("按回车键退出...")
            sys.exit(1)

        # 2. 后台启动Streamlit服务（线程方式，不卡窗口）
        st_thread = threading.Thread(target=start_streamlit_server, daemon=True)
        st_thread.start()

        # 3. 等待服务启动（必须等服务起来再开窗口，否则加载失败）
        time.sleep(2)

        # 4. 弹出桌面窗口，加载Streamlit页面
        webview.create_window(
            title=WINDOW_TITLE,
            url=f"http://localhost:{STREAMLIT_PORT}",
            width=WINDOW_WIDTH,
            height=WINDOW_HEIGHT,
            resizable=True,  # 允许调整窗口大小
            minimized=False   # 不最小化启动
        )
        webview.start(debug=False)  # 启动窗口（阻塞，直到关闭窗口）

    except ImportError:
        # 提示用户安装缺失的库（代码白痴友好）
        print("\n❌ 缺少必要的库，请先执行以下命令安装：")
        print("pip install pywebview streamlit pandas")
        input("按回车键退出...")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 桌面窗口启动失败：{e}")
        input("按回车键退出...")
        sys.exit(1)

if __name__ == "__main__":
    main()