# -*- coding: utf-8 -*-
"""
CAE多数据库检索工具 - 数据导出工具
支持CSV/Excel格式导出检索结果，解决中文乱码问题
"""
import pandas as pd
from io import BytesIO, StringIO

def export_to_csv(df):
    """
    将DataFrame导出为CSV格式（解决中文乱码）
    :param df: 检索结果DataFrame
    :return: bytes - CSV字节数据（可直接用于Streamlit下载）
    """
    try:
        # StringIO处理字符串，指定编码为utf-8-sig解决中文乱码
        output = StringIO()
        df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)  # 重置指针到开头
        return output.getvalue().encode("utf-8-sig")
    except Exception as e:
        print(f"CSV导出失败：{str(e)}")
        return b""

def export_to_excel(df):
    """
    将DataFrame导出为Excel格式
    :param df: 检索结果DataFrame
    :return: bytes - Excel字节数据（可直接用于Streamlit下载）
    """
    try:
        # BytesIO处理二进制数据
        output = BytesIO()
        # 使用openpyxl引擎，支持.xlsx格式
        df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)  # 重置指针到开头
        return output.getvalue()
    except Exception as e:
        print(f"Excel导出失败：{str(e)}")
        return b""