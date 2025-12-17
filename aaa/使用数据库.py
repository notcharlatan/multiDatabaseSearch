import pymysql


def search_material_by_keyword(keyword="材料"):
    """在preprae数据库的material表中搜索包含关键词的记录"""
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='2003.guo',
        database='preprae',
        charset='utf8mb4'
    )

    try:
        with connection.cursor() as cursor:


            #方案2：如果希望在所有列中搜索（包括idmaterial和hardness）
            sql = """
            SELECT * FROM material 
            WHERE CONCAT_WS(' ', 
                IFNULL(idmaterial, ''),
                IFNULL(materialNAME, ''),
                IFNULL(materialTYPE, ''),
                IFNULL(hardness, '')
            ) LIKE %s
            """

            # 添加通配符进行模糊匹配
            search_pattern = f"%{keyword}%"


            cursor.execute(sql, (search_pattern,))

            results = cursor.fetchall()

            # 打印结果
            print("=" * 60)
            print(f"🔍 在 preprae.material 表中搜索关键词: '{keyword}'")
            print(f"📊 找到 {cursor.rowcount} 条记录")
            print("=" * 60)

            if cursor.rowcount > 0:
                # 获取列名
                column_names = [desc[0] for desc in cursor.description]
                print("字段列表:", column_names)
                print("-" * 60)

                for idx, row in enumerate(results, 1):
                    print(f"\n📄 记录 #{idx}:")
                    print("-" * 40)

                    for col_name, value in zip(column_names, row):
                        if value is None:
                            display_value = "NULL"
                        else:
                            display_value = str(value)

                        # 检查并高亮关键词
                        if value and keyword in display_value:
                            # 在终端中高亮显示
                            highlighted = display_value.replace(
                                keyword, f"\033[91m{keyword}\033[0m"  # 红色
                            )
                            print(f"  {col_name:15s}: {highlighted}")
                        else:
                            print(f"  {col_name:15s}: {display_value}")
            else:
                print(f"❌ 未找到包含 '{keyword}' 的记录")

            print("=" * 60)

    except Exception as e:
        print(f"❌ 查询出错: {e}")
    finally:
        connection.close()
        print("✅ 数据库连接已关闭")


# 使用示例
if __name__ == "__main__":
    # 搜索关键词"材料"
    search_material_by_keyword("材料")

    # 也可以搜索其他关键词
    # search_material_by_keyword("钢")
    # search_material_by_keyword("铁")