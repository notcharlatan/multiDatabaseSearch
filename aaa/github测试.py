import requests
import subprocess

# 测试 Python 是否能连接
try:
    response = requests.get('https://github.com', timeout=10)
    print(f"Python 连接成功: {response.status_code}")
except Exception as e:
    print(f"Python 连接失败: {e}")

# 测试 Git 命令
try:
    result = subprocess.run(['git', 'ls-remote', 'https://github.com/notcharlatan/multiDatabaseSearch.git'],
                          capture_output=True, text=True, timeout=30)
    print(f"Git 输出: {result.stdout[:100]}")
except Exception as e:
    print(f"Git 命令失败: {e}")