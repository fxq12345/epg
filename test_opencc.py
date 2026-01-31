import sys
import time

print("=== 开始测试 opencc ===")
sys.stdout.flush()

# 测试 1: 尝试导入 opencc
try:
    from opencc import OpenCC
    print("✅ 成功导入 opencc")
    sys.stdout.flush()
except Exception as e:
    print(f"❌ 导入 opencc 失败: {e}")
    sys.stdout.flush()
    sys.exit(1)

# 测试 2: 尝试初始化 OpenCC
try:
    converter = OpenCC('t2s')
    print("✅ 成功初始化 OpenCC")
    sys.stdout.flush()
except Exception as e:
    print(f"❌ 初始化 OpenCC 失败: {e}")
    sys.stdout.flush()
    sys.exit(1)

# 测试 3: 尝试转换文本
try:
    result = converter.convert("繁體轉簡體測試")
    print(f"✅ 成功转换文本: {result}")
    sys.stdout.flush()
except Exception as e:
    print(f"❌ 转换文本失败: {e}")
    sys.stdout.flush()
    sys.exit(1)

print("=== 所有测试通过 ===")
sys.stdout.flush()
