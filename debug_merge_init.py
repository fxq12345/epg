import sys
import traceback

print("=== 开始调试 merge.py 初始化 ===")
sys.stdout.flush()

dependencies = [
    'sys', 'os', 're', 'json', 'time', 'datetime',
    'requests', 'lxml', 'bs4', 'xmltodict', 'aiohttp',
    'asyncio', 'tqdm', 'opencc'
]

for dep in dependencies:
    try:
        print(f"正在导入: {dep}")
        sys.stdout.flush()
        __import__(dep)
        print(f"✅ 成功导入: {dep}")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ 导入 {dep} 失败: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)

print("=== 所有依赖导入成功 ===")
sys.stdout.flush()

try:
    print("正在导入 merge.py")
    sys.stdout.flush()
    import merge
    print("✅ 成功导入 merge.py")
    sys.stdout.flush()
except Exception as e:
    print(f"❌ 导入 merge.py 失败: {e}")
    traceback.print_exc()
    sys.stdout.flush()
    sys.exit(1)

print("=== 初始化调试完成 ===")
sys.stdout.flush()
