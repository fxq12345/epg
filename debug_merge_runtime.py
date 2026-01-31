import sys
import traceback
import merge

print("=== 开始调试 merge.py 运行时 ===")
sys.stdout.flush()

original_print = print
def debug_print(*args, **kwargs):
    original_print(*args, **kwargs)
    sys.stdout.flush()

print = debug_print

try:
    print("正在调用 merge.py 的主函数...")
    merge.main()
    print("✅ merge.py 主函数执行完成")
except Exception as e:
    print(f"❌ merge.py 执行出错: {e}")
    traceback.print_exc()
    sys.stdout.flush()
except KeyboardInterrupt:
    print("⚠️  执行被中断")
    sys.stdout.flush()

print("=== 运行时调试完成 ===")
sys.stdout.flush()
