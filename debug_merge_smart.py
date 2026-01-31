import sys
import traceback
import importlib.util

print("=== 开始智能调试 merge.py ===")
sys.stdout.flush()

# 接管 print 函数，确保实时输出
original_print = print
def debug_print(*args, **kwargs):
    original_print(*args, **kwargs)
    sys.stdout.flush()

print = debug_print

# 加载 merge.py 并分析它的主入口
spec = importlib.util.spec_from_file_location("merge", "merge.py")
merge = importlib.util.module_from_spec(spec)

# 捕获并打印 merge.py 中的所有输出
class CapturingStdout:
    def __enter__(self):
        self._old_stdout = sys.stdout
        sys.stdout = self
        self.output = []
        return self
    def __exit__(self, *args):
        sys.stdout = self._old_stdout
    def write(self, data):
        self.output.append(data)
        debug_print(data, end='')

# 执行 merge.py
try:
    print("正在执行 merge.py 的完整逻辑...")
    with CapturingStdout():
        spec.loader.exec_module(merge)
    print("✅ merge.py 完整执行完成")
except Exception as e:
    print(f"❌ merge.py 执行出错: {e}")
    traceback.print_exc()
    sys.stdout.flush()
except KeyboardInterrupt:
    print("⚠️  执行被中断")
    sys.stdout.flush()

print("=== 智能调试完成 ===")
sys.stdout.flush()
