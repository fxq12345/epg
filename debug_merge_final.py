import sys
import subprocess
import threading
import time

print("=== 开始终极调试 merge.py ===")
sys.stdout.flush()

# 定义超时时间（秒），从300秒延长到600秒（10分钟）
TIMEOUT = 600

# 定义实时输出的线程
def stream_output(process):
    for line in iter(process.stdout.readline, ''):
        if line:
            print(line, end='')
            sys.stdout.flush()

# 直接用 subprocess 运行 merge.py，模拟用户操作
try:
    print("正在模拟运行 `python merge.py`...")
    sys.stdout.flush()

    # 启动子进程运行 merge.py
    process = subprocess.Popen(
        [sys.executable, 'merge.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        universal_newlines=True
    )

    # 启动线程实时输出
    output_thread = threading.Thread(target=stream_output, args=(process,))
    output_thread.start()

    # 等待进程完成或超时
    start_time = time.time()
    while process.poll() is None:
        if time.time() - start_time > TIMEOUT:
            print(f"⚠️  执行超时（{TIMEOUT}秒），正在终止进程...")
            sys.stdout.flush()
            process.kill()
            break
        time.sleep(1)

    # 等待输出线程结束
    output_thread.join()

    if process.returncode == 0:
        print("✅ merge.py 执行完成，退出码 0")
    elif process.returncode == -9:
        print("❌ merge.py 执行超时，已被终止")
    else:
        print(f"❌ merge.py 执行失败，退出码 {process.returncode}")

except Exception as e:
    print(f"❌ 调试脚本出错: {e}")
    sys.stdout.flush()

print("=== 终极调试完成 ===")
sys.stdout.flush()
