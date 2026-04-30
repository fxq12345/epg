import os
import gzip
import requests
import sys

# 强制控制台无缓冲输出
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    urls = read_config()
    if not urls:
        print("❌ config.txt 为空，没有源链接")
        return

    # 只取第一条源
    url = urls[0]
    print(f"🔽 正在下载: {url}")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # 直接原样打包，不解析、不修改
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with gzip.open(out_path, "wb") as f:
        f.write(r.content)

    print("✅ 完成！文件与源100%一致，无任何处理")

if __name__ == "__main__":
    main()
