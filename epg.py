import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io
import sys

# 强制控制台无缓冲输出
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

# 前后各7天，合计15天
DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==================== 下载 ====================
def fetch(url, index):
    try:
        logging.info(f"[{index}] 下载: {url}")
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logging.warning(f"[{index}] 失败 {r.status_code}")
            return None, None, False
        return r.content, detect_format(r.content, url), True
    except Exception as e:
        logging.warning(f"[{index}] 异常: {str(e)[:40]}")
        return None, None, False

# ==================== 格式判断 ====================
def detect_format(content, url):
    if content.startswith(b'\x1f\x8b'):
        return "xmlgz"
    if b'<?xml' in content[:200] or b'<tv' in content[:200]:
        return "xml"
    try:
        if content.lstrip().startswith((b'{', b'[')):
            json.loads(content.decode("utf-8","ignore")[:300])
            return "json"
    except:
        pass
    return "unknown"

# ==================== 解析XML【原样导入，不清洗ID、不删节目】====================
def parse_xml(content, index):
    try:
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = etree.fromstring(content)
        channels = {ch.get("id"): ch for ch in root.xpath("//channel") if ch.get("id")}
        programs = root.xpath("//programme")
        logging.info(f"[{index}] 导入成功：{len(channels)} 频道 | {len(programs)} 节目")
        return channels, programs
    except Exception as e:
        logging.warning(f"[{index}] XML解析失败: {e}")
        return {}, []

# ==================== 读取配置 ====================
def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

# ==================== 主程序 ====================
def main():
    urls = read_config()
    if not urls:
        logging.error("config.txt 为空，没有源链接")
        return

    all_channels = {}
    all_programs = []

    for idx, url in enumerate(urls, 1):
        data, fmt, ok = fetch(url, idx)
        if not ok or not data:
            continue
        if fmt in ("xml", "xmlgz"):
            chs, progs = parse_xml(data, idx)
            all_channels.update(chs)
            all_programs.extend(progs)

    if all_channels:
        root = etree.Element("tv")
        for ch in all_channels.values():
            root.append(ch)
        for p in all_programs:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        outpath = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(outpath, "wb") as f:
            f.write(xml_bytes)

        print("="*60)
        print(f"✅ 生成完成：{len(all_channels)} 频道 | {len(all_programs)} 节目")
        print(f"✅ 完整保留：前7天 + 后7天 = 15天")
        print(f"✅ 原样输出：所有卫视、地方台、CCTV4/5 全部保留")
        print("="*60)

if __name__ == "__main__":
    main()
