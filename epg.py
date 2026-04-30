import os
import gzip
import io
import requests
from lxml import etree
from datetime import datetime
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

def download_source(url):
    try:
        print(f"🔽 下载源: {url}")
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        content = r.content

        # 解压gzip压缩的XML
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        return content
    except Exception as e:
        print(f"❌ 下载失败: {url} 错误: {e}")
        return None

def parse_xml(content):
    try:
        root = etree.fromstring(content)
        channels = {ch.get("id"): ch for ch in root.xpath("//channel") if ch.get("id")}
        programmes = root.xpath("//programme")
        return channels, programmes
    except Exception as e:
        print(f"❌ XML解析失败: {e}")
        return {}, []

def parse_time(time_str):
    """解析EPG时间戳，转成datetime对象用于排序"""
    try:
        return datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
    except:
        return datetime.min

def main():
    urls = read_config()
    if not urls:
        print("❌ config.txt 为空，没有源链接")
        return

    all_channels = {}
    all_programmes = []

    # 下载并解析所有源
    for idx, url in enumerate(urls, 1):
        content = download_source(url)
        if not content:
            continue
        channels, programmes = parse_xml(content)
        print(f"✅ 源{idx}解析成功: {len(channels)}频道 | {len(programmes)}节目")
        all_channels.update(channels)
        all_programmes.extend(programmes)

    if not all_channels:
        print("❌ 没有任何频道数据")
        return

    # 1. 节目自动去重（同一频道、同一时间的节目只保留一个）
    seen = set()
    unique_programmes = []
    for p in all_programmes:
        channel = p.get("channel")
        start = p.get("start")
        key = (channel, start)
        if key not in seen:
            seen.add(key)
            unique_programmes.append(p)

    # 2. 节目按时间排序（同一频道的节目按开始时间升序排列）
    unique_programmes.sort(key=lambda x: parse_time(x.get("start")))

    # 构建合并后的XML
    root = etree.Element("tv")
    for ch in all_channels.values():
        root.append(ch)
    for p in unique_programmes:
        root.append(p)

    # 原样打包，不做额外修改
    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
    outpath = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with gzip.open(outpath, "wb") as f:
        f.write(xml_bytes)

    print("="*60)
    print(f"✅ 合并完成！")
    print(f"✅ 总频道数: {len(all_channels)}")
    print(f"✅ 总节目数（去重后）: {len(unique_programmes)}")
    print(f"✅ 自动去重 + 时间排序，数据100%原样保留")
    print("="*60)

if __name__ == "__main__":
    main()
