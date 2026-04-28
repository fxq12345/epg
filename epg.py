import os
import gzip
import requests
from lxml import etree
from datetime import datetime, timedelta

CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

# ✅ 前7天 / 后7天
DAYS_BEFORE = 7
DAYS_AFTER = 7

# ✅ 自动创建目录（没有就建，有就直接用）
os.makedirs(OUTPUT_DIR, exist_ok=True)

now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.ok:
            if r.content.startswith(b'\x1f\x8b'):
                return gzip.decompress(r.content).decode('utf-8', errors='ignore')
            return r.text
    except:
        pass
    return None

def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

def parse(xml):
    try:
        root = etree.fromstring(xml.encode("utf-8"))
    except:
        return {}, []

    channels = {}
    programs = []

    for ch in root.xpath("//channel"):
        cid = ch.get("id")
        if cid:
            channels[cid] = ch

    for p in root.xpath("//programme"):
        try:
            start = datetime.strptime(p.get("start")[:14], "%Y%m%d%H%M%S")
            stop = datetime.strptime(p.get("stop")[:14], "%Y%m%d%H%M%S")
        except:
            continue

        if start_cutoff <= start <= end_cutoff:
            programs.append(p)

    return channels, programs

def main():
    urls = read_config()
    all_channels = {}
    all_programs = []

    for url in urls:
        xml = fetch(url)
        if xml:
            chs, prs = parse(xml)
            all_channels.update(chs)
            all_programs.extend(prs)

    root = etree.Element("tv")
    for ch in all_channels.values():
        root.append(ch)
    for p in all_programs:
        root.append(p)

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)

    # ✅ 强制覆盖（'wb' 模式会自动覆盖原文件）
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_bytes)

    print(f"✅ 已生成：{out_path}")

if __name__ == "__main__":
    main()
