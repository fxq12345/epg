import os
import gzip
import requests
import time
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"

XMLTV_DECLARE = f'<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="fxq12345-epg-merge" generator-info-url="https://github.com/fxq12345/epg" last-update="{time.strftime("%Y%m%d%H%M%S")}">'
# ==================================================

def create_retry_session(retries=2, backoff_factor=1):
    """创建带重试机制的请求会话"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,  # 重试间隔：1s, 2s, 4s...
        status_forcelist=(500, 502, 503, 504, 408)  # 需要重试的状态码
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session

def read_epg_sources():
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ 未找到{CONFIG_FILE}")
        exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        sources = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                sources.append(line)
    if len(sources) < 5:
        print(f"⚠️ {CONFIG_FILE}中仅找到{len(sources)}个有效源")
    return sources[:5]

def decompress_gz(content):
    try:
        return gzip.decompress(content).decode("utf-8")
    except:
        return content.decode("utf-8")

def fetch_and_merge_epg(sources):
    root = etree.fromstring(f"{XMLTV_DECLARE}</tv>".encode("utf-8"))
    channel_ids = set()

    for idx, source in enumerate(sources, 1):
        print(f"[{idx}/{len(sources)}] 抓取源：{source}")
        try:
            # 第一个源单独处理：延长超时+自动重试
            if idx == 1:
                session = create_retry_session(retries=2)
                resp = session.get(source, timeout=30)  # 超时改为30秒
            else:
                # 其他源保持原有逻辑
                resp = requests.get(source, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            
            resp.raise_for_status()
            
            if source.endswith(".gz"):
                content = decompress_gz(resp.content)
            else:
                content = resp.text
            
            source_tree = etree.fromstring(content.encode("utf-8"))
            
            for channel in source_tree.xpath("//channel"):
                cid = channel.get("id")
                if cid not in channel_ids:
                    channel_ids.add(cid)
                    root.insert(0, channel)
            
            for programme in source_tree.xpath("//programme"):
                root.append(programme)

            print(f"✅ 成功：频道{len(channel_ids)}个 | 节目单{len(root.xpath('//programme'))}个")

        except Exception as e:
            print(f"❌ 失败：{str(e)}")
            continue

    if len(root) == 0:
        print("❌ 无有效EPG数据")
        exit(1)
    return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

def init_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for f in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, f))

def save_epg(xml_content):
    xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"📝 保存XML：{xml_path}（{os.path.getsize(xml_path)}字节）")

    gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(gz_path, "wb") as f:
        f.write(xml_content.encode("utf-8"))
    print(f"📝 保存GZIP：{gz_path}（{os.path.getsize(gz_path)}字节）")

if __name__ == "__main__":
    print("=== 开始生成EPG ===")
    sources = read_epg_sources()
    init_output_dir()
    epg_content = fetch_and_merge_epg(sources)
    save_epg(epg_content)
    print("=== EPG生成完成 ===")
