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
CORE_RETRY_COUNT = 2

# 核心频道关键词
CORE_CHANNEL_KEYWORDS = ["山东", "CCTV", "卫视"]

# 频道排序优先级
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 酷9专用ID映射表（数字ID→名称ID）
COOL9_ID_MAPPING = {
    "89": "山东卫视",
    "221": "山东教育",
    "381": "山东新闻",
    "382": "山东农科",
    "383": "山东齐鲁",
    "384": "山东文旅",
    "1": "CCTV1",
    "2": "CCTV2",
    "3": "CCTV3",
    "4": "CCTV4",
    "5": "CCTV5",
    "6": "CCTV6",
    "7": "CCTV7",
    "8": "CCTV8",
    "9": "CCTV9",
    "10": "CCTV10",
    # 新增4K频道映射
    "101": "CCTV4K",
    "102": "浙江卫视4K",
    "103": "湖南卫视4K",
    "104": "东方卫视4K"
}

XMLTV_DECLARE = f'<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="fxq12345-epg-merge" generator-info-url="https://github.com/fxq12345/epg" last-update="{time.strftime("%Y%m%d%H%M%S")}">'
# ==================================================

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
        return content.decode("utf-8", errors="ignore")

def fetch_and_merge_epg(sources):
    root = etree.fromstring(f"{XMLTV_DECLARE}</tv>".encode("utf-8"))
    channel_ids = set()
    # 按优先级分类存储频道
    priority_channels = {cat: [] for cat, _ in CHANNEL_PRIORITY}
    other_channels = []

    # 增强网络重试与超时配置
    session = requests.Session()
    retry_strategy = Retry(
        total=CORE_RETRY_COUNT + 3,  # 基于配置的重试次数
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    for idx, source in enumerate(sources, 1):
        print(f"[{idx}/{len(sources)}] 抓取源：{source}")
        try:
            resp = session.get(
                source,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            )
            resp.raise_for_status()
            
            if source.endswith(".gz"):
                content = decompress_gz(resp.content)
            else:
                content = resp.text
            
            source_tree = etree.fromstring(content.encode("utf-8", errors="ignore"))
            
            for channel in source_tree.xpath("//channel"):
                cid = channel.get("id").strip().replace(" ", "_")
                channel_name = channel.xpath(".//display-name/text()")[0].strip() if channel.xpath(".//display-name/text()") else ""
                if cid in channel_ids:
                    continue

                # 按新的优先级规则分类
                is_priority = False
                for cat, keywords in CHANNEL_PRIORITY:
                    for kw in keywords:
                        if kw in channel_name:
                            priority_channels[cat].append(channel)
                            channel_ids.add(cid)
                            is_priority = True
                            break
                    if is_priority:
                        break
                if not is_priority:
                    other_channels.append(channel)
                    channel_ids.add(cid)
            
            # 合并节目单（处理酷9 ID映射）
            for programme in source_tree.xpath("//programme"):
                channel_id = programme.get("channel")
                if channel_id in COOL9_ID_MAPPING:
                    programme.set("channel", COOL9_ID_MAPPING[channel_id])
                root.append(programme)

            print(f"✅ 成功：频道{len(channel_ids)}个 | 节目单{len(root.xpath('//programme'))}个")

        except Exception as e:
            print(f"❌ 失败：{str(e)}（网络波动或源失效，已跳过）")
            continue

    # 按新优先级插入频道
    insert_pos = 0
    for cat, _ in CHANNEL_PRIORITY:
        for channel in priority_channels[cat]:
            root.insert(insert_pos, channel)
            insert_pos += 1
    for channel in other_channels:
        root.insert(insert_pos, channel)
        insert_pos += 1

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
