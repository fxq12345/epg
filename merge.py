import os
import gzip
import requests
from lxml import etree

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
# 修复XML声明的格式（去掉多余换行）
XMLTV_DECLARE = '<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="fxq12345-epg-merge" generator-info-url="https://github.com/fxq12345/epg">'
# ==================================================

def read_epg_sources():
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ 未找到{CONFIG_FILE}")
        exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        sources = [line.strip() for line in f if line.strip()]
    if len(sources) < 5:
        print(f"⚠️ {CONFIG_FILE}中仅找到{len(sources)}个源")
    return sources[:5]

def init_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for f in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, f))

def fetch_and_merge_epg(sources):
    # 修复XML根节点初始化
    root = etree.fromstring(f"{XMLTV_DECLARE}</tv>".encode("utf-8"))
    channel_ids = set()

    for idx, source in enumerate(sources, 1):
        print(f"[{idx}/{len(sources)}] 抓取源：{source}")
        try:
            resp = requests.get(source, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            resp.encoding = "utf-8"
            
            source_tree = etree.fromstring(resp.text.encode("utf-8"))
            
            # 合并频道
            for channel in source_tree.xpath("//channel"):
                cid = channel.get("id")
                if cid not in channel_ids:
                    channel_ids.add(cid)
                    root.insert(0, channel)  # 插入到<tv>标签内
            
            # 合并节目单
            for programme in source_tree.xpath("//programme"):
                root.append(programme)

            print(f"✅ 成功：频道{len(channel_ids)}个 | 节目单{len(root.xpath('//programme'))}个")

        except Exception as e:
            print(f"❌ 失败：{str(e)}")
            continue

    if len(root) == 0:
        print("❌ 无有效EPG数据")
        exit(1)
    # 生成完整XML内容
    return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

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
