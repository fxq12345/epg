import os
import gzip
import requests
import time
from lxml import etree

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
XMLTV_DECLARE = f'<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="fxq12345-epg-merge" generator-info-url="https://github.com/fxq12345/epg" last-update="{time.strftime("%Y%m%d%H%M%S")}">'
TIMEOUT = 20
RETRY_COUNT = 3
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
    "11": "CCTV11",
    "12": "CCTV12",
    "13": "CCTV13",
    "14": "CCTV14",
    "15": "CCTV15",
    "16": "CCTV16",
    "501": "CCTV5+",
}
# ==================================================

def read_epg_sources():
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ 未找到配置文件：{CONFIG_FILE}")
        exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        sources = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    if not sources:
        print(f"❌ {CONFIG_FILE}中未找到有效EPG源")
        exit(1)
    print(f"✅ 读取到{len(sources)}个有效EPG源")
    return sources[:12]

def decompress_gz(content):
    try:
        return gzip.decompress(content).decode("utf-8", errors="ignore")
    except:
        try:
            return content.decode("gbk", errors="ignore")
        except:
            return content.decode("utf-8", errors="ignore")

def fetch_epg_source(source, retry=0):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate"
    }
    try:
        resp = requests.get(source, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        resp.raise_for_status()
        return decompress_gz(resp.content) if source.endswith(".gz") else resp.content.decode(
            resp.encoding if "charset" in resp.headers.get("Content-Type", "").lower() else "utf-8",
            errors="ignore"
        )
    except Exception as e:
        if retry < RETRY_COUNT:
            print(f"⚠️ 抓取失败，重试第{retry+1}次：{source}")
            time.sleep(3)
            return fetch_epg_source(source, retry+1)
        else:
            print(f"❌ 源失效，跳过：{source} | 错误：{str(e)}")
            return None

def check_core_programs(channel_ids, programs):
    core_categories = {
        "山东本地": 0, "山东本地有节目": 0,
        "央视": 0, "央视有节目": 0,
        "其他卫视": 0, "其他卫视有节目": 0
    }
    for cid in channel_ids:
        for cat_name, cat_keywords in CHANNEL_PRIORITY:
            if any(keyword in cid for keyword in cat_keywords):
                core_categories[cat_name] += 1
                for prog in programs:
                    if prog.get("channel") == cid:
                        core_categories[f"{cat_name}有节目"] += 1
                        break
                break
    print(f"\n📊 核心频道节目单统计：")
    for cat_name in ["山东本地", "央视", "其他卫视"]:
        print(f"   - {cat_name}：{core_categories[cat_name]}个 | 有节目：{core_categories[f'{cat_name}有节目']}个")
    if (core_categories["山东本地"] == 0 or core_categories["央视"] == 0 or core_categories["其他卫视"] == 0):
        print("❌ 核心频道类别缺失，跳过检测（仅本次）")
        return True
    if (core_categories["山东本地有节目"] / core_categories["山东本地"] < 0.8 or
        core_categories["央视有节目"] / core_categories["央视"] < 0.8 or
        core_categories["其他卫视有节目"] / core_categories["其他卫视"] < 0.8):
        print("❌ 核心频道节目单覆盖率不足，跳过检测（仅本次）")
        return True
    return True

def sort_channels(channels):
    sorted_channels = []
    channel_ids = set()
    for cat_name, cat_keywords in CHANNEL_PRIORITY:
        cat_channels = []
        for channel in channels:
            cid = channel.get("id")
            # 酷9专用：数字ID映射为名称ID
            if cid in COOL9_ID_MAPPING:
                cid = COOL9_ID_MAPPING[cid]
            if cid in channel_ids:
                continue
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0] if display_names else cid
            if any(keyword in channel_name or keyword in cid for keyword in cat_keywords):
                channel_ids.add(cid)
                channel.set("id", cid)  # 更新为酷9适配的ID
                cat_channels.append(channel)
        sorted_channels.extend(cat_channels)
        print(f"✅ {cat_name}：{len(cat_channels)}个")
    other_channels = []
    for channel in channels:
        cid = channel.get("id")
        if cid in COOL9_ID_MAPPING:
            cid = COOL9_ID_MAPPING[cid]
        if cid not in channel_ids:
            channel_ids.add(cid)
            channel.set("id", cid)
            other_channels.append(channel)
    sorted_channels.extend(other_channels)
    print(f"✅ 其他频道：{len(other_channels)}个")
    return sorted_channels

def fetch_and_merge_epg(sources):
    core_retry = 0
    while core_retry <= CORE_RETRY_COUNT:
        all_channels = []
        all_programs = []
        channel_ids = set()
        print(f"\n=== 第{core_retry+1}次抓取合并 ===")
        for idx, source in enumerate(sources, 1):
            print(f"\n[{idx}/{len(sources)}] 抓取源：{source}")
            content = fetch_epg_source(source)
            if not content:
                continue
            try:
                content = content.replace("&", "&amp;").replace("<![CDATA[", "").replace("]]>", "")
                source_tree = etree.fromstring(content.encode("utf-8"))
                sources_channels = source_tree.xpath("//channel")
                for channel in sources_channels:
                    cid = channel.get("id", f"channel_{idx}_{len(channel_ids)}")
                    if cid in COOL9_ID_MAPPING:
                        cid = COOL9_ID_MAPPING[cid]
                    if cid not in channel_ids:
                        channel_ids.add(cid)
                        channel.set("id", cid)
                        all_channels.append(channel)
                sources_programs = source_tree.xpath("//programme")
                for program in sources_programs:
                    prog_channel = program.get("channel", "")
                    if prog_channel in COOL9_ID_MAPPING:
                        prog_channel = COOL9_ID_MAPPING[prog_channel]
                    program.set("channel", prog_channel)
                    all_programs.append(program)
                print(f"✅ 成功：频道{len(sources_channels)}个 | 累计频道{len(channel_ids)}个 | 累计节目{len(all_programs)}个")
            except Exception as e:
                print(f"❌ 解析失败：{str(e)}")
                continue
        if check_core_programs(channel_ids, all_programs):
            print("\n✅ 核心频道检测通过")
            break
        elif core_retry < CORE_RETRY_COUNT:
            core_retry += 1
            print(f"🔄 开始第{core_retry+1}次重试")
            time.sleep(8)
        else:
            print("❌ 重试完成，继续生成EPG")
            break
    print("\n=== 按优先级排序频道 ===")
    sorted_channels = sort_channels(all_channels)
    final_root = etree.fromstring(f"{XMLTV_DECLARE}</tv>".encode("utf-8"))
    for channel in sorted_channels:
        final_root.append(channel)
    for program in all_programs:
        final_root.append(program)
    return etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=False).decode("utf-8")

def init_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for f in os.listdir(OUTPUT_DIR):
        file_path = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"⚠️ 清理文件失败：{file_path} | {str(e)}")

def save_epg(xml_content):
    xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"\n📝 保存XML文件：{xml_path}（{os.path.getsize(xml_path)}字节）")
    gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(gz_path, "wb") as f:
        f.write(xml_content.encode("utf-8"))
    print(f"📝 保存GZIP文件：{gz_path}（{os.path.getsize(gz_path)}字节）")

if __name__ == "__main__":
    print("=== 开始生成EPG节目指南（酷9专用） ===")
    start_time = time.time()
    sources = read_epg_sources()
    init_output_dir()
    epg_content = fetch_and_merge_epg(sources)
    save_epg(epg_content)
    end_time = time.time()
    print(f"\n=== EPG生成完成！总耗时：{round(end_time - start_time, 2)}秒 ===")
