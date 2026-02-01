import os
import gzip
import requests
import time
from lxml import etree

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
# 加入动态时间戳（让EPG文件内容每次不同，触发播放器更新）
XMLTV_DECLARE = f'<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="fxq12345-epg-merge" generator-info-url="https://github.com/fxq12345/epg" last-update="{time.strftime("%Y%m%d%H%M%S")}">'
# 超时时间（秒）
TIMEOUT = 20
# 最大重试次数
RETRY_COUNT = 2
# ==================================================

def read_epg_sources():
    """读取EPG源列表"""
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ 未找到配置文件：{CONFIG_FILE}")
        exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        sources = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                sources.append(line)
    if len(sources) == 0:
        print(f"❌ {CONFIG_FILE}中未找到有效EPG源")
        exit(1)
    if len(sources) < 5:
        print(f"⚠️ {CONFIG_FILE}中仅找到{len(sources)}个有效源")
    return sources[:10]  # 限制最大10个源，避免超时

def decompress_gz(content):
    """解压GZIP内容"""
    try:
        return gzip.decompress(content).decode("utf-8", errors="ignore")
    except:
        try:
            return content.decode("gbk", errors="ignore")  # 兼容GBK编码源
        except:
            return content.decode("utf-8", errors="ignore")

def fetch_epg_source(source, retry=0):
    """抓取单个EPG源（支持重试）"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate"
    }
    try:
        resp = requests.get(source, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        resp.raise_for_status()
        if source.endswith(".gz"):
            return decompress_gz(resp.content)
        else:
            # 自动识别编码
            if "charset" in resp.headers.get("Content-Type", "").lower():
                encoding = resp.encoding
            else:
                encoding = "utf-8"
            return resp.content.decode(encoding, errors="ignore")
    except Exception as e:
        if retry < RETRY_COUNT:
            print(f"⚠️ 抓取失败，重试第{retry+1}次：{source}")
            time.sleep(2)
            return fetch_epg_source(source, retry+1)
        else:
            raise Exception(f"超过最大重试次数：{str(e)}")

def fetch_and_merge_epg(sources):
    """抓取并合并所有EPG源"""
    root = etree.fromstring(f"{XMLTV_DECLARE}</tv>".encode("utf-8"))
    channel_ids = set()
    total_programs = 0

    for idx, source in enumerate(sources, 1):
        print(f"\n[{idx}/{len(sources)}] 正在抓取：{source}")
        try:
            content = fetch_epg_source(source)
            # 修复XML可能存在的语法错误
            content = content.replace("&", "&amp;").replace("<![CDATA[", "").replace("]]>", "")
            source_tree = etree.fromstring(content.encode("utf-8"))
            
            # 合并频道（去重）
            channels = source_tree.xpath("//channel")
            for channel in channels:
                cid = channel.get("id", f"channel_{idx}_{len(channel_ids)}")
                if cid not in channel_ids:
                    channel_ids.add(cid)
                    root.insert(0, channel)
            
            # 合并节目单
            programs = source_tree.xpath("//programme")
            for program in programs:
                root.append(program)
            total_programs += len(programs)

            print(f"✅ 成功：新增频道{len(channels)}个 | 累计频道{len(channel_ids)}个 | 累计节目{total_programs}个")

        except Exception as e:
            print(f"❌ 失败：{str(e)}")
            continue

    if len(channel_ids) == 0 or total_programs == 0:
        print("❌ 未获取到有效EPG数据")
        exit(1)
    return etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=False).decode("utf-8")

def init_output_dir():
    """初始化输出目录"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # 清空输出目录
    for f in os.listdir(OUTPUT_DIR):
        file_path = os.path.join(OUTPUT_DIR, f)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"⚠️ 清理文件失败：{file_path} | {str(e)}")

def save_epg(xml_content):
    """保存EPG文件（XML和GZIP格式）"""
    # 保存XML文件
    xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"\n📝 保存XML文件：{xml_path}（{os.path.getsize(xml_path)}字节）")

    # 保存GZIP文件
    gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(gz_path, "wb") as f:
        f.write(xml_content.encode("utf-8"))
    print(f"📝 保存GZIP文件：{gz_path}（{os.path.getsize(gz_path)}字节）")

if __name__ == "__main__":
    print("=== 开始生成EPG节目指南 ===")
    start_time = time.time()
    sources = read_epg_sources()
    init_output_dir()
    epg_content = fetch_and_merge_epg(sources)
    save_epg(epg_content)
    end_time = time.time()
    print(f"\n=== EPG生成完成！总耗时：{round(end_time - start_time, 2)}秒 ===")
