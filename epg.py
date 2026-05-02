import logging
import os
import gzip
import requests
from lxml import etree
from datetime import datetime
import io

# ================= 配置区域 =================
# 源列表（将你需要的源放在这里）
# 注意：如果某个链接失效（404），脚本会自动跳过它
SOURCES = [
    "http://epg.51zmt.top:8000/e.xml",       # 源1：通常包含基础频道
    "http://epg.dy2.fun:5678/xml",           # 源2：百川源（如果失效会自动跳过）
    # 你可以在这里添加更多备用源
]

# 输出文件路径
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "epgl.gz")

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ================= 核心逻辑 =================

def fetch_xml_source(url):
    """
    抓取单个源并返回 XML 根节点
    """
    try:
        logger.info(f"📥 开始抓取源: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            # 尝试解压 Gzip（如果源是压缩的）
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                    content = f.read()
                logger.info(f"   ✅ 抓取成功 (Gzip): {len(content)} 字节")
            except:
                # 如果不是 Gzip，直接作为普通 XML 处理
                content = response.content
                logger.info(f"   ✅ 抓取成功 (XML): {len(content)} 字节")
            
            # 解析 XML
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            root = etree.fromstring(content, parser)
            return root
        else:
            logger.warning(f"   ❌ 抓取失败: {response.status_code} (已跳过)")
            return None

    except Exception as e:
        logger.error(f"   ❌ 抓取异常: {e} (已跳过)")
        return None

def merge_epg_sources():
    """
    合并多个源
    """
    # 创建一个新的根节点
    merged_root = etree.Element("tv")
    # 用于去重的集合 (channel_id + date)
    seen_programs = set()
    # 用于存储频道信息，避免重复
    channels = {}

    total_programs = 0
    valid_sources = 0

    for url in SOURCES:
        root = fetch_xml_source(url)
        if root is not None:
            valid_sources += 1
            
            # 1. 提取频道信息 (channel)
            for channel in root.findall("channel"):
                channel_id = channel.get("id")
                if channel_id and channel_id not in channels:
                    # 深拷贝频道节点
                    channels[channel_id] = etree.fromstring(etree.tostring(channel, encoding='unicode'))

            # 2. 提取节目信息 (programme)
            for programme in root.findall("programme"):
                channel_id = programme.get("channel")
                start_time = programme.get("start")
                # 简单的去重键：频道ID + 开始时间
                unique_key = f"{channel_id}_{start_time}"
                
                if unique_key not in seen_programs:
                    seen_programs.add(unique_key)
                    # 深拷贝节目节点并添加到合并根节点
                    merged_root.append(programme)
                    total_programs += 1

    logger.info(f"📊 统计: 成功处理 {valid_sources}/{len(SOURCES)} 个源")
    logger.info(f"📺 发现频道: {len(channels)} 个")
    logger.info(f"🎬 总节目数: {total_programs} 条")

    # 将频道信息插入到根节点的最前面
    for channel in channels.values():
        merged_root.insert(0, channel)

    return merged_root

def save_gzip_xml(root):
    """
    将 XML 树保存为 Gzip 压缩文件
    """
    # 生成 XML 字符串
    xml_bytes = etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=True)
    logger.info(f"💾 原始 XML 大小: {len(xml_bytes) / 1024:.2f} KB")

    # 压缩并保存
    with gzip.open(OUTPUT_FILE, "wb") as f:
        f.write(xml_bytes)
    
    compressed_size = os.path.getsize(OUTPUT_FILE)
    logger.info(f"🚀 生成成功: {OUTPUT_FILE} ({compressed_size / 1024:.2f} KB)")

if __name__ == "__main__":
    try:
        # 1. 合并源
        final_root = merge_epg_sources()
        
        # 2. 保存为 Gzip
        save_gzip_xml(final_root)
        
        # 3. 创建一个空的 commit 标记文件（可选，用于触发 GitHub 提交）
        with open(os.path.join(OUTPUT_DIR, ".gitkeep"), "w") as f:
            f.write("EPG Generated")
            
    except Exception as e:
        logger.error(f"❌ 运行出错: {e}")
