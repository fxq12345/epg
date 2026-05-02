import logging
import os
import gzip
import requests
from lxml import etree
from datetime import datetime, timedelta
import io
import time
import re

# ========== 配置区域 ==========
# 输出文件名
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "epg.xml.gz")

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ========== 核心逻辑 ==========

def fetch_source(url):
    """抓取源数据"""
    try:
        logger.info(f"📥 开始抓取源: {url}")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # 处理 Gzip 压缩
        if url.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read()
        else:
            content = response.content
            
        logger.info(f"✅ 抓取成功: {len(content)} 字节")
        return content
    except Exception as e:
        logger.error(f"❌ 抓取失败: {e}")
        return None

def clean_channel_name(name):
    """清洗频道名称，去除后缀，统一格式"""
    if not name:
        return ""
        
    # 转字符串
    name = str(name)
    
    # 统一替换规则
    # 1. 去除后缀
    name = re.sub(r'(高清|HD|超清|720P|1080P|频道|CCTV-|CCTV_|\s)', '', name)
    
    # 2. 特殊修正
    if name.startswith('CCTV'):
        name = name.upper()
    elif '卫视' in name:
        name = name.replace('卫视', '卫视') # 保持原样或做特定替换
        
    return name.strip()

def parse_xml_source(content, target_channels):
    """解析 XML 源并合并到目标字典"""
    if not content:
        return 0
        
    try:
        # 解析 XML
        parser = etree.XMLParser(recover=True, encoding='utf-8')
        root = etree.fromstring(content, parser)
        
        count = 0
        # 查找所有 programme 节点
        programs = root.xpath('//programme')
        
        for prog in programs:
            channel_id = prog.get('channel')
            start_time = prog.get('start')
            stop_time = prog.get('stop')
            
            if not channel_id or not start_time:
                continue
                
            # 清洗频道名
            std_name = clean_channel_name(channel_id)
            if not std_name:
                continue
                
            # 提取标题
            title_elem = prog.find('title')
            title = title_elem.text if title_elem is not None and title_elem.text else "未知节目"
            
            # 存入字典 (这里我们直接保留所有抓取到的频道，不做白名单过滤，以解决CCTV10+缺失问题)
            # 格式：{ "CCTV1": [ {"start": "...", "end": "...", "title": "..."}, ... ] }
            if std_name not in target_channels:
                target_channels[std_name] = []
                
            target_channels[std_name].append({
                "start": start_time,
                "end": stop_time,
                "title": title
            })
            count += 1
            
        logger.info(f"   └─ 解析完成，提取节目数: {count}")
        return count
    except Exception as e:
        logger.error(f"   └─ XML 解析错误: {e}")
        return 0

def generate_xml_gz(data):
    """生成标准的 XMLTV 格式 Gzip 文件"""
    logger.info("🚀 开始生成 XMLTV 格式文件...")
    
    # 1. 构建 XML 树
    root = etree.Element("tv")
    root.set("generator-info-name", "CustomEPGScript")
    root.set("generator-info-url", "https://github.com")
    
    # 2. 遍历数据生成节目单
    # 为了让文件小一点且符合百川格式，我们只生成 programme 标签，不生成 channel 标签（百川格式通常不需要 channel 标签定义）
    count = 0
    for channel, programs in data.items():
        for prog in programs:
            # 创建 programme 节点
            p = etree.SubElement(root, "programme")
            p.set("start", prog['start'])
            if prog['end']:
                p.set("stop", prog['end'])
            p.set("channel", channel)
            
            # 创建 title 节点
            t = etree.SubElement(p, "title")
            t.set("lang", "zh")
            t.text = prog['title']
            
            count += 1
            
    # 3. 序列化为字节
    xml_bytes = etree.tostring(root, pretty_print=False, encoding='utf-8', xml_declaration=True)
    logger.info(f"   └─ XML 构建完成，原始大小: {len(xml_bytes) / 1024:.2f} KB")
    
    # 4. Gzip 压缩
    with gzip.open(OUTPUT_FILE, 'wb') as f:
        f.write(xml_bytes)
        
    final_size = os.path.getsize(OUTPUT_FILE)
    logger.info(f"✅ 生成成功: {OUTPUT_FILE} ({final_size / 1024:.2f} KB)")

def main():
    # 定义源列表 (你可以继续添加或修改)
    sources = [
        # 源1: 这里的格式通常是百川格式，包含大量频道
        "http://epg.51zmt.top:8000/e.xml", 
        
        # 源2: 备用源
        "http://epg.dy2.fun:5678/xml",
        
        # 源3: 如果有其他源可以继续加
        # "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
    ]
    
    # 存储所有频道数据的字典
    all_channels_data = {}
    
    # 依次抓取并解析
    for url in sources:
        content = fetch_source(url)
        if content:
            parse_xml_source(content, all_channels_data)
    
    # 统计频道数量
    logger.info(f"📊 统计: 共包含 {len(all_channels_data)} 个频道")
    
    # 生成最终文件
    generate_xml_gz(all_channels_data)

if __name__ == "__main__":
    main()
