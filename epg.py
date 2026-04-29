import os
import re
import json
import gzip
import io
import argparse
import logging
from datetime import datetime, timedelta
import requests
from lxml import etree

# =============================================
# 配置区域
# =============================================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
XML_OUTPUT_FILE = "epg.xml.gz"
BAICHUAN_OUTPUT_FILE = "epg_baichuan.json"

# 时间范围设置
DAYS_BEFORE = 7
DAYS_AFTER = 7

# 自动创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 固定时间（可修改为 datetime.now() 使用当前时间）
now = datetime(2026, 4, 29, 12, 0, 0)
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# =============================================
# 网络请求
# =============================================
def fetch(url, index):
    """获取EPG源"""
    try:
        logging.info(f"[{index}] 📡 正在获取: {url[:50]}...")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            logging.warning(f"[{index}] ❌ HTTP错误 {r.status_code}")
            return None, None, False
        
        content = r.content
        content_type = r.headers.get('Content-Type', '').lower()
        format_type = detect_format(content, url, content_type)
        
        logging.info(f"[{index}] ✅ 获取成功 ({format_type.upper()}格式)")
        return content, format_type, True
        
    except Exception as e:
        logging.warning(f"[{index}] ❌ 异常: {str(e)[:50]}")
        return None, None, False

def detect_format(content, url, content_type):
    """检测内容格式"""
    # 检测 Gzip
    if content.startswith(b'\x1f\x8b'):
        return "gzip"
    
    # 检测 XML
    if b'<?xml' in content[:100] or b'<tv' in content[:100]:
        return "xml"
    
    # 检测 JSON
    try:
        if content.startswith(b'{') or content.startswith(b'['):
            json.loads(content.decode('utf-8', errors='ignore')[:100])
            return "json"
    except:
        pass
    
    # 检测 M3U/TXT
    if b'#EXTM3U' in content[:100] or b'http://' in content[:200]:
        return "txt"
    
    return "unknown"

# =============================================
# 百川格式解析（原生JSON输出）
# =============================================
def parse_baichuan_native(content, index):
    """解析百川格式，保持原生JSON结构"""
    try:
        text = content.decode('utf-8', errors='ignore')
        data = json.loads(text)
        
        # 标准化百川数据
        standardized_data = []
        
        # 格式1: [{"tvid":"xxx","name":"xxx","list":[...]}]
        if isinstance(data, list) and len(data) > 0:
            first = data[0]
            if 'tvid' in first or 'id' in first:
                for item in data:
                    tvid = item.get('tvid') or item.get('id')
                    name = item.get('name', '未知频道')
                    program_list = item.get('list', [])
                    
                    if tvid:
                        standardized_data.append({
                            "tvid": str(tvid),
                            "name": name,
                            "list": program_list
                        })
        
        # 格式2: {"channels": [...]}
        elif isinstance(data, dict) and 'channels' in data:
            for ch in data['channels']:
                cid = ch.get('cid') or ch.get('id')
                name = ch.get('cname') or ch.get('name', '未知频道')
                epg = ch.get('epg', [])
                
                if cid:
                    standardized_data.append({
                        "tvid": str(cid),
                        "name": name,
                        "list": epg
                    })
        
        logging.info(f"[{index}] 🟦 百川解析: {len(standardized_data)} 个频道")
        return standardized_data
        
    except Exception as e:
        logging.error(f"[{index}] ❌ 百川解析失败: {str(e)}")
        return []

# =============================================
# XML格式解析（用于压缩输出）
# =============================================
def parse_to_xml(content, format_type, index):
    """解析为XML元素"""
    channels = {}
    programs = []
    
    try:
        # 处理 Gzip
        if format_type == "gzip" or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
            format_type = "xml"
        
        # 解码
        if isinstance(content, bytes):
            try:
                content_str = content.decode('utf-8')
            except:
                content_str = content.decode('gbk', errors='ignore')
        else:
            content_str = content
        
        # 解析 XML
        if format_type == "xml":
            root = etree.fromstring(content_str.encode('utf-8'))
            
            # 解析频道
            for ch in root.xpath("//channel"):
                cid = ch.get("id")
                if cid:
                    channels[cid] = ch
            
            # 解析节目
            for p in root.xpath("//programme"):
                start_time = parse_program_time(p.get("start", ""))
                if start_time and start_cutoff <= start_time <= end_cutoff:
                    programs.append(p)
        
        # 解析 JSON（百川转XML）
        elif format_type == "json":
            data = json.loads(content_str)
            
            # 百川格式转XML
            if isinstance(data, list):
                for item in data:
                    tvid = item.get('tvid') or item.get('id')
                    name = item.get('name', '未知频道')
                    program_list = item.get('list', [])
                    
                    if tvid:
                        # 创建频道
                        channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', str(tvid))
                        if channel_id not in channels:
                            channel_elem = etree.Element("channel")
                            channel_elem.set("id", channel_id)
                            display_name = etree.SubElement(channel_elem, "display-name")
                            display_name.text = name
                            channels[channel_id] = channel_elem
                        
                        # 创建节目
                        for prog in program_list:
                            time_str = prog.get('time', '')
                            title = prog.get('program', '未知节目')
                            
                            start_dt = parse_program_time(time_str)
                            if start_dt and start_cutoff <= start_dt <= end_cutoff:
                                stop_dt = start_dt + timedelta(minutes=30)
                                
                                prog_elem = etree.Element("programme")
                                prog_elem.set("start", start_dt.strftime("%Y%m%d%H%M%S 0"))
                                prog_elem.set("stop", stop_dt.strftime("%Y%m%d%H%M%S 0"))
                                prog_elem.set("channel", channel_id)
                                
                                title_elem = etree.SubElement(prog_elem, "title")
                                title_elem.text = title
                                
                                programs.append(prog_elem)
        
        logging.info(f"[{index}] 📺 XML解析: {len(channels)} 频道, {len(programs)} 节目")
        return channels, programs
        
    except Exception as e:
        logging.error(f"[{index}] ❌ XML解析失败: {str(e)}")
        return {}, []

def parse_program_time(time_str):
    """解析节目时间"""
    if not time_str:
        return None
    try:
        # 支持多种格式
        if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_str):
            return datetime.strptime(time_str[:16], "%Y-%m-%d %H:%M")
        elif len(time_str) >= 14 and time_str[:14].isdigit():
            return datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
        elif re.match(r'\d{1,2}:\d{2}', time_str):
            time_obj = datetime.strptime(time_str[:5], "%H:%M").time()
            dt = datetime.combine(today, time_obj)
            if dt < now:
                dt += timedelta(days=1)
            return dt
    except:
        pass
    return None

# =============================================
# 配置读取
# =============================================
def read_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            f.write("# 在此添加EPG源\n")
            f.write("https://example.com/epg.xml.gz\n")
        logging.info(f"✅ 已生成示例配置文件 {CONFIG_FILE}")
        return []
    
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return urls

# =============================================
# 主函数
# =============================================
def main():
    parser = argparse.ArgumentParser(description='EPG生成器')
    parser.add_argument('--mode', choices=['xml', 'baichuan', 'both'], default='both',
                       help='输出模式: xml=仅XML, baichuan=仅百川, both=两者都输出')
    parser.add_argument('--force', action='store_true', help='强制覆盖输出文件')
    args = parser.parse_args()
    
    logging.info("=" * 60)
    logging.info(f"🚀 开始EPG生成任务 (模式: {args.mode})")
    logging.info(f"📅 时间范围: {start_cutoff.date()} 至 {end_cutoff.date()}")
    logging.info("=" * 60)
    
    urls = read_config()
    if not urls:
        logging.error("❌ 没有可用的EPG源")
        return
    
    # 存储百川原生数据
    all_baichuan_data = []
    # 存储XML数据
    all_channels = {}
    all_programs = []
    
    for i, url in enumerate(urls, 1):
        content, format_type, success = fetch(url, i)
        if not success or not content:
            continue
        
        # 百川模式处理
        if args.mode in ['baichuan', 'both'] and format_type == 'json':
            baichuan_data = parse_baichuan_native(content, i)
            if baichuan_data:
                all_baichuan_data.extend(baichuan_data)
        
        # XML模式处理
        if args.mode in ['xml', 'both']:
            channels, programs = parse_to_xml(content, format_type, i)
            for cid, ch in channels.items():
                if cid not in all_channels:
                    all_channels[cid] = ch
            all_programs.extend(programs)
    
    # 输出百川格式
    if args.mode in ['baichuan', 'both'] and all_baichuan_data:
        baichuan_path = os.path.join(OUTPUT_DIR, BAICHUAN_OUTPUT_FILE)
        
        # 强制覆盖检查
        if os.path.exists(baichuan_path) and not args.force:
            logging.warning(f"⚠️ 文件 {baichuan_path} 已存在，使用 --force 强制覆盖")
        else:
            with open(baichuan_path, 'w', encoding='utf-8') as f:
                json.dump(all_baichuan_data, f, ensure_ascii=False, indent=2)
            
            size_kb = os.path.getsize(baichuan_path) / 1024
            logging.info(f"✅ 百川格式已生成: {baichuan_path} ({size_kb:.1f} KB)")
            logging.info(f"📊 百川数据: {len(all_baichuan_data)} 个频道")
    
    # 输出XML格式
    if args.mode in ['xml', 'both'] and all_channels and all_programs:
        xml_path = os.path.join(OUTPUT_DIR, XML_OUTPUT_FILE)
        
        # 强制覆盖检查
        if os.path.exists(xml_path) and not args.force:
            logging.warning(f"⚠️ 文件 {xml_path} 已存在，使用 --force 强制覆盖")
        else:
            root = etree.Element("tv")
            root.set("generator-info-name", "EPG多格式合并器")
            root.set("date", datetime.now().strftime("%Y%m%d%H%M%S"))
            
            for ch in all_channels.values():
                root.append(ch)
            for p in all_programs:
                root.append(p)
            
            xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
            
            with gzip.open(xml_path, "wb") as f:
                f.write(xml_bytes)
            
            size_kb = os.path.getsize(xml_path) / 1024
            logging.info(f"✅ XML格式已生成: {xml_path} ({size_kb:.1f} KB)")
            logging.info(f"📺 总频道: {len(all_channels)} 个")
            logging.info(f"📅 总节目: {len(all_programs)} 个")
    
    logging.info("=" * 60)
    logging.info("🎉 任务完成！")

if __name__ == "__main__":
    main()
