import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io

CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

# ✅ 前7天 / 后7天
DAYS_BEFORE = 7
DAYS_AFTER = 7

# ✅ 自动创建目录
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ 设置日志 (增加了更详细的信息)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch(url, index):
    """获取EPG源，返回二进制内容和格式类型"""
    try:
        logging.info(f"[{index}] 📡 正在获取: {url[:50]}...")
        r = requests.get(url, headers=HEADERS, timeout=30)
        
        if r.status_code != 200:
            logging.warning(f"[{index}] ❌ HTTP错误 {r.status_code}: {url[:50]}...")
            return None, None, False
            
        content = r.content
        content_type = r.headers.get('Content-Type', '').lower()
        
        # 检测格式
        format_type = detect_format(content, url, content_type)
        
        # ✅ 增加日志：如果是百川源（URL包含baichuan/bc），标记出来
        if "baichuan" in url.lower() or "bc" in url.lower():
            logging.info(f"[{index}] 🟦 检测到百川源候选: {url}")
            
        logging.info(f"[{index}] ✅ 获取成功 ({format_type.upper()}格式)")
        return content, format_type, True
        
    except requests.exceptions.Timeout:
        logging.warning(f"[{index}] ❌ 超时: {url[:50]}...")
        return None, None, False
    except requests.exceptions.ConnectionError:
        logging.warning(f"[{index}] ❌ 连接失败: {url[:50]}...")
        return None, None, False
    except Exception as e:
        logging.warning(f"[{index}] ❌ 异常: {url[:50]}... 错误: {str(e)[:50]}")
        return None, None, False

def detect_format(content, url, content_type):
    """检测内容格式"""
    # 检查是否为GZIP压缩
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                decompressed = f.read()
                # 递归检测解压后的格式
                return detect_format(decompressed, url, content_type)
        except:
            return "gzip"
    
    # 检查是否为XML
    if b'<?xml' in content[:100] or b'<tv' in content[:100] or b'<TV' in content[:100]:
        return "xml"
    
    # 检查是否为JSON
    try:
        if content.startswith(b'{') or content.startswith(b'['):
            json.loads(content.decode('utf-8', errors='ignore')[:100])
            return "json"
    except:
        pass
    
    # 检查是否为M3U格式
    if b'#EXTM3U' in content[:100]:
        return "m3u"
    
    # 检查是否为TXT格式（包含时间戳的文本）
    if b'http://' in content[:200] or b'https://' in content[:200]:
        return "txt"
    
    # 根据Content-Type判断
    if 'xml' in content_type:
        return "xml"
    elif 'json' in content_type:
        return "json"
    elif 'text' in content_type:
        return "txt"
    
    # 默认尝试解码为文本
    try:
        text = content.decode('utf-8', errors='ignore')
        if len(text) > 100 and ('http://' in text or 'https://' in text):
            return "txt"
    except:
        pass
    return "unknown"

def parse(content, format_type, index):
    """解析不同格式的内容为统一的XML结构"""
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0
    
    try:
        if format_type == "xml":
            root = etree.fromstring(content)
        elif format_type == "txt" or format_type == "m3u":
            # 尝试解析为M3U或TXT格式
            return parse_text_format(content, index)
        elif format_type == "json":
            # ✅ 尝试解析为JSON格式（包含百川源）
            return parse_json_format(content, index)
        else:
            logging.warning(f"[{index}] ⚠️ 不支持格式: {format_type}")
            return {}, [], 0, 0
    except Exception as e:
        logging.warning(f"[{index}] ❌ 解析失败 (根解析器): {str(e)[:50]}")
        return {}, [], 0, 0

    # XML 标准解析逻辑 (保持不变)
    # 解析频道
    for ch in root.xpath("//channel"):
        cid = ch.get("id")
        if cid:
            channels[cid] = ch
            channel_count += 1

    # 解析节目
    for p in root.xpath("//programme"):
        try:
            start_time = parse_program_time(p.get("start", ""))
            stop_time = parse_program_time(p.get("stop", ""))
            if not start_time:
                continue
            if start_cutoff <= start_time <= end_cutoff:
                programs.append(p)
                program_count += 1
        except Exception as e:
            continue

    logging.info(f"[{index}] 📺 频道: {channel_count} 个, 📅 节目: {program_count} 个")
    return channels, programs, channel_count, program_count

def parse_text_format(content, index):
    """解析TXT或M3U格式"""
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0
    
    try:
        text = content.decode('utf-8', errors='ignore')
        lines = text.split('\n')
        current_channel = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 解析M3U格式的频道信息
            if line.startswith('#EXTINF:'):
                # 提取频道信息
                match = re.search(r'tvg-id="([^"]+)"', line)
                if match:
                    tvg_id = match.group(1)
                    # 提取频道名称
                    name_match = re.search(r',(.+)$', line)
                    if name_match:
                        name = name_match.group(1)
                        # 创建频道元素
                        channel = etree.Element("channel")
                        channel.set("id", tvg_id)
                        display_name = etree.SubElement(channel, "display-name")
                        display_name.text = name
                        channels[tvg_id] = channel
                        channel_count += 1
                        current_channel = tvg_id
    except Exception as e:
        logging.debug(f"[{index}] M3U解析错误: {e}")
    
    return channels, programs, channel_count, program_count

def parse_json_format(content, index):
    """
    解析JSON格式，特别增加了对百川源 (Baichuan) 的支持
    百川源结构通常为: [{"tvid": "CCTV1", "name": "CCTV-1", "list": [{"time": "18:00", "program": "新闻联播"}, ...]}, ...]
    """
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0
    
    try:
        text = content.decode('utf-8', errors='ignore')
        data = json.loads(text)
        
        # ✅ 核心改动：检测是否为百川源结构
        # 检查数据是否为列表，且第一个元素包含 'tvid' 或 'name' 字段
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            if 'tvid' in first_item or 'name' in first_item:
                logging.info(f"[{index}] 🟦 正在解析百川源 (Baichuan JSON) 结构...")
                
                for item in data:
                    tvid = item.get('tvid') or item.get('id')
                    name = item.get('name')
                    program_list = item.get('list', [])
                    
                    if not tvid or not name:
                        continue
                    
                    # ✅ 生成标准的 XML Channel 节点
                    channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', tvid) # 清理ID
                    if channel_id not in channels:
                        channel_elem = etree.Element("channel")
                        channel_elem.set("id", channel_id)
                        
                        display_name = etree.SubElement(channel_elem, "display-name")
                        display_name.text = name
                        
                        # 可选：添加图标
                        icon = etree.SubElement(channel_elem, "icon")
                        icon.set("src", f"https://example.com/logo/{tvid}.png") # 这里可以替换为实际的Logo API
                        
                        channels[channel_id] = channel_elem
                        channel_count += 1
                    
                    # ✅ 解析节目单
                    for prog in program_list:
                        time_str = prog.get('time', '')
                        title = prog.get('program', '未知节目')
                        
                        # 构建标准的 XML Programme 节点
                        # 注意：百川源通常只给时间，不给日期和时区，这里需要结合当前日期
                        # 这里简化处理，假设节目是今天的或者未来几天的
                        # 实际应用中可能需要更复杂的日期逻辑
                        start_dt = datetime.combine(today, datetime.strptime(time_str, "%H:%M").time())
                        
                        # 简单的时间修正：如果时间比现在早，可能是明天的
                        if start_dt < now:
                            start_dt += timedelta(days=1)
                            
                        # 检查是否在输出范围内
                        if start_cutoff <= start_dt <= end_cutoff:
                            # 假设节目时长1小时 (Stop时间)
                            stop_dt = start_dt + timedelta(hours=1)
                            
                            prog_elem = etree.Element("programme")
                            prog_elem.set("start", start_dt.strftime("%Y%m%d%H%M%S 0")) # 0 代表时区
                            prog_elem.set("stop", stop_dt.strftime("%Y%m%d%H%M%S 0"))
                            prog_elem.set("channel", channel_id)
                            
                            title_elem = etree.SubElement(prog_elem, "title")
                            title_elem.text = title
                            
                            programs.append(prog_elem)
                            program_count += 1
                
                logging.info(f"[{index}] ✅ 百川源解析完成: {channel_count} 频道, {program_count} 节目")
                return channels, programs, channel_count, program_count
        
        # 如果不是百川源，尝试其他 JSON 结构 (基础实现)
        logging.info(f"[{index}] ⚠️ JSON格式非百川结构，跳过解析")
        
    except Exception as e:
        logging.error(f"[{index}] ❌ JSON解析失败 (百川源): {str(e)}")
    
    return channels, programs, channel_count, program_count

def parse_program_time(time_str):
    """
    解析节目时间，支持多种格式
    修改：增加了对 "YYYY-MM-DD HH:MM" 格式的支持 (百川源常见格式)
    """
    if not time_str:
        return None
        
    try:
        # 移除时区信息
        time_part = time_str.split()[0] if ' ' in time_str else time_str
        
        # ✅ 增加支持：支持 "YYYY-MM-DD HH:MM" 格式
        if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_part):
            return datetime.strptime(time_part, "%Y-%m-%d %H:%M")
            
        # 支持标准的 YYYYMMDDHHMMSS 格式
        if len(time_part) >= 14:
            return datetime.strptime(time_part[:14], "%Y%m%d%H%M%S")
            
        # 支持短日期格式
        elif len(time_part) >= 8:
            for fmt in ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(time_part[:len(fmt)], fmt)
                except ValueError:
                    continue
                    
    except Exception as e:
        logging.debug(f"时间解析错误: {time_str} -> {e}")
        pass
        
    return None

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.error(f"❌ 配置文件 {CONFIG_FILE} 不存在")
        return []
        
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        urls = [ line.strip() for line in f if line.strip() and not line.startswith("#") ]
        
    logging.info(f"📋 读取到 {len(urls)} 个EPG源")
    return urls

def main():
    start_time = datetime.now()
    logging.info("=" * 60)
    logging.info("🚀 开始EPG生成任务（多格式支持版 - 增强百川源解析）")
    logging.info(f"📅 时间范围: {start_cutoff.date()} 至 {end_cutoff.date()}")
    logging.info("=" * 60)
    
    urls = read_config()
    if not urls:
        logging.error("❌ 没有可用的EPG源，退出")
        return
        
    all_channels = {}
    all_programs = []
    source_stats = []

    for i, url in enumerate(urls, 1):
        content, format_type, success = fetch(url, i)
        if success and content and format_type:
            channels, programs, ch_count, prog_count = parse(content, format_type, i)
            
            # 合并频道（不重复）
            for cid, ch in channels.items():
                if cid not in all_channels:
                    all_channels[cid] = ch
                    
            # 合并节目
            all_programs.extend(programs)
            
            source_stats.append({
                'index': i,
                'url': url[:50],
                'status': '✅ 成功',
                'format': format_type,
                'channels': ch_count,
                'programs': prog_count
            })
        else:
            source_stats.append({
                'index': i,
                'url': url[:50],
                'status': '❌ 失败',
                'format': 'unknown',
                'channels': 0,
                'programs': 0
            })

    # 输出源状态汇总
    logging.info("=" * 60)
    logging.info("📊 EPG源状态汇总")
    logging.info("=" * 60)
    success_count = 0
    for stat in source_stats:
        status_icon = "✅" if stat['status'] == '✅ 成功' else "❌"
        logging.info(f"[{stat['index']}] {status_icon} {stat['status']} | "
                     f"📁 {stat['format']:6} | 📺 {stat['channels']:3} 频道 | 📅 {stat['programs']:5} 节目")
        if stat['status'] == '✅ 成功':
            success_count += 1
            
    logging.info("-" * 60)
    logging.info(f"总计: {success_count}/{len(urls)} 个源成功")

    # 生成EPG文件
    if all_channels and all_programs:
        root = etree.Element("tv")
        root.set("generator-info-name", "EPG多格式合并器 - 增强版")
        root.set("date", datetime.now().strftime("%Y%m%d%H%M%S"))
        
        for ch in all_channels.values():
            root.append(ch)
        for p in all_programs:
            root.append(p)
            
        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        with gzip.open(out_path, "wb") as f:
            f.write(xml_bytes)
            
        # 计算文件大小
        file_size = os.path.getsize(out_path)
        file_size_kb = file_size / 1024
        elapsed = datetime.now() - start_time
        
        logging.info("=" * 60)
        logging.info(f"✅ EPG生成完成!")
        logging.info(f"📁 文件: {out_path}")
        logging.info(f"📦 大小: {file_size_kb:.1f} KB")
        logging.info(f"📺 总频道: {len(all_channels)} 个")
        logging.info(f"📅 总节目: {len(all_programs)} 个")
        logging.info(f"⏱️ 耗时: {elapsed.total_seconds():.1f} 秒")
        
        # 显示格式统计
        format_stats = {}
        for stat in source_stats:
            if stat['status'] == '✅ 成功':
                fmt = stat['format']
                format_stats[fmt] = format_stats.get(fmt, 0) + 1
                
        if format_stats:
            logging.info(f"📊 格式统计: {', '.join([f'{k}:{v}' for k, v in format_stats.items()])}")
        logging.info("=" * 60)
        
    else:
        logging.error("❌ 没有有效的频道或节目，无法生成EPG")

if __name__ == "__main__":
    main()
