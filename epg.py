import os
import re
import sys
import json
import gzip
import shutil
from datetime import datetime, timedelta
from collections import OrderedDict
from urllib.parse import urlparse

# 第三方库导入
try:
    from lxml import etree
    import requests
    from dateutil import parser as date_parser
except ImportError as e:
    print(f"❌ 缺少依赖库: {e}")
    print("💡 请确保已安装: pip install lxml requests python-dateutil")
    sys.exit(1)

# ================= 配置区域 =================
CONFIG_FILE = 'config.txt'  # 配置文件名
OUTPUT_DIR = 'output'       # 输出目录
XML_OUTPUT_FILE = f"epg_{datetime.now().strftime('%Y%m%d_%H%M')}.xml.gz"  # 带时间戳的文件名
BAICHUAN_OUTPUT_FILE = 'epg_baichuan.json'

# 默认时间范围（天）
DEFAULT_DAYS = 7

# 请求超时时间
TIMEOUT = 30

# 日志设置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ================= 工具函数 =================

def get_current_time():
    """获取当前北京时间"""
    # GitHub Actions 默认可能是 UTC，这里强制转为 +0800
    return datetime.utcnow() + timedelta(hours=8)

def parse_program_time(time_str):
    """解析时间字符串"""
    if not time_str:
        return None
    try:
        # 尝试多种格式
        if 'T' in time_str:
            dt = date_parser.isoparse(time_str)
        else:
            # 处理 "2026-04-29 12:00:00" 或 "2026-04-29"
            time_str = str(time_str).strip()
            if len(time_str) == 10: # 只有日期
                time_str += " 12:00:00"
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        
        # 如果没有时区信息，假设是北京时间 (+0800)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
            
        return dt
    except Exception:
        return None

def download_url(url, index):
    """下载URL内容"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        logger.info(f"🌐 [{index}] 正在下载: {url[:50]}...")
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"✅ [{index}] 下载成功 ({len(response.content)} bytes, Type: {content_type})")
        return response.content, content_type
    except requests.RequestException as e:
        logger.error(f"❌ [{index}] 下载失败: {str(e)}")
        return None, None

def parse_xml(content):
    """解析 XML 格式"""
    try:
        # 如果是 gzip 压缩的，先解压
        if content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
            
        root = etree.fromstring(content)
        channels = OrderedDict()
        programs = []
        
        # 解析频道
        for channel in root.findall('channel'):
            channel_id = channel.get('id')
            if channel_id:
                channels[channel_id] = channel
                
        # 解析节目
        for programme in root.findall('programme'):
            # 过滤过期节目
            start_str = programme.get('start')
            if start_str:
                start_dt = parse_program_time(start_str)
                if start_dt and start_dt < get_current_time() - timedelta(days=DEFAULT_DAYS):
                    continue
            programs.append(programme)
            
        return channels, programs
    except Exception as e:
        logger.error(f"❌ XML解析错误: {str(e)}")
        return {}, []

def parse_baichuan(content):
    """解析 百川 JSON 格式 """
    try:
        data = json.loads(content)
        if not isinstance(data, list):
            data = data.get('data', [])
            
        channels = OrderedDict()
        programs = []
        
        for item in data:
            cid = item.get('tvg-id') or item.get('id') or item.get('channelId')
            name = item.get('tvg-name') or item.get('name')
            
            if not cid or not name:
                continue
                
            # 构建频道节点
            if cid not in channels:
                channel_elem = etree.Element("channel")
                channel_elem.set("id", cid)
                
                display_elem = etree.SubElement(channel_elem, "display-name")
                display_elem.text = name
                
                icon_elem = etree.SubElement(channel_elem, "icon")
                icon_elem.set("src", item.get('logo', ''))
                
                channels[cid] = channel_elem
            
            # 构建节目节点
            prog_list = item.get('programmes') or item.get('items') or []
            if not isinstance(prog_list, list):
                continue
                
            for prog in prog_list:
                title = prog.get('title')
                time_str = prog.get('time') or prog.get('start')
                
                if not title or not time_str:
                    continue
                    
                start_dt = parse_program_time(time_str)
                if not start_dt:
                    continue
                    
                # 过滤过期
                if start_dt < get_current_time() - timedelta(days=DEFAULT_DAYS):
                    continue
                    
                # 计算结束时间
                stop_dt = None
                if 'stop' in prog:
                    stop_dt = parse_program_time(prog['stop'])
                elif 'duration' in prog:
                    try:
                        dur_sec = int(prog['duration'])
                        stop_dt = start_dt + timedelta(seconds=dur_sec)
                    except:
                        stop_dt = start_dt + timedelta(minutes=30)
                else:
                    stop_dt = start_dt + timedelta(minutes=30)
                
                prog_elem = etree.Element("programme")
                prog_elem.set("start", start_dt.strftime("%Y%m%d%H%M%S +0800"))
                prog_elem.set("stop", stop_dt.strftime("%Y%m%d%H%M%S +0800"))
                prog_elem.set("channel", cid)
                
                title_elem = etree.SubElement(prog_elem, "title")
                title_elem.text = title
                
                programs.append(prog_elem)
                
        return channels, programs
    except Exception as e:
        logger.error(f"❌ 百川解析错误: {str(e)}")
        return {}, []

def clean_old_outputs():
    """清理旧的缓存和输出，防止 GitHub Actions 还原旧文件 """
    # 1. 清理 output 目录下的旧文件（保留目录本身，防止 actions/cache 报错）
    if os.path.exists(OUTPUT_DIR):
        for file in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, file)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    logger.info(f"🧹 清理旧文件: {file_path}")
            except Exception as e:
                logger.error(f'❌ 清理文件 {file_path} 失败: {e}')
    
    # 2. 强制清理可能的缓存标记（配合 .gitignore 使用）
    # 这一步是为了防止 Actions 把空的 output 目录缓存下来
    gitignore_path = '.gitignore'
    if os.path.exists(gitignore_path):
        with open(gitignore_path, 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            f.truncate()
            for line in lines:
                if line.strip() != OUTPUT_DIR:
                    f.write(line)

def main():
    logger.info("🚀 开始EPG生成任务")
    
    # 0. 清理环境
    clean_old_outputs()
    
    # 1. 确保输出目录存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"📂 创建输出目录: {OUTPUT_DIR}")
        
    # 2. 读取配置
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"❌ 找不到配置文件: {CONFIG_FILE}")
        sys.exit(1)
        
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
    if not urls:
        logger.error("❌ 配置文件中没有有效的URL")
        sys.exit(1)
        
    logger.info(f"📄 共读取到 {len(urls)} 个源")
    
    # 3. 合并数据
    all_channels = OrderedDict()
    all_programs = []
    
    for i, url in enumerate(urls):
        content, content_type = download_url(url, i+1)
        if not content:
            continue
            
        # 智能识别格式
        parsed_channels = {}
        parsed_programs = []
        
        # 通过 Content-Type 判断
        if 'json' in content_type.lower():
            logger.info(f"🔍 [{i+1}] 识别为 JSON 格式")
            parsed_channels, parsed_programs = parse_baichuan(content)
        else:
            # 尝试解压判断是否为 XML GZIP
            try:
                decompressed = gzip.decompress(content)
                root = etree.fromstring(decompressed)
                if root.tag == 'tv':
                    logger.info(f"🔍 [{i+1}] 识别为 XML (GZIP) 格式")
                    parsed_channels, parsed_programs = parse_xml(decompressed)
            except:
                # 不是 gzip，按普通 XML 解析
                try:
                    root = etree.fromstring(content)
                    if root.tag == 'tv':
                        logger.info(f"🔍 [{i+1}] 识别为 XML 格式")
                        parsed_channels, parsed_programs = parse_xml(content)
                except:
                    # 可能是 Baichuan 裸 JSON
                    try:
                        text_content = content.decode('utf-8')
                        data = json.loads(text_content)
                        if isinstance(data, list) and len(data) > 0 and 'programmes' in data[0]:
                            logger.info(f"🔍 [{i+1}] 识别为 百川 JSON 格式")
                            parsed_channels, parsed_programs = parse_baichuan(text_content)
                    except:
                        logger.warning(f"⚠️ [{i+1}] 无法识别格式，跳过")
                        continue
        
        if parsed_channels:
            all_channels.update(parsed_channels)
            all_programs.extend(parsed_programs)
            logger.info(f"✅ [{i+1}] 合并成功: +{len(parsed_channels)}频道, +{len(parsed_programs)}节目")
        else:
            logger.warning(f"⚠️ [{i+1}] 未解析出有效数据")
            
    if not all_channels:
        logger.error("❌ 没有任何有效数据，退出")
        sys.exit(1)
        
    logger.info(f"📊 总计: {len(all_channels)} 个频道, {len(all_programs)} 个节目")
    
    # 4. 生成 XML 文件 (带时间戳，防止缓存)
    xml_path = os.path.join(OUTPUT_DIR, XML_OUTPUT_FILE)
    
    try:
        root = etree.Element("tv")
        root.set("generator-info-name", "EPG-Merger")
        root.set("date", datetime.now().strftime("%Y%m%d%H%M%S +0800"))
        
        # 排序频道 ID 以保持稳定
        for cid in sorted(all_channels.keys()):
            root.append(all_channels[cid])
            
        for prog in all_programs:
            root.append(prog)
            
        # 使用 Gzip 压缩
        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        
        with gzip.open(xml_path, "wb") as f:
            f.write(xml_bytes)
            
        size_kb = os.path.getsize(xml_path) / 1024
        logger.info(f"✅ 成功生成 XML: {xml_path} ({size_kb:.1f} KB)")
        
    except Exception as e:
        logger.error(f"❌ 写入 XML 失败: {str(e)}")
        sys.exit(1)
        
    # 5. 生成 Baichuan JSON (可选)
    try:
        baichuan_path = os.path.join(OUTPUT_DIR, BAICHUAN_OUTPUT_FILE)
        # 转换回百川格式供参考
        baichuan_data = []
        for cid, chan_elem in all_channels.items():
            chan_info = {
                "id": cid,
                "name": chan_elem.findtext('display-name'),
                "logo": chan_elem.findtext('icon[@src]'),
                "programmes": []
            }
            # 提取该频道的节目
            for prog_elem in all_programs:
                if prog_elem.get('channel') == cid:
                    chan_info['programmes'].append({
                        "title": prog_elem.findtext('title'),
                        "start": prog_elem.get('start'),
                        "stop": prog_elem.get('stop')
                    })
            baichuan_data.append(chan_info)
            
        with open(baichuan_path, 'w', encoding='utf-8') as f:
            json.dump(baichuan_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"✅ 成功生成 百川JSON: {baichuan_path}")
        
    except Exception as e:
        logger.error(f"❌ 写入 JSON 失败: {str(e)}")
        
    logger.info("🎉 任务全部完成！")

if __name__ == "__main__":
    main()
