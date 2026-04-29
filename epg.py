import os
import re
import gzip
import json
import argparse
import requests
from lxml import etree
from datetime import datetime, timedelta
from dateutil import parser

# ================= 配置区域 =================
CONFIG_FILE = 'config.txt'
OUTPUT_DIR = 'output'
FIXED_FILENAME = 'epg.xml.gz'  # 默认固定文件名

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
    return datetime.utcnow() + timedelta(hours=8)

def parse_program_time(time_str):
    """解析时间字符串"""
    if not time_str:
        return None
    try:
        if 'T' in time_str:
            dt = parser.isoparse(time_str)
        else:
            time_str = str(time_str).strip()
            if len(time_str) == 10:
                time_str += " 12:00:00"
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        
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
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"✅ [{index}] 下载成功 ({len(response.content)} bytes)")
        return response.content, content_type
    except requests.RequestException as e:
        logger.error(f"❌ [{index}] 下载失败: {str(e)}")
        return None, None

def parse_xml(content):
    """解析 XML 格式"""
    try:
        if content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
            
        root = etree.fromstring(content)
        channels = {}
        programs = []
        
        for channel in root.findall('channel'):
            channel_id = channel.get('id')
            if channel_id:
                channels[channel_id] = channel
                
        for programme in root.findall('programme'):
            start_str = programme.get('start')
            if start_str:
                start_dt = parse_program_time(start_str)
                if start_dt and start_dt < get_current_time() - timedelta(days=7):
                    continue
            programs.append(programme)
            
        return channels, programs
    except Exception as e:
        logger.error(f"❌ XML解析错误: {str(e)}")
        return {}, []

def parse_baichuan(content):
    """解析百川 JSON 格式"""
    try:
        data = json.loads(content)
        if not isinstance(data, list):
            data = data.get('data', [])
            
        channels = {}
        programs = []
        
        for item in data:
            cid = item.get('tvg-id') or item.get('id') or item.get('channelId')
            name = item.get('tvg-name') or item.get('name')
            
            if not cid or not name:
                continue
                
            if cid not in channels:
                channel_elem = etree.Element("channel")
                channel_elem.set("id", cid)
                display_elem = etree.SubElement(channel_elem, "display-name")
                display_elem.text = name
                icon_elem = etree.SubElement(channel_elem, "icon")
                icon_elem.set("src", item.get('logo', ''))
                channels[cid] = channel_elem
            
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
                    
                if start_dt < get_current_time() - timedelta(days=7):
                    continue
                    
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

def clean_output_dir():
    """清理输出目录"""
    if os.path.exists(OUTPUT_DIR):
        for file in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                    logger.info(f"🧹 清理旧文件: {file_path}")
            except Exception as e:
                logger.error(f'❌ 清理文件失败: {e}')

def main():
    parser = argparse.ArgumentParser(description='EPG生成器')
    parser.add_argument('--mode', choices=['xml', 'baichuan', 'both'], default='both')
    args = parser.parse_args()
    
    logger.info("🚀 开始EPG生成任务")
    
    # 清理输出目录
    clean_output_dir()
    
    # 确保输出目录存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"📂 创建输出目录: {OUTPUT_DIR}")
        
    # 读取配置
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"❌ 找不到配置文件: {CONFIG_FILE}")
        return
        
    # 读取config.txt，获取固定文件名和EPG源URL
    fixed_filename = FIXED_FILENAME
    urls = []
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if line.startswith('fixed_filename='):
                    fixed_filename = line.split('=', 1)[1].strip()
                elif line.startswith('output/'):
                    continue
                else:
                    urls.append(line)
        
    if not urls:
        logger.error("❌ 配置文件中没有有效的URL")
        return
        
    logger.info(f"📄 共读取到 {len(urls)} 个源")
    
    # 合并数据
    all_channels = {}
    all_programs = []
    
    for i, url in enumerate(urls):
        content, content_type = download_url(url, i+1)
        if not content:
            continue
            
        parsed_channels = {}
        parsed_programs = []
        
        # 智能识别格式
        if 'json' in content_type.lower():
            logger.info(f"🔍 [{i+1}] 识别为 JSON 格式")
            parsed_channels, parsed_programs = parse_baichuan(content)
        else:
            try:
                decompressed = gzip.decompress(content)
                root = etree.fromstring(decompressed)
                if root.tag == 'tv':
                    logger.info(f"🔍 [{i+1}] 识别为 XML (GZIP) 格式")
                    parsed_channels, parsed_programs = parse_xml(decompressed)
            except:
                try:
                    root = etree.fromstring(content)
                    if root.tag == 'tv':
                        logger.info(f"🔍 [{i+1}] 识别为 XML 格式")
                        parsed_channels, parsed_programs = parse_xml(content)
                except:
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
        return
        
    logger.info(f"📊 总计: {len(all_channels)} 个频道, {len(all_programs)} 个节目")
    
    # 生成 XML 文件（固定文件名）
    xml_path = os.path.join(OUTPUT_DIR, fixed_filename)
    try:
        root = etree.Element("tv")
        root.set("generator-info-name", "EPG-Merger")
        root.set("date", datetime.now().strftime("%Y%m%d%H%M%S +0800"))
        
        for cid in sorted(all_channels.keys()):
            root.append(all_channels[cid])
            
        for prog in all_programs:
            root.append(prog)
            
        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        
        with gzip.open(xml_path, "wb") as f:
            f.write(xml_bytes)
            
        size_kb = os.path.getsize(xml_path) / 1024
        logger.info(f"✅ 成功生成 XML: {xml_path} ({size_kb:.1f} KB)")
    except Exception as e:
        logger.error(f"❌ 写入 XML 失败: {str(e)}")
    
    # 生成百川 JSON（可选）
    if args.mode in ['baichuan', 'both']:
        try:
            baichuan_path = os.path.join(OUTPUT_DIR, 'epg_baichuan.json')
            baichuan_data = []
            for cid, chan_elem in all_channels.items():
                chan_info = {
                    "id": cid,
                    "name": chan_elem.findtext('display-name'),
                    "logo": chan_elem.find('icon').get('src') if chan_elem.find('icon') is not None else "",
                    "programmes": []
                }
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
