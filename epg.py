import os
import sys
import gzip
import json
import time
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import urlparse

# ================= 配置区域 =================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "epg.xml.gz")
# 超时时间（秒）
TIMEOUT = 30 
# 日志级别
LOG_LEVEL = logging.INFO
# ============================================

# 设置日志格式
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class EPGMerger:
    def __init__(self):
        # 使用字典存储数据，key为channel_id，value为xml元素列表，用于去重和合并
        self.programs = {} 
        self.channels_info = {} # 存储channel display-name信息
        self.session = requests.Session()
        # 伪装请求头，防止部分源拒绝Python默认请求
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        })

    def fetch_content(self, url):
        """下载内容，自动处理Gzip"""
        try:
            logger.info(f"正在获取: {url}")
            response = self.session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # 自动判断是否Gzip
            if response.headers.get('Content-Encoding') == 'gzip' or url.endswith('.gz'):
                try:
                    with gzip.GzipFile(fileobj=response.raw) as f:
                        content = f.read()
                    logger.debug("检测到Gzip压缩，已解压")
                except:
                    content = response.content
            else:
                content = response.content
            
            # 尝试检测编码
            encoding = response.encoding if 'utf-8' in response.encoding.lower() else 'utf-8'
            return content.decode(encoding, errors='ignore')
            
        except Exception as e:
            logger.error(f"❌ 下载失败: {url} | 错误: {e}")
            return None

    def parse_time(self, time_str):
        """
        统一时间格式转换
        输入可能是: 20260428154500 (XMLTV标准) 或 2026-04-28 15:45 (百川/JSON常见)
        输出: 20260428154500 +0800
        """
        try:
            # 处理百川源常见格式: 2026-04-28 15:45:00 或 2026-04-28 15:45
            if '-' in time_str:
                time_str = time_str.strip().split('.')[0] # 去掉毫秒
                if len(time_str) == 16: # YYYY-MM-DD HH:MM
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
                elif len(time_str) == 19: # YYYY-MM-DD HH:MM:SS
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                else:
                    return None
            else:
                # 处理标准XMLTV格式: YYYYMMDDHHMMSS
                dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            
            return dt.strftime("%Y%m%d%H%M%S") + " +0800"
        except Exception as e:
            return None

    def parse_baichuan_json(self, data, source_name):
        """解析百川源 (JSON格式)"""
        try:
            json_data = json.loads(data)
            count_p = 0
            count_c = 0
            
            # 百川源通常是一个列表或者包含list的字典
            # 这里做一个通用的遍历逻辑
            channels = json_data if isinstance(json_data, list) else json_data.get('channels', []) or json_data.get('list', [])
            
            # 如果根节点直接包含 tvid，说明只有一个频道
            if 'tvid' in json_data:
                channels = [json_data]

            for ch in channels:
                if not isinstance(ch, dict): continue
                
                # 获取频道ID (百川源常用 tvid 或 id)
                ch_id = str(ch.get('tvid') or ch.get('id') or ch.get('channel_id'))
                if not ch_id: continue
                
                # 获取频道名称
                ch_name = ch.get('name') or ch.get('channel_name')
                if not ch_name: continue
                
                # 记录频道信息
                if ch_id not in self.channels_info:
                    self.channels_info[ch_id] = ch_name
                    count_c += 1
                
                # 获取节目列表 (百川源常用 list, programs, epg_data)
                programs = ch.get('list') or ch.get('programs') or ch.get('epg_data') or []
                
                for p in programs:
                    if not isinstance(p, dict): continue
                    
                    start_raw = p.get('start') or p.get('time') or p.get('showtime')
                    title = p.get('title') or p.get('program') or p.get('name')
                    
                    if not start_raw or not title: continue
                    
                    start_time = self.parse_time(str(start_raw))
                    if not start_time: continue
                    
                    # 计算结束时间 (百川源通常有 duration 或 end，如果没有则估算)
                    end_time = None
                    if p.get('end'):
                        end_time = self.parse_time(str(p.get('end')))
                    elif p.get('duration'):
                        # 简单处理 duration (假设单位是分钟)
                        try:
                            dur = int(p.get('duration'))
                            start_dt = datetime.strptime(start_time[:-6], "%Y%m%d%H%M%S")
                            end_dt = start_dt + timedelta(minutes=dur)
                            end_time = end_dt.strftime("%Y%m%d%H%M%S") + " +0800"
                        except: pass
                    
                    # 如果没有结束时间，默认1小时
                    if not end_time:
                        start_dt = datetime.strptime(start_time[:-6], "%Y%m%d%H%M%S")
                        end_dt = start_dt + timedelta(hours=1)
                        end_time = end_dt.strftime("%Y%m%d%H%M%S") + " +0800"

                    # 构建 XML 结构
                    prog_elem = ET.Element("programme", {
                        "start": start_time,
                        "stop": end_time,
                        "channel": ch_id
                    })
                    
                    title_elem = ET.SubElement(prog_elem, "title")
                    title_elem.text = str(title)
                    title_elem.set("lang", "zh")
                    
                    # 如果有简介
                    if p.get('desc'):
                        desc_elem = ET.SubElement(prog_elem, "desc")
                        desc_elem.text = str(p.get('desc'))
                        desc_elem.set("lang", "zh")

                    # 存入字典
                    if ch_id not in self.programs:
                        self.programs[ch_id] = []
                    self.programs[ch_id].append(prog_elem)
                    count_p += 1
            
            logger.info(f"✅ 解析成功: {source_name} (百川源格式) - 频道: {count_c}, 节目: {count_p}")
            return True

        except Exception as e:
            logger.debug(f"非百川源格式或解析失败: {e}")
            return False

    def parse_standard_xml(self, data, source_name):
        """解析标准 XMLTV"""
        try:
            # 修复一些常见的XML错误 (如未转义的 &)
            data = data.replace('&', '&amp;').replace('&amp;amp;', '&amp;')
            
            root = ET.fromstring(data)
            count_p = 0
            count_c = 0
            
            # 解析频道
            for channel in root.findall("channel"):
                ch_id = channel.get("id")
                if ch_id and ch_id not in self.channels_info:
                    display_name = channel.find("display-name")
                    name = display_name.text if display_name is not None else ch_id
                    self.channels_info[ch_id] = name
                    count_c += 1
            
            # 解析节目
            for prog in root.findall("programme"):
                ch_id = prog.get("channel")
                if not ch_id: continue
                
                # 简单复制节点
                # 注意：这里为了性能直接引用，实际合并时可能需要深拷贝，但ElementTree处理大文件时深拷贝很慢
                # 此处采用重建或移动策略，这里简化为重建关键信息或直接append
                
                # 如果是跨源合并，需要确保channel ID存在
                # 如果原XML里有channel定义但上面没抓到，这里补一下
                if ch_id not in self.channels_info:
                     # 尝试寻找display-name
                     dn = prog.find("title") # 近似处理
                     self.channels_info[ch_id] = ch_id 

                if ch_id not in self.programs:
                    self.programs[ch_id] = []
                
                self.programs[ch_id].append(prog)
                count_p += 1
                
            logger.info(f"✅ 解析成功: {source_name} (XML格式) - 频道: {count_c}, 节目: {count_p}")
            return True
        except Exception as e:
            logger.error(f"❌ XML解析失败: {source_name} | 错误: {e}")
            return False

    def process_source(self, url):
        """处理单个源"""
        content = self.fetch_content(url)
        if not content:
            return False
        
        # 自动识别格式
        # 1. 百川源通常是 JSON
        # 2. 标准源是 XML
        content_stripped = content.strip()
        
        if content_stripped.startswith('{') or content_stripped.startswith('['):
            # 可能是JSON (百川源)
            success = self.parse_baichuan_json(content, url)
            if success: return True
            
        # 尝试XML解析
        # 有时候百川源可能返回的是XML字符串包裹在JSON里，或者纯XML
        # 这里做一个兜底
        if '<tv' in content_stripped[:100]: # 简单检查根节点
            return self.parse_standard_xml(content, url)
            
        logger.warning(f"⚠️ 未知格式或解析全失败: {url}")
        return False

    def save(self):
        """生成最终文件并Gzip压缩"""
        logger.info("开始生成合并文件...")
        
        # 构建根节点
        root = ET.Element("tv")
        root.set("generator-info-name", "EPG-Merger-Python")
        root.set("generator-info-url", "github.com")
        
        # 添加频道信息
        for ch_id, ch_name in self.channels_info.items():
            ch_elem = ET.SubElement(root, "channel", {"id": ch_id})
            dn_elem = ET.SubElement(ch_elem, "display-name")
            dn_elem.text = ch_name
            dn_elem.set("lang", "zh")
            
        # 添加节目信息
        total_p = 0
        for ch_id, programs in self.programs.items():
            for prog in programs:
                # 确保channel属性正确
                prog.set("channel", ch_id)
                root.append(prog)
                total_p += 1
        
        # 写入Gzip
        tree = ET.ElementTree(root)
        
        # 使用Gzip压缩写入
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
            # ElementTree 的 write 方法不直接支持 file object 的 text mode with gzip in some envs
            # 所以这里稍微绕一下，先转字符串再写入，或者使用 bytes
            # 为了兼容性和内存，我们直接写入 bytes
            pass 

        # 更稳妥的写法：
        xml_str = ET.tostring(root, encoding='utf-8', method='xml')
        with gzip.open(OUTPUT_FILE, 'wb') as f:
            f.write(xml_str)
            
        logger.info(f"🎉 完成！文件已保存至: {OUTPUT_FILE}")
        logger.info(f"📊 统计: 频道 {len(self.channels_info)} 个, 节目 {total_p} 个")

if __name__ == "__main__":
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"配置文件 {CONFIG_FILE} 不存在！")
        sys.exit(1)
        
    merger = EPGMerger()
    
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    valid_sources = 0
    for line in lines:
        url = line.strip()
        if url and not url.startswith('#'):
            if merger.process_source(url):
                valid_sources += 1
            time.sleep(0.5) # 避免请求过快
            
    if valid_sources > 0:
        merger.save()
    else:
        logger.error("没有成功解析到任何有效的数据源。")
