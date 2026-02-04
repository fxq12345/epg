import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 3
TIMEOUT = 30
CORE_RETRY_COUNT = 2

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 核心频道配置（扩展高清和4K频道支持）
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("央视高清", ["CCTV", "高清", "HD"]),
    ("卫视4K", ["4K", "超高清", "UHD"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 扩展酷9专用ID映射表（添加高清和4K频道）
COOL9_ID_MAPPING = {
    # 山东本地频道
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", 
    "382": "山东农科", "383": "山东齐鲁", "384": "山东文旅",
    
    # 央视常规频道
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4", 
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10", "11": "CCTV11", "12": "CCTV12",
    "13": "CCTV13", "14": "CCTV14", "15": "CCTV15", "16": "CCTV16",
    "17": "CCTV17",
    
    # 央视高清频道（补充完整高清频道映射）
    "101": "CCTV1高清", "102": "CCTV2高清", "103": "CCTV3高清",
    "104": "CCTV4高清", "105": "CCTV5高清", "106": "CCTV6高清",
    "107": "CCTV7高清", "108": "CCTV8高清", "109": "CCTV9高清",
    "110": "CCTV10高清", "111": "CCTV11高清", "112": "CCTV12高清",
    "113": "CCTV13高清", "114": "CCTV14高清", "115": "CCTV15高清",
    "116": "CCTV16高清", "117": "CCTV17高清",
    
    # 4K超高清频道（完整补充）
    "201": "CCTV4K", "202": "北京卫视4K", "203": "湖南卫视4K",
    "204": "浙江卫视4K", "205": "江苏卫视4K", "206": "东方卫视4K",
    "207": "广东卫视4K", "208": "深圳卫视4K", "209": "山东卫视4K"
}

# 高清/4K频道回退机制：当高清频道无节目时使用标清频道节目单
HD_SD_MAPPING = {
    "CCTV1高清": "CCTV1", "CCTV2高清": "CCTV2", "CCTV3高清": "CCTV3",
    "CCTV4高清": "CCTV4", "CCTV5高清": "CCTV5", "CCTV6高清": "CCTV6",
    "CCTV7高清": "CCTV7", "CCTV8高清": "CCTV8", "CCTV9高清": "CCTV9",
    "CCTV10高清": "CCTV10", "CCTV11高清": "CCTV11", "CCTV12高清": "CCTV12",
    "CCTV13高清": "CCTV13", "CCTV14高清": "CCTV14", "CCTV15高清": "CCTV15",
    "CCTV16高清": "CCTV16", "CCTV17高清": "CCTV17",
    "CCTV4K": "CCTV4",
    "北京卫视4K": "北京卫视", "湖南卫视4K": "湖南卫视", "浙江卫视4K": "浙江卫视",
    "江苏卫视4K": "江苏卫视", "东方卫视4K": "东方卫视", "广东卫视4K": "广东卫视",
    "深圳卫视4K": "深圳卫视", "山东卫视4K": "山东卫视"
}

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.priority_channels = {cat[0]: [] for cat in CHANNEL_PRIORITY}
        self.other_channels: List = []
        self.all_programs: List = []
        self.channel_programs_map: Dict[str, List] = {}  # 频道到节目单的映射
        
    def _create_session(self) -> requests.Session:
        """创建带重试机制的会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT + 2,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def read_epg_sources(self) -> List[str]:
        """读取配置文件中的EPG源"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            raise FileNotFoundError(f"找不到配置文件: {CONFIG_FILE}")
            
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                sources = []
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if line.startswith(("http://", "https://")):
                            sources.append(line)
                        else:
                            logging.warning(f"第{line_num}行格式错误，已跳过: {line}")
                
                if len(sources) < 3:
                    logging.warning(f"仅找到{len(sources)}个有效EPG源，建议至少配置3个")
                
                return sources[:8]
                
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容中的无效字符"""
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, any]:
        """并发获取单个EPG源数据"""
        try:
            start_time = time.time()
            logging.info(f"开始抓取: {source}")
            
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功抓取: {source} | 耗时: {cost_time:.2f}s")
            return True, source, xml_tree
            
        except Exception as e:
            logging.error(f"抓取失败 {source}: {str(e)}")
            return False, source, None

    def process_channels(self, xml_tree, source: str) -> int:
        """处理频道数据"""
        channels = xml_tree.xpath("//channel")
        shandong_count = 0
        
        for channel in channels:
            cid = channel.get("id", "").strip()
            if not cid:
                continue
                
            # 应用酷9ID映射
            original_cid = cid
            if cid in COOL9_ID_MAPPING:
                cid = COOL9_ID_MAPPING[cid]
                
            if cid in self.channel_ids:
                continue
                
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else ""
            
            # 更新频道ID
            channel.set("id", cid)
            self.channel_ids.add(cid)
            
            # 按优先级分类
            channel_added = False
            for cat_name, keywords in CHANNEL_PRIORITY:
                if any(kw in channel_name for kw in keywords):
                    self.priority_channels[cat_name].append(channel)
                    channel_added = True
                    if "山东" in channel_name:
                        shandong_count += 1
                    break
                    
            if not channel_added:
                self.other_channels.append(channel)
                
        return shandong_count

    def process_programs(self, xml_tree):
        """处理节目单数据，建立频道到节目的映射"""
        programs = xml_tree.xpath("//programme")
        for program in programs:
            channel_id = program.get("channel", "")
            # 应用频道ID映射
            if channel_id in COOL9_ID_MAPPING:
                channel_id = COOL9_ID_MAPPING[channel_id]
            program.set("channel", channel_id)
            
            # 建立频道到节目的映射
            if channel_id not in self.channel_programs_map:
                self.channel_programs_map[channel_id] = []
            self.channel_programs_map[channel_id].append(program)
            
        self.all_programs.extend(programs)

    def enhance_hd_programs(self):
        """增强高清和4K频道节目单：为缺少节目单的高清频道添加标清频道的节目"""
        logging.info("🔧 增强高清/4K频道节目单...")
        enhanced_count = 0
        
        for hd_channel, sd_channel in HD_SD_MAPPING.items():
            # 如果高清频道没有节目单，但标清频道有节目单
            if (hd_channel in self.channel_ids and 
                hd_channel not in self.channel_programs_map and 
                sd_channel in self.channel_programs_map):
                
                logging.info(f"  为 {hd_channel} 添加 {sd_channel} 的节目单")
                sd_programs = self.channel_programs_map[sd_channel]
                
                # 复制标清频道的节目单到高清频道
                for program in sd_programs:
                    # 深拷贝节目元素
                    program_str = etree.tostring(program, encoding='unicode')
                    new_program = etree.fromstring(program_str)
                    new_program.set("channel", hd_channel)
                    self.all_programs.append(new_program)
                    
                    if hd_channel not in self.channel_programs_map:
                        self.channel_programs_map[hd_channel] = []
                    self.channel_programs_map[hd_channel].append(new_program)
                
                enhanced_count += 1
        
        logging.info(f"✅ 已增强 {enhanced_count} 个高清/4K频道的节目单")

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源数据并处理"""
        successful_sources = 0
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {
                executor.submit(self.fetch_single_source, source): source 
                for source in sources
            }
            
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, _, xml_tree = future.result()
                    if success and xml_tree is not None:
                        shandong_count = self.process_channels(xml_tree, source)
                        self.process_programs(xml_tree)
                        successful_sources += 1
                        logging.info(f"处理完成: {source} | 山东频道: {shandong_count}个")
                        
                except Exception as e:
                    logging.error(f"处理源数据失败 {source}: {str(e)}")
        
        # 处理完所有源后，增强高清频道节目单
        if successful_sources > 0:
            self.enhance_hd_programs()
        
        return successful_sources > 0

    def generate_final_xml(self) -> str:
        """生成最终的EPG XML文件"""
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="enhanced-epg-generator" 
    generator-info-url="https://github.com/fxq12345/epg" 
    last-update="{datetime.now().strftime('%Y%m%d%H%M%S')}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        # 按优先级添加频道
        insert_position = 0
        for category, _ in CHANNEL_PRIORITY:
            for channel in self.priority_channels[category]:
                root.insert(insert_position, channel)
                insert_position += 1
                
        # 添加所有其他频道
        for channel in self.other_channels:
            root.insert(insert_position, channel)
            insert_position += 1
            
        # 添加所有节目单
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_epg_files(self, xml_content: str):
        """保存EPG文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件
        clean_extensions = ('.xml', '.gz')
        for f in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, f)
            if f.endswith(clean_extensions) and os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
        
        # 保存XML文件
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xml_size = os.path.getsize(xml_path) / 1024 / 1024  # MB
        
        # 保存GZIP压缩文件
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        gz_size = os.path.getsize(gz_path) / 1024  # KB
        
        logging.info(f"EPG文件生成完成: XML={xml_size:.2f}MB, GZIP={gz_size:.1f}KB")

    def print_statistics(self):
        """打印详细统计报告"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        # 统计高清/4K频道情况
        hd_channels = [chan for chan in self.channel_ids 
                      if any(x in chan for x in ['高清', 'HD', '4K', 'UHD'])]
        hd_with_programs = [chan for chan in hd_channels 
                           if chan in self.channel_programs_map]
        
        logging.info("\n" + "="*60)
        logging.info("📊 EPG生成统计报告")
        logging.info("="*60)
        
        for category, _ in CHANNEL_PRIORITY:
            count = len(self.priority_channels[category])
            logging.info(f"  {category}: {count}个频道")
            
        other_count = len(self.other_channels)
        logging.info(f"  其他频道: {other_count}个")
        logging.info(f"  总频道数: {total_channels}个")
        logging.info(f"  总节目数: {total_programs}个")
        logging.info(f"  高清/4K频道: {len(hd_channels)}个")
        logging.info(f"  有节目单的高清频道: {len(hd_with_programs)}个")
        
        # 显示缺少节目单的高清频道
        missing_hd = [chan for chan in hd_channels 
                     if chan not in self.channel_programs_map]
        if missing_hd:
            logging.info(f"  缺少节目单的高清频道: {len(missing_hd)}个")
            for chan in missing_hd[:5]:  # 只显示前5个
                logging.info(f"    - {chan}")
            if len(missing_hd) > 5:
                logging.info(f"    ... 还有{len(missing_hd)-5}个")
        
        logging.info("="*60)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("=== EPG生成开始 ===")
        
        try:
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个EPG源")
            
            if not self.fetch_all_sources(sources):
                logging.error("所有EPG源获取失败")
                return False
                
            xml_content = self.generate_final_xml()
            self.save_epg_files(xml_content)
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== EPG生成完成! 总耗时: {total_time:.2f}秒 ===")
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}")
            return False

def main():
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
