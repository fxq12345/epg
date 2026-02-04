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

# 高清频道到标清频道的映射（如果高清频道没有节目单，使用标清频道节目单）
HD_TO_SD_MAPPING = {
    "CCTV1高清": "CCTV1",
    "CCTV2高清": "CCTV2", 
    "CCTV3高清": "CCTV3",
    "CCTV4高清": "CCTV4",
    "CCTV5高清": "CCTV5",
    "CCTV6高清": "CCTV6",
    "CCTV7高清": "CCTV7",
    "CCTV8高清": "CCTV8",
    "CCTV9高清": "CCTV9",
    "CCTV10高清": "CCTV10",
    "CCTV11高清": "CCTV11",
    "CCTV12高清": "CCTV12",
    "CCTV13高清": "CCTV13",
    "CCTV14高清": "CCTV14",
    "CCTV15高清": "CCTV15",
    "CCTV16高清": "CCTV16",
    "CCTV17高清": "CCTV17",
    "CCTV4K": "CCTV4",
    "北京卫视4K": "北京卫视",
    "湖南卫视4K": "湖南卫视",
    "浙江卫视4K": "浙江卫视",
    "江苏卫视4K": "江苏卫视",
    "东方卫视4K": "东方卫视",
    "广东卫视4K": "广东卫视",
    "深圳卫视4K": "深圳卫视",
    "山东卫视4K": "山东卫视"
}

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.channel_programs: Dict[str, List] = {}  # 频道ID -> 节目列表
        
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
                    if line and not line.startswith("#") and line.startswith(("http://", "https://")):
                        sources.append(line)
                
                logging.info(f"读取到{len(sources)}个EPG源")
                for i, source in enumerate(sources, 1):
                    logging.info(f"  {i}. {source}")
                
                return sources
                
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容"""
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, any]:
        """获取单个EPG源数据"""
        try:
            start_time = time.time()
            logging.info(f"抓取: {source[:60]}...")
            
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功: {cost_time:.2f}s")
            return True, xml_tree
            
        except Exception as e:
            logging.error(f"失败: {str(e)[:80]}")
            return False, None

    def process_channels_and_programs(self, xml_tree):
        """处理频道和节目数据"""
        # 处理频道
        channels = xml_tree.xpath("//channel")
        for channel in channels:
            channel_id = channel.get("id", "").strip()
            if not channel_id or channel_id in self.channel_ids:
                continue
                
            self.channel_ids.add(channel_id)
            self.all_channels.append(channel)
            
            # 初始化该频道的节目列表
            if channel_id not in self.channel_programs:
                self.channel_programs[channel_id] = []
        
        # 处理节目
        programs = xml_tree.xpath("//programme")
        for program in programs:
            channel_id = program.get("channel", "").strip()
            if channel_id and channel_id in self.channel_programs:
                self.channel_programs[channel_id].append(program)
                self.all_programs.append(program)

    def enhance_hd_channels(self):
        """增强高清频道：如果高清频道没有节目单，使用标清频道的节目单"""
        logging.info("🔧 增强高清频道节目单...")
        enhanced_count = 0
        
        for hd_channel, sd_channel in HD_TO_SD_MAPPING.items():
            # 如果高清频道存在但没有节目单，且标清频道有节目单
            if (hd_channel in self.channel_ids and 
                hd_channel not in self.channel_programs and 
                sd_channel in self.channel_programs and 
                self.channel_programs[sd_channel]):
                
                logging.info(f"  ✅ {hd_channel} ← {sd_channel}")
                sd_programs = self.channel_programs[sd_channel]
                
                # 复制标清频道的节目单到高清频道
                for program in sd_programs:
                    # 深拷贝节目元素
                    program_str = etree.tostring(program, encoding='unicode')
                    new_program = etree.fromstring(program_str)
                    new_program.set("channel", hd_channel)
                    self.all_programs.append(new_program)
                    
                self.channel_programs[hd_channel] = [etree.fromstring(etree.tostring(p, encoding='unicode')) 
                                                   for p in sd_programs]
                for p in self.channel_programs[hd_channel]:
                    p.set("channel", hd_channel)
                    
                enhanced_count += 1
        
        logging.info(f"✅ 增强了{enhanced_count}个高清频道的节目单")

    def fetch_and_process_all_sources(self, sources: List[str]) -> bool:
        """获取并处理所有EPG源"""
        successful_sources = 0
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {executor.submit(self.fetch_single_source, source): source 
                              for source in sources}
            
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, xml_tree = future.result()
                    if success and xml_tree is not None:
                        self.process_channels_and_programs(xml_tree)
                        successful_sources += 1
                except Exception as e:
                    logging.error(f"处理失败 {source}: {str(e)[:80]}")
        
        if successful_sources > 0:
            self.enhance_hd_channels()
            return True
        
        return False

    def generate_final_xml(self) -> str:
        """生成最终的EPG XML文件"""
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="EPG合并器" 
    last-update="{datetime.now().strftime('%Y%m%d%H%M%S')}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        # 添加所有频道
        for channel in self.all_channels:
            root.append(channel)
            
        # 添加所有节目单
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_files(self, xml_content: str):
        """保存EPG文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz')) and os.path.isfile(os.path.join(OUTPUT_DIR, f)):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
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
        
        logging.info(f"💾 文件保存成功:")
        logging.info(f"  📄 epg.xml: {xml_size:.2f} MB")
        logging.info(f"  📦 epg.gz: {gz_size:.1f} KB")

    def print_statistics(self):
        """打印统计信息"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        # 统计高清频道
        hd_channels = [c for c in self.channel_ids if any(x in c for x in ['高清', '4K', 'HD'])]
        hd_with_programs = len([c for c in hd_channels if c in self.channel_programs and self.channel_programs[c]])
        
        logging.info("\n" + "="*50)
        logging.info("📊 EPG统计报告")
        logging.info("="*50)
        logging.info(f"总频道数: {total_channels}")
        logging.info(f"总节目数: {total_programs}")
        logging.info(f"高清/4K频道: {len(hd_channels)}个")
        logging.info(f"有节目单的高清频道: {hd_with_programs}个")
        
        # 显示没有节目单的频道
        channels_without_programs = [c for c in self.channel_ids 
                                   if c not in self.channel_programs or not self.channel_programs[c]]
        if channels_without_programs:
            logging.info(f"无节目单的频道: {len(channels_without_programs)}个")
            for channel in channels_without_programs[:10]:  # 只显示前10个
                logging.info(f"  - {channel}")
        
        logging.info("="*50)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("🚀 开始EPG合并")
        
        try:
            # 读取EPG源
            sources = self.read_epg_sources()
            if not sources:
                logging.error("❌ 没有找到可用的EPG源")
                return False
            
            # 获取并处理所有源
            if not self.fetch_and_process_all_sources(sources):
                logging.error("❌ EPG源获取失败")
                return False
                
            # 生成最终XML
            xml_content = self.generate_final_xml()
            
            # 保存文件
            self.save_files(xml_content)
            
            # 打印统计
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"✅ 完成! 耗时: {total_time:.2f}秒")
            return True
            
        except Exception as e:
            logging.error(f"💥 失败: {str(e)}")
            return False

def main():
    """主函数"""
    print("\n" + "="*50)
    print("📺 EPG合并工具")
    print("="*50)
    
    generator = EPGGenerator()
    success = generator.run()
    
    if success:
        print("\n✅ EPG文件生成成功!")
        print(f"📁 输出目录: {os.path.abspath(OUTPUT_DIR)}")
    else:
        print("\n❌ EPG文件生成失败!")
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
