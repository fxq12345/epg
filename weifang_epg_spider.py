import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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
# 本地潍坊EPG文件路径
LOCAL_WEIFANG_EPG = os.path.join(OUTPUT_DIR, "weifang.xml")

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.channel_programs: Dict[str, List] = {}
        
    def _create_session(self) -> requests.Session:
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
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            return []
            
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                sources = [
                    line.strip() for line_num, line in enumerate(f, 1)
                    if line.strip() and not line.startswith("#") and line.startswith(("http://", "https://"))
                ]
                logging.info(f"读取到{len(sources)}个EPG源")
                return sources
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            return []

    def clean_xml_content(self, content: str) -> str:
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        return content_clean.replace('& ', '&amp; ')

    def fetch_single_source(self, source: str) -> Tuple[bool, any]:
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
            logging.info(f"成功: {time.time() - start_time:.2f}s")
            return True, xml_tree
        except Exception as e:
            logging.error(f"失败: {str(e)[:80]}")
            return False, None

    def process_channels_and_programs(self, xml_tree):
        # 处理频道
        for channel in xml_tree.xpath("//channel"):
            channel_id = channel.get("id", "").strip()
            if not channel_id or channel_id in self.channel_ids:
                continue
            self.channel_ids.add(channel_id)
            self.all_channels.append(channel)
            self.channel_programs[channel_id] = []
        
        # 处理节目
        for program in xml_tree.xpath("//programme"):
            channel_id = program.get("channel", "").strip()
            if channel_id and channel_id in self.channel_programs:
                self.channel_programs[channel_id].append(program)
                self.all_programs.append(program)

    # 处理本地潍坊EPG（失败不中断）
    def process_local_weifang_epg(self):
        if not os.path.exists(LOCAL_WEIFANG_EPG):
            logging.warning(f"本地潍坊EPG文件不存在: {LOCAL_WEIFANG_EPG}，跳过")
            return
        
        try:
            logging.info(f"开始合并本地潍坊EPG文件")
            with open(LOCAL_WEIFANG_EPG, "r", encoding="utf-8") as f:
                content = f.read()
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            self.process_channels_and_programs(xml_tree)
            logging.info(f"✅ 成功合并本地潍坊EPG")
        except Exception as e:
            logging.error(f"合并本地潍坊EPG失败: {str(e)}")
            # 仅打印日志，不中断流程

    def fetch_and_process_all_sources(self, sources: List[str]):
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {executor.submit(self.fetch_single_source, source): source for source in sources}
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, xml_tree = future.result()
                    if success and xml_tree is not None:
                        self.process_channels_and_programs(xml_tree)
                        logging.info(f"✅ 成功处理源: {source[:60]}")
                except Exception as e:
                    logging.error(f"处理源{source}失败: {str(e)}")
        
        # 处理本地潍坊EPG（无论外部源是否成功，都执行）
        self.process_local_weifang_epg()

    def generate_final_xml(self) -> str:
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="EPG合并器" last-update="{datetime.now().strftime('%Y%m%d%H%M%S')}">'''
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        for channel in self.all_channels:
            root.append(channel)
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_files(self, xml_content: str):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # 清理旧文件
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz')) and os.path.isfile(os.path.join(OUTPUT_DIR, f)):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except Exception:
                    pass
        
        # 保存XML和GZIP
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
            f.write(xml_content.encode("utf-8"))
        
        logging.info(f"💾 文件保存成功: epg.xml / epg.gz")

    def print_statistics(self):
        logging.info("\n" + "="*50)
        logging.info("📊 EPG统计报告")
        logging.info(f"总频道数: {len(self.channel_ids)}")
        logging.info(f"总节目数: {len(self.all_programs)}")
        logging.info("="*50)

    def run(self):
        start_time = time.time()
        logging.info("🚀 开始EPG合并")
        
        try:
            sources = self.read_epg_sources()
            self.fetch_and_process_all_sources(sources)
            
            # 即使无数据也生成文件
            xml_content = self.generate_final_xml()
            self.save_files(xml_content)
            self.print_statistics()
            
            logging.info(f"✅ 完成! 耗时: {time.time() - start_time:.2f}秒")
            return True
        except Exception as e:
            logging.error(f"💥 合并失败: {str(e)}")
            return False

def main():
    print("\n" + "="*50)
    print("📺 EPG合并工具")
    print("="*50)
    
    generator = EPGGenerator()
    success = generator.run()
    
    print("\n✅ EPG文件生成完成!" if success else "\n❌ EPG合并流程已执行（部分环节失败）")
    print(f"📁 输出目录: {os.path.abspath(OUTPUT_DIR)}")
    exit(0)

if __name__ == "__main__":
    main()
