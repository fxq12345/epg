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

===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 5  # 同时抓取5条源
TIMEOUT = 30
CORE_RETRY_COUNT = 2
本地潍坊EPG文件路径（可选）
LOCAL_WEIFANG_EPG = os.path.join(OUTPUT_DIR, "weifang.xml")

配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
==================================================

class EPGGenerator:
    def init(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.channel_programs: Dict[str, List] = {}
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
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
            "Accept": "application/xml, /",
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
            logging.info(f"从{CONFIG_FILE}读取到{len(sources)}条EPG源:")
            for idx, source in enumerate(sources, 1):
                logging.info(f"  {idx}. {source[:60]}...")
            return sources
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            return []

    def clean_xml_content(self, content: str) -> str:
        content_clean = re.sub(r'[x00-x08x0Bx0Cx0E-x1Fx7F]', '', content)
        return content_clean.replace('& ', '&amp; ')

    def fetch_single_source(self, source: str) -> Tuple[bool, any]:
        try:
            start_time = time.time()
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            cost_time = time.time() - start_time
            logging.info(f"✅ 抓取成功: {source[:30]}... (耗时{cost_time:.2f}s)")
            return True, xml_tree
        except Exception as e:
            logging.error(f"❌ 抓取失败: {source[:30]}... -> {str(e)[:50]}")
            return False, None

    def process_channels_and_programs(self, xml_tree, source: str):
        channel_count = 0
        for channel in xml_tree.xpath("//channel"):
            channel_id = channel.get("id", "").strip()
            if not channel_id or channel_id in self.channel_ids:
                continue
            self.channel_ids.add(channel_id)
            self.all_channels.append(channel)
            self.channel_programs[channel_id] = []
            channel_count += 1
        
        program_count = 0
        for program in xml_tree.xpath("//programme"):
            channel_id = program.get("channel", "").strip()
            if channel_id and channel_id in self.channel_programs:
                self.channel_programs[channel_id].append(program)
                self.all_programs.append(program)
                program_count += 1
        
        logging.info(f"🔧 处理{source[:30]}...: 新增频道{channel_count}个，新增节目{program_count}个")

    def process_local_weifang_epg(self):
        if not os.path.exists(LOCAL_WEIFANG_EPG):
            logging.warning(f"⚠️ 本地潍坊EPG文件不存在: {LOCAL_WEIFANG_EPG}，跳过")
            return
        
        try:
            logging.info(f"开始合并本地潍坊EPG文件")
            with open(LOCAL_WEIFANG_EPG, "r", encoding="utf-8") as f:
                content = f.read()
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            self.process_channels_and_programs(xml_tree, "本地潍坊EPG")
            logging.info(f"✅ 成功合并本地潍坊EPG")
        except Exception as e:
            logging.error(f"❌ 合并本地潍坊EPG失败: {str(e)}")

    def fetch_and_process_all_sources(self, sources: List[str]):
        logging.info("n开始抓取所有EPG源:")
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {executor.submit(self.fetch_single_source, source): source for source in sources}
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, xml_tree = future.result()
                    if success and xml_tree is not None:
                        self.process_channels_and_programs(xml_tree, source)
                except Exception as e:
                    logging.error(f"处理源{source[:30]}...失败: {str(e)}")
        
        self.process_local_weifang_epg()

    def generate_final_xml(self) -> str:
        xml_declare = f'''
'''
        root = etree.fromstring(f"{xml_declare}".encode("utf-8"))
        
        for channel in self.all_channels:
            root.append(channel)
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_files(self, xml_content: str):
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        
        logging.info(f"n💾 文件保存成功:")
        logging.info(f"  - XML文件: {os.path.abspath(xml_path)}")
        logging.info(f"  - GZIP文件: {os.path.abspath(gz_path)}")

    def print_statistics(self):
        logging.info("n" + "="*50)
        logging.info("📊 EPG合并统计报告")
        logging.info(f"  总频道数: {len(self.channel_ids)}")
        logging.info(f"  总节目数: {len(self.all_programs)}")
        logging.info("="*50)

    def run(self):
        start_time = time.time()
        logging.info("n" + "="*50)
        logging.info("🚀 启动EPG合并流程")
        logging.info("="*50)
        
        try:
            sources = self.read_epg_sources()
            if not sources:
                logging.error("❌ 无可用EPG源，流程终止")
                return False
            
            self.fetch_and_process_all_sources(sources)
            xml_content = self.generate_final_xml()
            self.save_files(xml_content)
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"n✅ 合并流程完成! 总耗时: {total_time:.2f}秒")
            return True
        except Exception as e:
            logging.error(f"n💥 合并流程异常失败: {str(e)}", exc_info=True)
            return False

def main():
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if name == "main":
    main()
