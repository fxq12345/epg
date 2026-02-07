import os
import gzip
import re
import time
import signal
import logging
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 原版配置完全不动 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 5
TIMEOUT = 30
CORE_RETRY_COUNT = 2

LOCAL_WEIFANG_EPG = os.path.join(OUTPUT_DIR, "weifang.xml")

# 10分钟超时强制退出
GLOBAL_TIMEOUT_SECONDS = 600
def timeout_handler(signum, frame):
    os._exit(0)
signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(GLOBAL_TIMEOUT_SECONDS)

# 日志完全保留你原版
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ===================== 你原版代码全部不动 =====================
class EPGGenerator:
    def __init__(self):
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
            logging.info(f"从{CONFIG_FILE}读取到{len(sources)}条EPG源:")
            for idx, source in enumerate(sources, 1):
                logging.info(f"  {idx}. {source[:60]}...")
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
            logging.warning(f"⚠️ 抓取失败(跳过): {source[:30]}... -> {str(e)[:50]}")
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
            logging.warning(f"⚠️ 合并本地潍坊EPG失败，已跳过: {str(e)}")

    def fetch_and_process_all_sources(self, sources: List[str]):
        logging.info("\n开始抓取所有EPG源:")
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {executor.submit(self.fetch_single_source, source): source for source in sources}
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, xml_tree = future.result()
                    if success and xml_tree is not None:
                        self.process_channels_and_programs(xml_tree, source)
                except Exception as e:
                    logging.warning(f"处理源{source[:30]}...失败: {str(e)}")
        self.process_local_weifang_epg()

    def generate_final_xml(self) -> str:
        xml_declare = '<?xml version="1.0" encoding="UTF-8"?>'
        root = etree.fromstring(f"{xml_declare}<tv></tv>".encode("utf-8"))
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
        logging.info(f"\n💾 文件保存成功:")
        logging.info(f"  - XML文件: {os.path.abspath(xml_path)}")
        logging.info(f"  - GZIP文件: {os.path.abspath(gz_path)}")

    def print_statistics(self):
        logging.info("\n" + "="*50)
        logging.info("📊 EPG合并统计报告")
        logging.info(f"  总频道数: {len(self.channel_ids)}")
        logging.info(f"  总节目数: {len(self.all_programs)}")
        logging.info("="*50)

    def run(self):
        start_time = time.time()
        logging.info("\n" + "="*50)
        logging.info("🚀 启动EPG合并流程")
        logging.info("="*50)
        try:
            sources = self.read_epg_sources()
            if not sources:
                logging.warning("❌ 无可用EPG源，继续本地源")
            self.fetch_and_process_all_sources(sources)
            xml_content = self.generate_final_xml()
            self.save_files(xml_content)
            self.print_statistics()
            total_time = time.time() - start_time
            logging.info(f"\n✅ 合并流程完成! 总耗时: {total_time:.2f}秒")
            return True
        except Exception as e:
            logging.warning(f"\n⚠️ 合并流程异常，已跳过: {str(e)}")
            return False

# ===================== 嵌入潍坊抓取（你原版逻辑完全不变） =====================
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import timedelta
import random

WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_MAP = {
    "周一": "w1", "周二": "w2", "周三": "w3", "周四": "w4",
    "周五": "w5", "周六": "w6", "周日": "w7"
}

HEADERS_WF = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com"
}

def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_page_html(url):
    try:
        resp = requests.get(url, headers=HEADERS_WF, timeout=15)
        resp.encoding = 'utf-8'
        if "节目单" in resp.text or len(re.findall(r'\d{1,2}:\d{2}', resp.text)) > 5:
            return resp.text
    except:
        return ""
    return ""

def get_day_program(channel_name, base_url, week_name, w_suffix):
    url = f"{base_url}/{w_suffix}" if not base_url.endswith('/') else f"{base_url}{w_suffix}"
    programs = []
    try:
        html = get_page_html(url)
        if not html:
            return programs
        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I)) or soup.find_all("li")
        for item in items:
            match = re.search(r'(\d{1,2}:\d{2})\s*(.+)', item.get_text(strip=True))
            if match:
                t, title = match.groups()
                if len(title) > 1 and '广告' not in title:
                    programs.append((t.strip(), title.strip()))
        programs = sorted(list(set(programs)), key=lambda x: x[0])
    except:
        pass
    return programs

def build_weifang_xml(channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "Weifang Local EPG")
    for ch_name, _ in WEIFANG_CHANNELS:
        ch = ET.SubElement(root, "channel", id=ch_name)
        ET.SubElement(ch, "display-name", lang="zh").text = ch_name
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    for ch_name, week_list in channel_data.items():
        for i, (wname, wsuffix, progs) in enumerate(week_list):
            current_date = monday + timedelta(days=i)
            for idx in range(len(progs)):
                s_time, title = progs[idx]
                e_time = progs[idx+1][0] if idx < len(progs)-1 else (datetime.strptime(s_time,"%H:%M")+timedelta(minutes=30)).strftime("%H:%M")
                s_xml = time_to_xmltv(current_date, s_time)
                e_xml = time_to_xmltv(current_date, e_time)
                if s_xml and e_xml:
                    prog = ET.SubElement(root, "programme")
                    prog.set("start", s_xml)
                    prog.set("stop", e_xml)
                    prog.set("channel", ch_name)
                    ET.SubElement(prog, "title", lang="zh").text = title
    rough_str = ET.tostring(root, encoding="utf-8")
    return minidom.parseString(rough_str).toprettyxml(indent="  ", encoding="utf-8")

def run_weifang_crawler():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "weifang.xml")
    try:
        channel_data = {}
        for ch_name, base_url in WEIFANG_CHANNELS:
            week_data = []
            for wname, wsuffix in WEEK_MAP.items():
                progs = get_day_program(ch_name, base_url, wname, wsuffix)
                week_data.append((wname, wsuffix, progs))
                time.sleep(0.7)
            channel_data[ch_name] = week_data
        xml_bytes = build_weifang_xml(channel_data)
        with open(out_path, "wb") as f:
            f.write(xml_bytes)
    except Exception as e:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>')

# ===================== 主入口（原版结构不变） =====================
def main():
    try:
        run_weifang_crawler()
    except:
        pass
    EPGGenerator().run()

if __name__ == "__main__":
    main()
