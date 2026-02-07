import os
import gzip
import re
import time
import signal
import logging
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom

# 10分钟强制终止
def timeout_exit(signum, frame):
    os._exit(0)
signal.signal(signal.SIGALRM, timeout_exit)
signal.alarm(600)

# ===================== 你原版配置 完全不动 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 5
TIMEOUT = 30
CORE_RETRY_COUNT = 2

LOCAL_WEIFANG_EPG = os.path.join(OUTPUT_DIR, "weifang.xml")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/xml, */*",
    "Accept-Encoding": "gzip, deflate"
}

# 潍坊频道（与电视完全一致）
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ===================== 潍坊抓取模块 =====================
def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_page_html(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.encoding = 'utf-8'
        return resp.text if "节目" in resp.text else ""
    except:
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
                if len(title) > 1 and "广告" not in title:
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
    rough = ET.tostring(root, encoding='utf-8')
    return minidom.parseString(rough).toprettyxml(indent="  ", encoding="utf-8")

def run_weifang_crawler():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        channel_data = {}
        for ch_name, base_url in WEIFANG_CHANNELS:
            week_data = []
            for wname, wsuffix in WEEK_MAP.items():
                progs = get_day_program(ch_name, base_url, wname, wsuffix)
                week_data.append((wname, wsuffix, progs))
                time.sleep(0.5)
            channel_data[ch_name] = week_data
        xml_bytes = build_weifang_xml(channel_data)
        with open(LOCAL_WEIFANG_EPG, "wb") as f:
            f.write(xml_bytes)
    except:
        with open(LOCAL_WEIFANG_EPG, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>')

# ===================== 你原版合并模块 完全不动 =====================
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
        retry_strategy = Retry(total=CORE_RETRY_COUNT + 2, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)
        return session

    def read_epg_sources(self) -> List[str]:
        if not os.path.exists(CONFIG_FILE):
            return []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip() and not line.startswith("#") and line.startswith(("http://", "https://"))]
        except:
            return []

    def clean_xml_content(self, content: str) -> str:
        content = re.sub(r'[\x00-\x1F\x7F]', '', content)
        return content.replace('& ', '&amp; ')

    def fetch_single_source(self, source: str):
        try:
            r = self.session.get(source, timeout=TIMEOUT)
            r.raise_for_status()
            c = gzip.decompress(r.content).decode('utf-8') if source.endswith('.gz') else r.text
            return etree.fromstring(self.clean_xml_content(c).encode('utf-8'))
        except:
            return None

    def process_channels_and_programs(self, xml_tree):
        try:
            for ch in xml_tree.xpath("//channel"):
                cid = ch.get("id", "").strip()
                if cid and cid not in self.channel_ids:
                    self.channel_ids.add(cid)
                    self.all_channels.append(ch)
                    self.channel_programs[cid] = []
            for p in xml_tree.xpath("//programme"):
                cid = p.get("channel", "").strip()
                if cid in self.channel_programs:
                    self.all_programs.append(p)
        except:
            pass

    def process_local_weifang_epg(self):
        if not os.path.exists(LOCAL_WEIFANG_EPG):
            return
        try:
            with open(LOCAL_WEIFANG_EPG, "r", encoding="utf-8") as f:
                tree = etree.fromstring(self.clean_xml_content(f.read()).encode("utf-8"))
                self.process_channels_and_programs(tree)
        except:
            pass

    def run(self):
        sources = self.read_epg_sources()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {executor.submit(self.fetch_single_source, s): s for s in sources}
            for fut in as_completed(future_map):
                tree = fut.result()
                if tree:
                    self.process_channels_and_programs(tree)
        self.process_local_weifang_epg()
        root = etree.fromstring(b'<?xml version="1.0" encoding="UTF-8"?><tv/>')
        for ch in self.all_channels:
            root.append(ch)
        for p in self.all_programs:
            root.append(p)
        final = etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")
        with open(os.path.join(OUTPUT_DIR, "epg.xml"), "w", encoding="utf-8") as f:
            f.write(final)
        with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
            f.write(final.encode("utf-8"))

# ===================== 主入口 =====================
def main():
    try:
        run_weifang_crawler()
    except:
        pass
    try:
        EPGGenerator().run()
    except:
        pass

if __name__ == "__main__":
    main()
