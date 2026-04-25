import os
import gzip
import re
import logging
import io
from datetime import datetime, timedelta
from typing import List, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
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
CORE_RETRY_COUNT = 3

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国", "泰国",
    "越南", "印尼", "马来西亚", "新加坡", "澳洲", "欧洲", "美洲",
    "非洲", "俄罗斯", "印度", "巴西"
]

# 山东台完整别名映射（和你json里的名字完全对应）
SD_ALIAS = {
    "山东齐鲁": ["山东齐鲁", "齐鲁频道", "山东齐鲁hd", "山东齐鲁高清"],
    "山东体育休闲": ["山东体育", "山东体育hd", "山东体育高清", "山东体育休闲频道"],
    "山东农科": ["山东农科", "山东农科hd", "山东农科高清", "农科频道"],
    "山东文旅": ["山东文旅", "文旅频道", "山东影视", "山东文旅频道"],
    "山东生活": ["山东生活", "生活频道", "山东生活频道"],
    "山东综艺": ["山东综艺", "综艺频道", "山东综艺频道"],
    "山东卫视": ["山东卫视", "山东卫视hd", "山东卫视高清"]
}
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.orig_id_to_final_id: Dict[str, str] = {}
        self.alias_map: Dict[str, str] = {}
        for main, aliases in SD_ALIAS.items():
            for a in aliases:
                self.alias_map[a] = main

        now = datetime.now()
        self.today_start = datetime(now.year, now.month, now.day, 0, 0, 0)
        self.start_cutoff = self.today_start - timedelta(days=DAYS_BEFORE)
        self.end_cutoff = self.today_start + timedelta(days=DAYS_AFTER)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml,text/xml,*/*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def clean_xml_content(self, content: str) -> str:
        content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content = content.replace('& ', '&amp; ')
        return content

    def get_content(self, source: str) -> str | None:
        try:
            resp = self.session.get(source, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.content
            if data.startswith(b'\x1f\x8b'):
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                    content = f.read().decode('utf-8', errors='ignore')
            else:
                content = data.decode('utf-8', errors='ignore')
            return self.clean_xml_content(content)
        except Exception as e:
            logging.warning(f"获取失败: {source} | {str(e)[:60]}")
            return None

    def read_epg_sources(self) -> List[str]:
        if not os.path.exists(CONFIG_FILE):
            logging.warning("配置文件不存在")
            return []
        sources = []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith(("http://", "https://")):
                        sources.append(line)
            logging.info(f"读取源：{len(sources)} 个")
            return sources
        except Exception as e:
            logging.error(f"读取配置失败：{e}")
            return []

    def process_channels(self, xml_tree):
        channels = xml_tree.xpath("//channel")
        for ch in channels:
            orig_id = ch.get("id", "").strip()
            names = ch.xpath(".//display-name/text()")
            if not names:
                continue
            display_name = names[0].strip()

            if any(kw in display_name for kw in FOREIGN_KEYWORDS):
                continue

            # 别名替换，保证和你json里的epgid对应
            final_name = self.alias_map.get(display_name, display_name)

            if not final_name or final_name in self.channel_ids:
                continue

            self.orig_id_to_final_id[orig_id] = final_name
            ch.set("id", final_name)
            self.all_channels.append(ch)
            self.channel_ids.add(final_name)

    def parse_program_time(self, ts):
        try:
            part = ts.split()[0]
            return datetime.strptime(part[:14], "%Y%m%d%H%M%S")
        except:
            return None

    def adjust_program_time(self, program, hours=0):
        for attr in ["start", "stop"]:
            ts = program.get(attr, "")
            if not ts or " " not in ts:
                continue
            part, tz = ts.split(" ", 1)
            if len(part) < 14:
                continue
            try:
                dt = datetime.strptime(part[:14], "%Y%m%d%H%M%S")
                dt += timedelta(hours=hours)
                program.set(attr, dt.strftime("%Y%m%d%H%M%S") + " " + tz)
            except Exception:
                pass

    def process_programs(self, xml_tree):
        programs = xml_tree.xpath("//programme")
        keep = []
        for p in programs:
            orig_cid = p.get("channel", "").strip()
            final_id = self.orig_id_to_final_id.get(orig_cid)
            if not final_id:
                continue

            p.set("channel", final_id)
            if "iHOT" in final_id or "ihot" in final_id.lower():
                self.adjust_program_time(p, hours=+8)

            st = self.parse_program_time(p.get("start", ""))
            if not st:
                continue

            if self.start_cutoff <= st <= self.end_cutoff:
                keep.append(p)

        self.all_programs.extend(keep)
        logging.info(f"本轮保留节目：{len(keep)} 条")

    def build_xml_tree(self):
        root = etree.Element("tv")
        for c in self.all_channels:
            root.append(c)
        for p in self.all_programs:
            root.append(p)
        return root

    def save_epg(self):
        if not self.all_channels or not self.all_programs:
            logging.warning("无有效频道或节目")
            return

        tree = self.build_xml_tree()
        xml_str = etree.tostring(tree, encoding="utf-8", xml_declaration=True, pretty_print=True)

        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.GzipFile(filename="epg.xml", mode='wb', fileobj=open(gz_path, 'wb')) as f:
            f.write(xml_str)

        logging.info(f"✅ 生成完成：{gz_path}")
        logging.info(f"📺 频道：{len(self.all_channels)} | 🎬 节目：{len(self.all_programs)}")

    def run(self):
        sources = self.read_epg_sources()
        if not sources:
            return

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.get_content, src) for src in sources]
            for future in as_completed(futures):
                content = future.result()
                if content:
                    try:
                        tree = etree.fromstring(content.encode("utf-8"))
                        self.process_channels(tree)
                        self.process_programs(tree)
                    except Exception as e:
                        logging.warning(f"解析XML失败: {e}")

        self.save_epg()

if __name__ == "__main__":
    gen = EPGGenerator()
    gen.run()
