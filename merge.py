import os
import gzip
import re
import time
import logging
import io
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
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
# 时间范围：今天前后各7天，共14天
DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国", "泰国",
    "越南", "印尼", "马来西亚", "新加坡", "澳洲", "欧洲", "美洲",
    "非洲", "俄罗斯", "印度", "巴西"
]
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.orig_id_to_lower_id: Dict[str, str] = {}

        # 时间范围：今天0点 - 7天前 到 今天0点 + 7天后
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
            logging.warning(f"获取内容失败: {source} | {str(e)[:60]}")
            return None

    def read_epg_sources(self) -> List[str]:
        if not os.path.exists(CONFIG_FILE):
            logging.warning("配置文件不存在")
            return []
        sources = []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith(("http://", "https://")):
                        sources.append(line)
            logging.info(f"读取配置完成，共找到 {len(sources)} 个源")
            return sources
        except Exception as e:
            logging.error(f"读取配置失败：{str(e)}")
            return []

    def clean_xml_content(self, content: str) -> str:
        content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content = content.replace('& ', '&amp; ')
        return content

    def normalize_channel_name(self, name: str) -> str:
        # 大小写不敏感 + 去符号，适配酷9匹配
        name = re.sub(r'[^\u4e00-\u9fff0-9a-zA-Z]', '', name)
        name = name.lower()
        name = re.sub(r'^iptv', '', name)
        return name.strip()

    def fetch_single_source(self, source: str) -> Tuple[bool, str, int, int, int]:
        retry_count = 0
        start_time = time.time()
        while retry_count < CORE_RETRY_COUNT:
            try:
                logging.info(f"🔄 第{retry_count+1}次尝试抓取：{source}")
                content = self.get_content(source)
                if not content:
                    raise Exception("空内容")
                tree = etree.fromstring(content.encode("utf-8"))
                chan_cnt = len(tree.xpath("//channel"))
                prog_cnt = len(tree.xpath("//programme"))
                cost = time.time() - start_time
                logging.info(f"✅ 成功抓取 {source} | 耗时 {cost:.2f}s | 频道 {chan_cnt} | 节目 {prog_cnt}")
                return True, source, retry_count, chan_cnt, prog_cnt
            except Exception as e:
                retry_count += 1
                logging.warning(f"⚠️ 失败[{retry_count}/{CORE_RETRY_COUNT}] {source} | {str(e)[:60]}")
                time.sleep(0.8)
        cost = time.time() - start_time
        logging.error(f"💥 全部重试失败 {source} | 耗时 {cost:.2f}s")
        return False, source, CORE_RETRY_COUNT, 0, 0

    def process_channels(self, xml_tree):
        channels = xml_tree.xpath("//channel")
        for ch in channels:
            orig_id = ch.get("id", "").strip()
            names = ch.xpath(".//display-name/text()")
            if not names:
                continue
            name = names[0].strip()
            lower_id = self.normalize_channel_name(name)

            if any(kw in name for kw in FOREIGN_KEYWORDS):
                continue
            if not lower_id or lower_id in self.channel_ids:
                continue

            self.orig_id_to_lower_id[orig_id] = lower_id
            ch.set("id", lower_id)
            self.all_channels.append(ch)
            self.channel_ids.add(lower_id)

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
            lower_id = self.orig_id_to_lower_id.get(orig_cid)
            if not lower_id or lower_id not in self.channel_ids:
                continue

            # 替换节目ID为标准化名称，适配酷9
            p.set("channel", lower_id)
            
            # iHOT频道时间+8小时
            if "ihot" in lower_id:
                self.adjust_program_time(p, hours=+8)

            # 解析时间并过滤范围（今天前后各7天）
            st = self.parse_program_time(p.get("start", ""))
            if not st:
                continue
            if st < self.start_cutoff or st > self.end_cutoff:
                continue

            keep.append(p)

        self.all_programs.extend(keep)
        logging.info(f"✅ 保留今天前后各7天，共14天有效节目：{len(keep)} 条")

    def build_xml_tree(self):
        root = etree.Element("tv")
        for c in self.all_channels:
            root.append(c)
        for p in self.all_programs:
            root.append(p)
        return root

    def save_epg(self):
        if not self.all_channels or not self.all_programs:
            logging.warning("⚠️ 无有效频道或节目")
            return

        tree = self.build_xml_tree()
        xml_str = etree.tostring(tree, encoding="utf-8", xml_declaration=True, pretty_print=True)
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_str)
        logging.info(f"✅ 已生成：{gz_path}")
        logging.info(f"📺 频道：{len(self.all_channels)} | 🎬 节目：{len(self.all_programs)}")

    def run(self):
        sources = self.read_epg_sources()
        if not sources:
            return

        for src in sources:
            try:
                content = self.get_content(src)
                if content:
                    tree = etree.fromstring(content.encode("utf-8"))
                    self.process_channels(tree)
                    self.process_programs(tree)
            except Exception as e:
                logging.warning(f"处理失败 {src}: {e}")

        self.save_epg()

if __name__ == "__main__":
    gen = EPGGenerator()
    gen.run()
