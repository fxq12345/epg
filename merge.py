import os
import gzip
import re
import logging
import io
from datetime import datetime, timedelta
from typing import List, Dict, Set
# 修复点：导入线程池相关模块
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

# 保留前7天 + 后7天，总计14天范围
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

# 国际台过滤（可选）
FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国", "泰国",
    "越南", "印尼", "马来西亚", "新加坡", "澳洲", "欧洲", "美洲",
    "非洲", "俄罗斯", "印度", "巴西"
]

# 山东台完整别名映射
SD_ALIAS = {
    "山东齐鲁": ["齐鲁频道", "山东齐鲁HD", "山东齐鲁高清"],
    "山东体育": ["山东体育HD", "山东体育高清", "山东体育休闲频道"],
    "山东农科": ["农科频道", "山东农科HD", "山东农科高清"],
    "山东文旅": ["文旅频道", "山东影视", "山东文旅频道"],
    "山东生活": ["生活频道", "山东生活频道"],
    "山东综艺": ["综艺频道", "山东综艺频道"],
    "山东卫视": ["山东卫视HD", "山东卫视高清"]
}
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_map: Dict[str, str] = {}
        self.final_channels: List = []
        self.valid_programs: List = []
        self.local_tz = 8  # 本地时区UTC+8

        # 初始化别名映射
        for main_name, aliases in SD_ALIAS.items():
            for alias in aliases:
                self.channel_map[alias.lower()] = main_name

        # 14天时间范围
        self.now = datetime.now()
        self.valid_start = self.now - timedelta(days=DAYS_BEFORE)
        self.valid_end = self.now + timedelta(days=DAYS_AFTER)
        self.valid_start = datetime(self.valid_start.year, self.valid_start.month, self.valid_start.day, 0, 0, 0)
        self.valid_end = datetime(self.valid_end.year, self.valid_end.month, self.valid_end.day, 23, 59, 59)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=CORE_RETRY_COUNT,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WebKit/537.36",
            "Accept": "application/xml,text/xml,*/*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def clean_xml_content(self, content: str) -> str:
        content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        return content.replace('& ', '&amp; ')

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
            logging.info(f"读取EPG源：{len(sources)} 个")
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
            display_name = names[0].strip().lower()

            if any(kw in display_name for kw in FOREIGN_KEYWORDS):
                continue

            final_name = self.channel_map.get(display_name, names[0].strip())
            if final_name in [c.get("id") for c in self.final_channels]:
                continue

            ch.set("id", final_name)
            self.final_channels.append(ch)

    def parse_and_adjust_time(self, time_str: str) -> datetime | None:
        try:
            if " " in time_str:
                time_part, tz = time_str.split(" ", 1)
                dt = datetime.strptime(time_part[:14], "%Y%m%d%H%M%S")
                tz_offset = int(tz[:3]) if tz[:3] in ['+08', '+00', '-08'] else 0
                dt -= timedelta(hours=tz_offset)
                dt += timedelta(hours=self.local_tz)
                return dt
            else:
                return datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
        except Exception as e:
            logging.debug(f"时间解析失败：{time_str} | {str(e)[:30]}")
            return None

    def process_programs(self, xml_tree):
        programs = xml_tree.xpath("//programme")
        for p in programs:
            channel = p.get("channel", "").strip()
            if not channel or channel not in [c.get("id") for c in self.final_channels]:
                continue

            start_str = p.get("start", "")
            stop_str = p.get("stop", "")

            start_dt = self.parse_and_adjust_time(start_str)
            if not start_dt:
                continue
            stop_dt = self.parse_and_adjust_time(stop_str) if stop_str else None

            # 自动矫正异常时间
            if start_dt < self.valid_start - timedelta(days=1):
                logging.info(f"[{channel}] 检测到UTC+0时区，自动矫正为本地时间")
                start_dt += timedelta(hours=8)
                if stop_dt:
                    stop_dt += timedelta(hours=8)
            elif start_dt > self.valid_end + timedelta(days=1):
                logging.info(f"[{channel}] 检测到时区异常，自动矫正为本地时间")
                start_dt -= timedelta(hours=8)
                if stop_dt:
                    stop_dt -= timedelta(hours=8)

            # 保留14天范围内节目
            if self.valid_start <= start_dt <= self.valid_end:
                p.set("start", start_dt.strftime("%Y%m%d%H%M%S"))
                if stop_dt and self.valid_start <= stop_dt <= self.valid_end:
                    p.set("stop", stop_dt.strftime("%Y%m%d%H%M%S"))
                self.valid_programs.append(p)

        logging.info(f"本轮有效节目（14天范围）：{len(self.valid_programs)} 条")

    def save_epg(self):
        # 强制覆盖逻辑：无论是否存在，都删除旧文件再生成
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        if os.path.exists(gz_path):
            try:
                os.remove(gz_path)
                logging.info(f"🗑️  强制删除旧文件：{gz_path}")
            except Exception as e:
                logging.error(f"删除旧文件失败：{e}")
                return

        if not self.final_channels or not self.valid_programs:
            logging.warning("无有效频道或节目，未生成EPG")
            return

        root = etree.Element("tv")
        for ch in self.final_channels:
            root.append(ch)
        for p in self.valid_programs:
            root.append(p)

        xml_str = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        # 强制写入新文件
        with open(gz_path, 'wb') as f:
            with gzip.GzipFile(filename="epg.xml", mode='wb', fileobj=f) as gz:
                gz.write(xml_str)

        logging.info(f"✅ 强制生成新EPG：{gz_path}")
        logging.info(f"📺 有效频道：{len(self.final_channels)} 个 | 🎬 有效节目：{len(self.valid_programs)} 条")
        logging.info(f"📅 时间范围：{self.valid_start.strftime('%Y-%m-%d')} 至 {self.valid_end.strftime('%Y-%m-%d')}（共14天）")

    def run(self):
        sources = self.read_epg_sources()
        if not sources:
            return

        # 多线程并发获取EPG内容
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
                        logging.warning(f"解析XML失败：{str(e)[:50]}")

        self.save_epg()

if __name__ == "__main__":
    gen = EPGGenerator()
    gen.run()
