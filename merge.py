import os
import gzip
import re
import time
import logging
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
CORE_RETRY_COUNT = 3  # 增加重试次数

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志格式增强：详细时间 + 源地址 + 结果统计
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 全频道手动映射表（兜底用）
COOL9_ID_MAPPING = {
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4",
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10", "11": "CCTV11", "12": "CCTV12",
    "13": "CCTV13", "14": "CCTV14", "15": "CCTV15", "16": "CCTV16",
    "21": "北京卫视", "22": "河南卫视", "23": "河北卫视",
    "24": "湖北卫视", "25": "海南卫视", "26": "贵州卫视", "27": "厦门卫视",
    "28": "CCTV5+", "29": "峨眉电影", "30": "峨眉电影4K", "31": "北京IPTV4K超清",
    "32": "淘电影", "33": "淘娱乐", "34": "淘剧场", "35": "淘baby", "36": "淘精彩",
    "37": "萌宠TV", "38": "优漫卡通"
}

# 国外频道过滤
FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国",
    "泰国", "越南", "印尼", "马来西亚", "新加坡", "澳洲",
    "欧洲", "美洲", "非洲", "俄罗斯", "印度", "巴西"
]

DOMESTIC_SPECIAL = ["popc", "淘", "new", "NEW", "POPC", "超级电影", "IPTV", "new系列", "NewTV"]
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.name_to_final_id = dict()
        self.program_channel_map = dict()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def read_epg_sources(self) -> List[str]:
        """读取 config.txt，返回所有有效源（支持gz/xml/其他）"""
        if not os.path.exists(CONFIG_FILE):
            logging.warning(f"配置文件 {CONFIG_FILE} 不存在，使用空列表")
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
                    else:
                        logging.warning(f"第{line_num}行：无效地址，跳过 -> {line}")
            logging.info(f"读取配置完成，共找到 {len(sources)} 个源")
            return sources
        except Exception as e:
            logging.error(f"读取配置失败：{str(e)}")
            return []

    def clean_xml_content(self, content: str) -> str:
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, int, int, int]:
        """
        抓取单个源，并返回详细统计：
        (是否成功, 源地址, 重试次数, 频道数, 节目数)
        """
        retry_count = 0
        start_time = time.time()

        while retry_count < CORE_RETRY_COUNT:
            try:
                logging.info(f"🔄 第{retry_count+1}次尝试抓取：{source}")
                resp = self.session.get(source, timeout=TIMEOUT)
                resp.raise_for_status()
                cost = time.time() - start_time

                # 识别内容类型
                if source.endswith(".gz") or resp.headers.get("Content-Encoding") == "gzip":
                    content = gzip.decompress(resp.content).decode("utf-8", errors="ignore")
                    fmt = "gz"
                elif "xml" in resp.headers.get("Content-Type", "") or source.endswith(".xml"):
                    content = resp.text
                    fmt = "xml"
                else:
                    content = resp.text
                    fmt = "other"

                content_clean = self.clean_xml_content(content)
                xml_tree = etree.fromstring(content_clean.encode("utf-8"))

                # 统计频道 & 节目
                channels = xml_tree.xpath("//channel")
                programs = xml_tree.xpath("//programme")
                chan_cnt = len(channels)
                prog_cnt = len(programs)

                logging.info(
                    f"✅ 成功抓取 [{fmt}] {source} | "
                    f"耗时 {cost:.2f}s | "
                    f"频道 {chan_cnt} | "
                    f"节目 {prog_cnt} | "
                    f"重试 {retry_count} 次"
                )
                return True, source, retry_count, chan_cnt, prog_cnt

            except requests.exceptions.RetryError as e:
                retry_count += 1
                logging.warning(
                    f"⚠️  重试失败[{retry_count}/{CORE_RETRY_COUNT}] {source} | 错误：{str(e)[:80]}"
                )
                time.sleep(0.5)
            except Exception as e:
                retry_count += 1
                logging.warning(
                    f"❌ 抓取失败[{retry_count}/{CORE_RETRY_COUNT}] {source} | 错误：{str(e)[:80]}"
                )
                time.sleep(0.8)

        # 全部重试失败
        cost = time.time() - start_time
        logging.error(
            f"💥 所有重试均失败 {source} | "
            f"耗时 {cost:.2f}s | "
            f"重试 {CORE_RETRY_COUNT} 次"
        )
        return False, source, CORE_RETRY_COUNT, 0, 0

    def normalize_channel_name(self, name: str) -> str:
        name = re.sub(r'[^\u4e00-\u9fff0-9a-zA-Z]', '', name)
        name = name.replace("new", "NEW").replace("newtv", "NEWTV")
        name = re.sub(r'^IPTV', '', name)
        return name.strip()

    def pre_fetch_program_channels(self, sources: List[str]):
        """预抓取频道映射，辅助精准匹配"""
        logging.info("开始预抓取频道映射...")
        map_count = 0
        for source in sources:
            try:
                success, _, _, _, _ = self.fetch_single_source(source)
                if not success:
                    continue
                # 这里只做预解析，不写入all_channels
                resp = self.session.get(source, timeout=TIMEOUT)
                if source.endswith(".gz"):
                    content = gzip.decompress(resp.content).decode("utf-8")
                else:
                    content = resp.text
                tree = etree.fromstring(self.clean_xml_content(content).encode("utf-8"))
                chans = tree.xpath("//channel")
                for ch in chans:
                    cid = ch.get("id", "").strip()
                    names = ch.xpath(".//display-name/text()")
                    if not cid or not names:
                        continue
                    norm = self.normalize_channel_name(names[0])
                    if norm and norm not in self.program_channel_map:
                        self.program_channel_map[norm] = cid
                        map_count += 1
            except Exception as e:
                logging.debug(f"预抓取跳过 {source}：{str(e)[:60]}")
        logging.info(f"预抓取完成，建立 {map_count} 个名称-ID映射")

    def process_channels(self, xml_tree, source: str) -> int:
        channels = xml_tree.xpath("//channel")
        add_count = 0
        for ch in channels:
            orig_id = ch.get("id", "").strip()
            names = ch.xpath(".//display-name/text()")
            if not names:
                continue
            name = names[0].strip()
            norm_name = self.normalize_channel_name(name)

            if any(kw in name for kw in FOREIGN_KEYWORDS):
                continue
            if any(kw in name for kw in DOMESTIC_SPECIAL):
                pass

            final_id = orig_id
            if norm_name in self.program_channel_map:
                final_id = self.program_channel_map[norm_name]
            elif norm_name in COOL9_ID_MAPPING:
                final_id = COOL9_ID_MAPPING[norm_name]

            if not final_id or final_id in self.channel_ids:
                continue

            ch.set("id", final_id)
            self.all_channels.append(ch)
            self.channel_ids.add(final_id)
            self.name_to_final_id[norm_name] = final_id
            add_count += 1
        return add_count

    def get_channel_name_by_id(self, cid: str) -> str:
        for ch in self.all_channels:
            if ch.get("id") == cid:
                names = ch.xpath(".//display-name/text()")
                return names[0] if names else ""
        return ""

    def adjust_program_time(self, program, days=0, hours=0):
        for attr in ["start", "stop"]:
            ts = program.get(attr, "")
            if not ts or " " not in ts:
                continue
            part, tz = ts.split(" ", 1)
            if len(part) < 14:
                continue
            try:
                dt = datetime.strptime(part[:14], "%Y%m%d%H%M%S")
                dt += timedelta(days=days, hours=hours)
                program.set(attr, dt.strftime("%Y%m%d%H%M%S") + " " + tz)
            except Exception:
                pass

    def process_programs(self, xml_tree):
        programs = xml_tree.xpath("//programme")
        ihot_cnt = 0
        for p in programs:
            cid = p.get("channel", "").strip()
            if cid not in self.channel_ids:
                continue
            name = self.get_channel_name_by_id(cid)
            if "iHOT" in name:
                self.adjust_program_time(p, hours=+8)
                ihot_cnt += 1
            self.all_programs.append(p)
        logging.info(f"时间调整完成：iHOT+8小时 {ihot_cnt} 条")

    def generate_default_epg(self):
        """兜底：当没有任何源抓取到数据时生成默认EPG"""
        logging.warning("⚠️  未抓取到任何有效EPG数据，正在生成默认EPG...")

        # 1. 生成默认频道
        for cid, name in COOL9_ID_MAPPING.items():
            chan = etree.Element("channel", id=cid)
            dn = etree.SubElement(chan, "display-name")
            dn.text = name
            self.all_channels.append(chan)
            self.channel_ids.add(cid)
            self.name_to_final_id[name] = cid

        # 2. 生成未来7天每天3个默认节目
        now = datetime.now()
        for cid in self.channel_ids:
            chan_name = self.name_to_final_id[cid]
            for day in range(7):
                base = now + timedelta(days=day)
                start = base.strftime("%Y%m%d000000")
                stop = (base + timedelta(hours=8)).strftime("%Y%m%d080000")
                prog = etree.Element("programme", start=f"{start} +0800", stop=f"{stop} +0800", channel=cid)
                title = etree.SubElement(prog, "title")
                title.text = f"{chan_name} 默认节目{day+1}"
                self.all_programs.append(prog)

        logging.info(f"默认EPG生成完成：频道 {len(self.all_channels)} | 节目 {len(self.all_programs)}")

    def build_xml_tree(self) -> etree._Element:
        root = etree.Element("tv")
        for chan in self.all_channels:
            root.append(chan)
        for prog in self.all_programs:
            root.append(prog)
        return root

    def save_epg(self):
        """最终生成 epg.gz"""
        if not self.all_channels and not self.all_programs:
            self.generate_default_epg()

        tree = self.build_xml_tree()
        xml_str = etree.tostring(tree, encoding="utf-8", xml_declaration=True, pretty_print=True)

        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_str)

        logging.info(f"✅ 最终EPG已保存 -> {gz_path}")
        logging.info(f"📊 最终统计：频道 {len(self.all_channels)} | 节目 {len(self.all_programs)}")

    def run(self):
        sources = self.read_epg_sources()
        if not sources:
            self.generate_default_epg()
            self.save_epg()
            return

        self.pre_fetch_program_channels(sources)

        total_success = 0
        total_fail = 0
        total_chan = 0
        total_prog = 0

        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_src = {executor.submit(self.fetch_single_source, s): s for s in sources}
            for fut in as_completed(future_to_src):
                success, src, retries, chans, progs = fut.result()
                if success:
                    total_success += 1
                    total_chan += chans
                    total_prog += progs
                    # 继续处理频道和节目
                    try:
                        resp = self.session.get(src, timeout=TIMEOUT)
                        if src.endswith(".gz"):
                            content = gzip.decompress(resp.content).decode("utf-8")
                        else:
                            content = resp.text
                        tree = etree.fromstring(self.clean_xml_content(content).encode("utf-8"))
                        self.process_channels(tree, src)
                        self.process_programs(tree)
                    except Exception as e:
                        logging.warning(f"处理 {src} 频道/节目失败：{str(e)}")
                else:
                    total_fail += 1

        logging.info("=" * 60)
        logging.info(f"📌 抓取汇总：")
        logging.info(f"✅ 成功源数量：{total_success}")
        logging.info(f"❌
