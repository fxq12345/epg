import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 配置区 =====================
LOG_FILE = "epg_update.log"
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

# 严格控制合理时间范围：前后7天
DAYS_BEFORE = 7
DAYS_AFTER = 7
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===================== 日志 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===================== 时间控制核心 =====================
now = datetime.now()
TIME_MIN = now - timedelta(days=DAYS_BEFORE)
TIME_MAX = now + timedelta(days=DAYS_AFTER)

def is_time_valid(dt):
    """只保留合理区间时间，过滤过期、超大跨度乱时间"""
    if not dt:
        return False
    return TIME_MIN <= dt <= TIME_MAX

def parse_time_str(time_str):
    """多格式兼容解析时间"""
    if not time_str:
        return None
    try:
        if len(time_str) >= 14:
            return datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
        elif len(time_str) == 12:
            return datetime.strptime(time_str, "%Y%m%d%H%M")
        elif len(time_str) == 8:
            return datetime.strptime(time_str, "%Y%m%d")
        else:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    except:
        return None

def fmt_epg_time(dt):
    """统一输出标准EPG北京时间格式"""
    return dt.strftime("%Y%m%d%H%M%S +0800")

# ===================== 网络请求 =====================
HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

def requests_session_with_retry():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500,502,503,504,522])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_source(url):
    logger.info(f"抓取源: {url}")
    session = requests_session_with_retry()
    try:
        resp = session.get(url, headers=HEADERS, timeout=30, verify=False)
        resp.raise_for_status()
        content = resp.content
        return content, "xml"
    except Exception as e:
        logger.error(f"抓取失败: {e}")
        return None, None

# ===================== XML解析（不修改频道ID，只过滤乱时间） =====================
def parse_xml(content):
    if content.startswith(b'\x1f\x8b'):
        try:
            with io.BytesIO(content) as gzfile:
                with gzip.GzipFile(fileobj=gzfile) as f:
                    content = f.read()
        except:
            logger.warning("解压异常，直接原生解析")

    try:
        root = etree.fromstring(content)
    except:
        logger.error("XML解析失败")
        return {}, []

    channel_dict = {}
    prog_list = []

    # 处理频道：完全原样保留，不修改任何ID
    for ch_node in root.xpath("//channel"):
        ch_id = ch_node.get("id", "").strip()
        if ch_id:
            channel_dict[ch_id] = ch_node

    # 处理节目：只过滤乱时间，不修改channel属性
    valid_cnt = 0
    drop_cnt = 0

    for p_node in root.xpath("//programme"):
        start_str = p_node.get("start", "")
        stop_str = p_node.get("stop", "")
        title = p_node.findtext("title", "")
        channel_id = p_node.get("channel", "")

        dt_start = parse_time_str(start_str)
        dt_stop = parse_time_str(stop_str)

        # 关键：时间不合格直接丢弃，解决时间乱、跨度大问题
        if not is_time_valid(dt_start) or not is_time_valid(dt_stop):
            drop_cnt += 1
            continue

        # 重建标准节目节点，channel属性完全原样保留
        new_p = etree.Element("programme")
        new_p.set("channel", channel_id)
        new_p.set("start", fmt_epg_time(dt_start))
        new_p.set("stop", fmt_epg_time(dt_stop))
        title_node = etree.SubElement(new_p, "title")
        title_node.text = title
        prog_list.append(new_p)
        valid_cnt += 1

    logger.info(f"XML节目有效:{valid_cnt} 丢弃乱时间:{drop_cnt}")
    return channel_dict, prog_list

# ===================== 读取源列表 =====================
def read_url_list():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

# ===================== 主入口 =====================
def main():
    logger.info("========== 开始生成EPG（不修改频道ID版） ==========")
    url_list = read_url_list()
    if not url_list:
        logger.error("config.txt 无源地址")
        return

    all_ch = {}
    all_prog = []

    for url in url_list:
        content, _ = fetch_source(url)
        if not content:
            continue
        chs, progs = parse_xml(content)
        # 合并频道
        for k,v in chs.items():
            if k not in all_ch:
                all_ch[k] = v
        all_prog.extend(progs)

    # 去重
    seen = set()
    unique_prog = []
    for p in all_prog:
        key = (p.get("channel"), p.get("start"))
        if key not in seen:
            seen.add(key)
            unique_prog.append(p)

    # 组装最终XML
    root = etree.Element("tv")
    for ch in all_ch.values():
        root.append(ch)
    for p in unique_prog:
        root.append(p)

    # 压缩保存
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    xml_bytes = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_bytes)

    logger.info(f"完成！频道总数:{len(all_ch)} 有效节目总数:{len(unique_prog)}")
    logger.info("========== 不修改频道ID，只修复时间完成 ==========")

if __name__ == "__main__":
    main()
