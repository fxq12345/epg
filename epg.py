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

# 严格控制合理时间范围：前后7天，不长跨度、不杂乱
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

# ===================== 繁转简 =====================
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺","頻":"频","廣":"广","東":"东"}
def f2s(text):
    if not text: return ""
    for a, b in F2S.items():
        text = text.replace(a, b)
    return text.strip()

# ===================== 频道标准化映射 =====================
def unified_name(raw_name):
    if not raw_name: return raw_name
    n = f2s(raw_name).strip()

    # CCTV全系列
    if any(x in n for x in ["CCTV-1", "CCTV1", "综合"]):
        return "CCTV1"
    if any(x in n for x in ["CCTV-2", "CCTV2", "财经"]):
        return "CCTV2"
    if any(x in n for x in ["CCTV-3", "CCTV3", "综艺"]):
        return "CCTV3"
    if any(x in n for x in ["CCTV-4", "CCTV4", "国际"]):
        return "CCTV4"
    if any(x in n for x in ["CCTV-5", "CCTV5", "体育"]) and not any(x in n for x in ["+", "5+"]):
        return "CCTV5"
    if any(x in n for x in ["CCTV5+", "CCTV-5+", "5+"]):
        return "CCTV5+"
    if any(x in n for x in ["CCTV-6", "CCTV6", "电影"]):
        return "CCTV6"
    if any(x in n for x in ["CCTV-7", "CCTV7", "国防", "军事"]):
        return "CCTV7"
    if any(x in n for x in ["CCTV-8", "CCTV8", "电视剧"]):
        return "CCTV8"
    if any(x in n for x in ["CCTV-9", "CCTV9", "纪录"]):
        return "CCTV9"
    if any(x in n for x in ["CCTV-10", "CCTV10", "科教"]):
        return "CCTV10"
    if any(x in n for x in ["CCTV-11", "CCTV11", "戏曲"]):
        return "CCTV11"
    if any(x in n for x in ["CCTV-12", "CCTV12", "社会与法"]):
        return "CCTV12"
    if any(x in n for x in ["CCTV-13", "CCTV13", "新闻"]):
        return "CCTV13"
    if any(x in n for x in ["CCTV-14", "CCTV14", "少儿"]):
        return "CCTV14"
    if any(x in n for x in ["CCTV-15", "CCTV15", "音乐"]):
        return "CCTV15"
    if any(x in n for x in ["CCTV-17", "CCTV17", "农业农村", "农业"]):
        return "CCTV17"
    if any(x in n for x in ["4K", "CCTV4K", "CCTV-4K"]):
        return "CCTV4K"

    # 山东频道
    if "山东卫视" in n:
        return "山东卫视"
    if any(x in n for x in ["山东新闻", "新闻频道"]):
        return "山东新闻"
    if any(x in n for x in ["山东齐鲁", "齐鲁频道"]):
        return "山东齐鲁"
    if any(x in n for x in ["山东体育", "体育休闲"]):
        return "山东体育"
    if any(x in n for x in ["山东文旅", "文旅频道"]):
        return "山东文旅"
    if any(x in n for x in ["山东生活", "生活频道"]):
        return "山东生活"
    if any(x in n for x in ["山东综艺", "综艺频道"]):
        return "山东综艺"
    if any(x in n for x in ["山东农科", "农科频道"]):
        return "山东农科"
    if any(x in n for x in ["山东少儿", "少儿频道"]):
        return "山东少儿"
    if any(x in n for x in ["山东教育", "教育卫视"]):
        return "山东教育卫视"

    # 主流卫视
    if any(x in n for x in ["北京卫视", "BTV"]):
        return "北京卫视"
    if "浙江卫视" in n: return "浙江卫视"
    if "江苏卫视" in n: return "江苏卫视"
    if any(x in n for x in ["东方卫视", "上海卫视"]): return "东方卫视"
    if "湖南卫视" in n: return "湖南卫视"
    if "安徽卫视" in n: return "安徽卫视"
    if "广东卫视" in n: return "广东卫视"
    if "深圳卫视" in n: return "深圳卫视"

    return n

# ===================== 时间控制核心（修复错乱关键） =====================
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
        if content.startswith(b'\x1f\x8b'):
            return content, "xml"
        try:
            json.loads(content)
            return content, "json"
        except:
            return content, "xml"
    except Exception as e:
        logger.error(f"抓取失败: {e}")
        return None, None

# ===================== XML解析 =====================
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
    id_map = {}

    # 处理频道
    for ch_node in root.xpath("//channel"):
        raw_id = ch_node.get("id", "").strip()
        raw_name = ch_node.findtext("display-name", "").strip()
        std_name = unified_name(raw_name)
        id_map[raw_id] = std_name
        id_map[raw_name] = std_name
        ch_node.set("id", std_name)
        ch_node.find("display-name").text = std_name
        channel_dict[std_name] = ch_node

    # 处理节目 + 严格时间过滤
    valid_cnt = 0
    drop_cnt = 0

    for p_node in root.xpath("//programme"):
        raw_ch = p_node.get("channel", "").strip()
        # 匹配标准频道ID
        if raw_ch in id_map:
            std_ch = id_map[raw_ch]
        else:
            std_ch = unified_name(raw_ch)

        start_str = p_node.get("start", "")
        stop_str = p_node.get("stop", "")
        title = f2s(p_node.findtext("title", ""))

        dt_start = parse_time_str(start_str)
        dt_stop = parse_time_str(stop_str)

        # 关键：时间不合格直接丢弃，解决时间乱、跨度大问题
        if not is_time_valid(dt_start) or not is_time_valid(dt_stop):
            drop_cnt += 1
            continue

        # 重建标准节目节点
        new_p = etree.Element("programme")
        new_p.set("channel", std_ch)
        new_p.set("start", fmt_epg_time(dt_start))
        new_p.set("stop", fmt_epg_time(dt_stop))
        title_node = etree.SubElement(new_p, "title")
        title_node.text = title
        prog_list.append(new_p)
        valid_cnt += 1

    logger.info(f"XML节目有效:{valid_cnt} 丢弃乱时间:{drop_cnt}")
    return channel_dict, prog_list

# ===================== JSON解析 =====================
def parse_json(content):
    try:
        data = json.loads(content)
    except:
        return {}, []

    channel_dict = {}
    prog_list = []

    for item in data:
        ch_name = item.get("channel_name") or item.get("name")
        date_str = item.get("date")
        epg_list = item.get("epg_data") or item.get("list")
        if not all([ch_name, date_str, epg_list]):
            continue

        std_ch = unified_name(ch_name)
        if std_ch not in channel_dict:
            ch_node = etree.Element("channel")
            ch_node.set("id", std_ch)
            dn = etree.SubElement(ch_node, "display-name")
            dn.text = std_ch
            channel_dict[std_ch] = ch_node

        try:
            base_day = datetime.strptime(date_str, "%Y-%m-%d")
        except:
            continue

        for p in epg_list:
            s_time = p.get("start")
            e_time = p.get("end")
            title = f2s(p.get("title", ""))
            if not s_time or not e_time:
                continue

            try:
                sh, sm = int(s_time.split(":")[0]), int(s_time.split(":")[1])
                eh, em = int(e_time.split(":")[0]), int(e_time.split(":")[1])
                dt_s = base_day + timedelta(hours=sh, minutes=sm)
                dt_e = base_day + timedelta(hours=eh, minutes=em)
            except:
                continue

            if not is_time_valid(dt_s) or not is_time_valid(dt_e):
                continue

            new_p = etree.Element("programme")
            new_p.set("channel", std_ch)
            new_p.set("start", fmt_epg_time(dt_s))
            new_p.set("stop", fmt_epg_time(dt_e))
            t_node = etree.SubElement(new_p, "title")
            t_node.text = title
            prog_list.append(new_p)

    return channel_dict, prog_list

# ===================== 读取源列表 =====================
def read_url_list():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

# ===================== 主入口 =====================
def main():
    logger.info("========== 开始生成EPG（央视时间修复版） ==========")
    url_list = read_url_list()
    if not url_list:
        logger.error("config.txt 无源地址")
        return

    all_ch = {}
    all_prog = []

    for url in url_list:
        content, ctype = fetch_source(url)
        if not content:
            continue
        if ctype == "json":
            chs, progs = parse_json(content)
        else:
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
    logger.info("========== 央视时间修复完成 ==========")

if __name__ == "__main__":
    main()
