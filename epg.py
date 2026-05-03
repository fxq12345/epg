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
JSON_MAP_FILE = "epg_data.json"

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
    if not dt:
        return False
    return TIME_MIN <= dt <= TIME_MAX

def parse_time_str(time_str):
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

# ===================== 核心修改：适配酷九的epgid映射 + 容错 =====================
def load_channel_map():
    """
    读取 epg_data.json，生成以 epgid 为标准ID的映射表
    - alias_to_std: {所有别名/tvid/epgid → 标准epgid（如CCTV1）}
    - std_to_display: {标准epgid → 频道显示名}
    """
    alias_to_std = {}
    std_to_display = {}
    if not os.path.exists(JSON_MAP_FILE):
        logger.warning(f"未找到映射文件: {JSON_MAP_FILE}，将使用原始名称")
        return alias_to_std, std_to_display

    try:
        with open(JSON_MAP_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for item in data.get("epgs", []):
            tvid = item.get("tvid", "").strip()
            epgid = item.get("epgid", "").strip()  # 标准ID，酷九认这个
            display_name = item.get("note", "").strip() or epgid
            names_str = item.get("name", "")

            if not epgid:
                continue

            # 1. 把epgid本身加入映射
            alias_to_std[epgid.lower()] = epgid
            # 2. 把tvid也加入映射（适配数字ID源）
            if tvid:
                alias_to_std[tvid.lower()] = epgid

            # 3. 把所有别名加入映射
            aliases = names_str.split(",")
            for alias in aliases:
                clean_alias = alias.strip().lower()
                if clean_alias:
                    alias_to_std[clean_alias] = epgid

            # 4. 记录标准ID对应的显示名，用于生成channel标签
            std_to_display[epgid] = display_name

        logger.info(f"成功加载频道映射表，共 {len(alias_to_std)} 个别名映射")
        return alias_to_std, std_to_display

    except Exception as e:
        logger.error(f"解析JSON映射表失败: {e}，将使用原始频道ID")
        # 【关键修复】解析失败时返回空映射，程序继续运行
        return {}, {}

def get_standard_id(raw_id, alias_to_std):
    """
    根据原始ID或名称，返回统一的标准epgid（适配酷九）
    """
    if not raw_id:
        return None

    clean_id = raw_id.strip().lower()

    # 1. 直接查表
    if clean_id in alias_to_std:
        return alias_to_std[clean_id]

    # 2. 兜底：如果是数字，尝试匹配（比如"1" → "CCTV1"）
    if clean_id.isdigit():
        cctv_candidate = f"cctv{clean_id}"
        if cctv_candidate in alias_to_std:
            return alias_to_std[cctv_candidate]

    # 3. 兜底：如果是CCTV开头的，尝试提取数字匹配
    if clean_id.startswith("cctv"):
        num = ''.join([c for c in clean_id if c.isdigit()])
        if num:
            cctv_candidate = f"cctv{num}"
            if cctv_candidate in alias_to_std:
                return alias_to_std[cctv_candidate]

    # 4. 最终兜底：返回原清洗后的ID（保证不会丢频道）
    return clean_id.replace(" ", "").replace("-", "").lower()

# ===================== XML解析 =====================
def parse_xml(content, alias_to_std):
    """
    解析XML，并利用 alias_to_std 统一频道ID为标准epgid（适配酷九）
    """
    if not content:
        return []

    if content.startswith(b'\x1f\x8b'):
        try:
            with io.BytesIO(content) as gzfile:
                with gzip.GzipFile(fileobj=gzfile) as f:
                    content = f.read()
        except:
            logger.warning("解压异常，尝试原生解析")

    try:
        root = etree.fromstring(content)
    except:
        logger.error("XML解析失败")
        return []

    # --- 第一步：建立当前源的 Channel ID → 频道名 映射 ---
    id_to_name_map = {}
    for ch_node in root.xpath("//channel"):
        ch_id = ch_node.get("id", "").strip()
        disp_name = ch_node.findtext("display-name")
        if ch_id and disp_name:
            id_to_name_map[ch_id] = disp_name

    # --- 第二步：处理节目 ---
    valid_cnt = 0
    drop_cnt = 0
    temp_programs = []

    for p_node in root.xpath("//programme"):
        start_str = p_node.get("start", "")
        stop_str = p_node.get("stop", "")
        old_ch_id = p_node.get("channel", "")

        # 1. 时间校验
        dt_start = parse_time_str(start_str)
        dt_stop = parse_time_str(stop_str)
        if not is_time_valid(dt_start) or not is_time_valid(dt_stop):
            drop_cnt += 1
            continue

        # 2. ID 转换核心逻辑
        # 先通过 old_ch_id 找频道名
        raw_name = id_to_name_map.get(old_ch_id, old_ch_id)
        # 再通过名字/ID查表，得到标准epgid
        standard_id = get_standard_id(raw_name, alias_to_std)

        if not standard_id:
            drop_cnt += 1
            continue

        # 3. 创建新的标准化节目节点
        new_p = etree.Element("programme")
        new_p.set("channel", standard_id)
        new_p.set("start", fmt_epg_time(dt_start))
        new_p.set("stop", fmt_epg_time(dt_stop))

        # 复制其他信息
        for child in p_node:
            new_p.append(child)

        temp_programs.append(new_p)
        valid_cnt += 1

    logger.info(f"处理完成: 有效节目 {valid_cnt}, 丢弃 {drop_cnt}")
    return temp_programs

# ===================== 主程序 =====================
def read_url_list():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

def main():
    logger.info("========== 开始生成EPG（酷九epgid适配版） ==========")

    # 1. 加载映射表
    alias_to_std, std_to_display = load_channel_map()

    url_list = read_url_list()
    if not url_list:
        logger.error("config.txt 为空")
        return

    all_programs = []

    for url in url_list:
        content, _ = fetch_source(url)
        if not content:
            continue

        progs = parse_xml(content, alias_to_std)
        all_programs.extend(progs)

    # --- 节目去重逻辑 ---
    logger.info("正在进行节目单去重...")
    seen_prog = set()
    unique_programs = []
    for p in all_programs:
        key = (p.get("channel"), p.get("start"))
        if key not in seen_prog:
            seen_prog.add(key)
            unique_programs.append(p)

    # --- 关键修复：收集所有节目里出现过的频道ID ---
    all_channel_ids = set()
    for p in unique_programs:
        all_channel_ids.add(p.get("channel"))
    logger.info(f"本次解析到的频道总数: {len(all_channel_ids)}")

    # --- 生成最终XML ---
    root = etree.Element("tv")

    # 【核心修复】优先使用映射表的频道信息，无映射时用原始ID作为频道名
    for ch_id in all_channel_ids:
        display_name = std_to_display.get(ch_id, ch_id)
        ch_node = etree.SubElement(root, "channel")
        ch_node.set("id", ch_id)
        dn_node = etree.SubElement(ch_node, "display-name", attrib={"lang":"zh"})
        dn_node.text = display_name

    # 添加节目
    for p in unique_programs:
        root.append(p)

    # --- 输出 ---
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    xml_bytes = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_bytes)

    logger.info(f"完成！总频道数: {len(all_channel_ids)}, 总节目数: {len(unique_programs)}")
    logger.info(f"文件已保存至: {out_path}")
    logger.info("========== 结束 ==========")

if __name__ == "__main__":
    main()
