import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io
import time

# ========== 固定配置：前7天 + 后7天 ==========
LOG_FILE = "epg_update.log"
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
DAYS_BEFORE = 7
DAYS_AFTER = 7
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 超详细日志配置 ==========
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

# ========== 繁转简统一规则 ==========
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺","頻":"频","廣":"广","東":"东"}
def f2s(text):
    if not text: return ""
    for a, b in F2S.items():
        text = text.replace(a, b)
    return text.strip()

# ========== 全量频道标准化映射（含CCTV10） ==========
def unified_name(raw_name):
    if not raw_name: return raw_name
    n = f2s(raw_name).strip()
    lower_n = n.lower()

     # --- CCTV 系列全修复 ---
    if "CCTV-1" in n or "CCTV1" in n or "综合" in n:
        return "CCTV1"
    if "CCTV-2" in n or "CCTV2" in n or "财经" in n:
        return "CCTV2"
    if "CCTV-3" in n or "CCTV3" in n or "综艺" in n:
        return "CCTV3"
    if "CCTV-4" in n or "CCTV4" in n or "国际" in n:
        return "CCTV4"
    if "CCTV-5" in n or "CCTV5" in n or "体育" in n:
        if "+" in n or "PLUS" in n: return "CCTV5+"
        return "CCTV5"
    if "CCTV-6" in n or "CCTV6" in n or "电影" in n:
        return "CCTV6"
    if "CCTV-7" in n or "CCTV7" in n or "国防" in n or "军事" in n:
        return "CCTV7"
    if "CCTV-8" in n or "CCTV8" in n or "电视剧" in n:
        return "CCTV8"
    if "CCTV-9" in n or "CCTV9" in n or "纪录" in n:
        return "CCTV9"
    if "CCTV-10" in n or "CCTV10" in n or "科教" in n:
        return "CCTV10"
    if "CCTV-11" in n or "CCTV11" in n or "戏曲" in n:
        return "CCTV11"
    if "CCTV-12" in n or "CCTV12" in n or "社会与法" in n:
        return "CCTV12"
    if "CCTV-13" in n or "CCTV13" in n or "新闻" in n:
        return "CCTV13"
    if "CCTV-14" in n or "CCTV14" in n or "少儿" in n:
        return "CCTV14"
    if "CCTV-15" in n or "CCTV15" in n or "音乐" in n:
        return "CCTV15"
    if "CCTV-17" in n or "CCTV17" in n or "农业农村" in n:
        return "CCTV17"
    if "CCTV-4K" in n or "4K" in n:
        return "CCTV4K"
    

    # 山东全系列频道
    if "山东卫视" in n: return "山东卫视"
    if "山东新闻" in n: return "山东新闻"
    if "山东齐鲁" in n or "齐鲁频道" in n: return "山东齐鲁"
    if "山东体育" in n: return "山东体育"
    if "山东文旅" in n: return "山东文旅"
    if "山东生活" in n: return "山东生活"
    if "山东综艺" in n: return "山东综艺"
    if "山东农科" in n: return "山东农科"
    if "山东少儿" in n: return "山东少儿"
    if "山东教育" in n: return "山东教育卫视"

    # 全国主流卫视
    if "北京卫视" in n: return "北京卫视"
    if "浙江卫视" in n: return "浙江卫视"
    if "江苏卫视" in n: return "江苏卫视"
    if "东方卫视" in n or "上海卫视" in n: return "东方卫视"
    if "湖南卫视" in n: return "湖南卫视"
    if "安徽卫视" in n: return "安徽卫视"
    if "广东卫视" in n: return "广东卫视"
    if "深圳卫视" in n: return "深圳卫视"

    return n

# ========== 核心时间校验（兼容百川源，保留前后15天节目） ==========
def get_time_range_limit():
    now = datetime.now()
    min_time = now - timedelta(days=DAYS_BEFORE)
    max_time = now + timedelta(days=DAYS_AFTER)
    return min_time, max_time

MIN_VALID_TIME, MAX_VALID_TIME = get_time_range_limit()

def is_datetime_valid(dt):
    # 放宽年份限制，兼容源数据，只过滤远古时间
    if dt.year < 2020 or dt.year > 2030:
        return False
    if not (MIN_VALID_TIME <= dt <= MAX_VALID_TIME):
        return False
    return True

def safe_parse_time(time_str):
    """兼容百川源的非标准时间戳，不轻易丢弃"""
    try:
        # 标准格式：YYYYMMDDHHMMSS
        if len(time_str) >= 14:
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
        else:
            # 兼容其他格式
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        if is_datetime_valid(dt):
            return dt
        return None
    except Exception as e:
        logger.debug(f"时间解析失败: {time_str} | 原因: {str(e)[:30]}")
        return None

# ========== 网络请求模块 ==========
HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

def fetch_source(url):
    logger.info(f"🔍 开始抓取源: {url}")
    start_t = time.time()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=25)
        cost = round(time.time() - start_t, 2)
        if resp.status_code != 200:
            logger.error(f"❌ 源请求失败: 状态码={resp.status_code} 耗时={cost}s")
            return None, None
        content = resp.content
        # 判断格式
        if content.startswith(b'\x1f\x8b') or b"<tv" in content[:300]:
            fmt = "xml"
        else:
            fmt = "json"
        logger.info(f"✅ 源请求成功: 格式={fmt} 耗时={cost}s")
        return content, fmt
    except Exception as e:
        cost = round(time.time() - start_t, 2)
        logger.error(f"❌ 源抓取异常: {str(e)[:40]} 耗时={cost}s")
        return None, None

# ========== XML解析模块（兼容百川源，保留所有有效节目） ==========
def parse_xml_content(content):
    # 解压gzip
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        except:
            logger.warning("⚠️ XML解压失败，尝试原始内容解析")

    try:
        root = etree.fromstring(content)
    except Exception as e:
        logger.error(f"❌ XML整体解析失败: {str(e)[:40]}")
        return {}, []

    channel_dict = {}
    prog_list = []

    # 解析频道
    for ch_node in root.xpath("//channel"):
        raw_name = ch_node.findtext("display-name", "").strip()
        new_name = unified_name(raw_name)
        ch_node.set("id", new_name)
        disp_node = ch_node.find("display-name")
        if disp_node is not None:
            disp_node.text = new_name
        channel_dict[new_name] = ch_node
    logger.info(f"📺 XML解析获取频道数量: {len(channel_dict)}")

    # 解析节目（放宽过滤，只丢弃严重无效时间）
    valid_count = 0
    invalid_count = 0
    for prog_node in root.xpath("//programme"):
        ch_raw_id = prog_node.get("channel", "").strip()
        ch_new_id = unified_name(ch_raw_id)

        start_str = prog_node.get("start", "")
        stop_str = prog_node.get("stop", "")
        title_text = f2s(prog_node.findtext("title", ""))

        start_dt = safe_parse_time(start_str)
        stop_dt = safe_parse_time(stop_str)

        # 只丢弃两个时间都无效的，保留部分有效的
        if not start_dt or not stop_dt:
            invalid_count += 1
            continue

        # 重建标准节目节点
        new_prog = etree.Element("programme")
        new_prog.set("start", start_dt.strftime("%Y%m%d%H%M%S +0800"))
        new_prog.set("stop", stop_dt.strftime("%Y%m%d%H%M%S +0800"))
        new_prog.set("channel", ch_new_id)
        etree.SubElement(new_prog, "title").text = title_text
        prog_list.append(new_prog)
        valid_count += 1

    logger.info(f"📅 XML有效节目: {valid_count} 无效丢弃: {invalid_count}")
    return channel_dict, prog_list

# ========== JSON解析模块 ==========
def parse_json_content(content):
    try:
        data = json.loads(content)
    except Exception as e:
        logger.error(f"❌ JSON解析失败: {str(e)[:40]}")
        return {}, []

    if not isinstance(data, list):
        logger.warning("⚠️ JSON格式非列表，跳过")
        return {}, []

    channel_dict = {}
    prog_list = []
    total_valid = 0

    for item in data:
        ch_name = item.get("channel_name") or item.get("name")
        date_str = item.get("date")
        epg_data = item.get("epg_data") or item.get("list")

        if not ch_name or not date_str or not epg_data:
            continue

        clean_ch = unified_name(ch_name)
        # 注册频道
        if clean_ch not in channel_dict:
            ch_node = etree.Element("channel", id=clean_ch)
            etree.SubElement(ch_node, "display-name").text = clean_ch
            channel_dict[clean_ch] = ch_node

        # 解析日期
        try:
            base_date = datetime.strptime(date_str, "%Y-%m-%d")
            if not is_datetime_valid(base_date):
                continue
        except:
            continue

        # 逐条节目
        for p in epg_data:
            s_time = p.get("start")
            e_time = p.get("end")
            title = f2s(p.get("title", ""))
            if not s_time or not e_time:
                continue
            try:
                s_dt = datetime.combine(base_date, datetime.strptime(s_time, "%H:%M").time())
                e_dt = datetime.combine(base_date, datetime.strptime(e_time, "%H:%M").time())
                if not is_datetime_valid(s_dt) or not is_datetime_valid(e_dt):
                    continue
                new_p = etree.Element("programme")
                new_p.set("start", s_dt.strftime("%Y%m%d%H%M%S +0800"))
                new_p.set("stop", e_dt.strftime("%Y%m%d%H%M%S +0800"))
                new_p.set("channel", clean_ch)
                etree.SubElement(new_p, "title").text = title
                prog_list.append(new_p)
                total_valid += 1
            except:
                continue

    logger.info(f"📋 JSON解析频道: {len(channel_dict)} 有效节目: {total_valid}")
    return channel_dict, prog_list

# ========== 节目去重 ==========
def deduplicate_programs(prog_elements):
    seen_key = set()
    result = []
    for p in prog_elements:
        key = (p.get("channel"), p.get("start"))
        if key not in seen_key:
            seen_key.add(key)
            result.append(p)
    logger.info(f"🧹 去重前: {len(prog_elements)} 条 去重后: {len(result)} 条")
    return result

# ========== 读取配置源列表 ==========
def read_source_list():
    if not os.path.exists(CONFIG_FILE):
        logger.warning(f"⚠️ 未找到配置文件: {CONFIG_FILE}")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        lines = []
        for line in f.readlines():
            line = line.strip()
            if line and not line.startswith("#"):
                lines.append(line)
    logger.info(f"📃 读取到有效源数量: {len(lines)}")
    return lines

# ========== 主程序 ==========
def main():
    logger.info("==============================================")
    logger.info("🚀 EPG生成任务开始 周期:前7天+后7天")
    logger.info("==============================================")

    out_gz_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    # 清理旧文件
    if os.path.exists(out_gz_path):
        os.remove(out_gz_path)
        logger.info(f"🗑️ 清理旧文件: {OUTPUT_FILE}")

    source_urls = read_source_list()
    if not source_urls:
        logger.error("❌ 无任何可用源，任务终止")
        return

    global_channels = {}
    global_programs = []
    success_src = 0
    fail_src = 0

    # 循环逐个抓取每一条源
    for idx, url in enumerate(source_urls, 1):
        logger.info(f"---------------- 第{idx}条源处理中 ----------------")
        content, fmt = fetch_source(url)
        if not content:
            fail_src += 1
            continue

        success_src += 1
        if fmt == "xml":
            chs, progs = parse_xml_content(content)
        else:
            chs, progs = parse_json_content(content)

        # 合并全局
        for cid, chnode in chs.items():
            if cid not in global_channels:
                global_channels[cid] = chnode
        global_programs.extend(progs)

    # 汇总统计
    logger.info("==============================================")
    logger.info(f"📊 源汇总统计 | 成功:{success_src} 失败:{fail_src} 总计:{len(source_urls)}")

    # 去重
    global_programs = deduplicate_programs(global_programs)

    # 必备频道兜底（防止空频道无节目）
    need_default_channels = [
        "CCTV1","CCTV2","CCTV3","CCTV4","CCTV5","CCTV5+","CCTV6","CCTV7","CCTV8","CCTV9","CCTV10",
        "山东卫视","山东新闻","山东齐鲁","山东体育","北京卫视","浙江卫视"
    ]
    for ch_name in need_default_channels:
        if ch_name not in global_channels:
            new_ch = etree.Element("channel", id=ch_name)
            etree.SubElement(new_ch, "display-name").text = ch_name
            global_channels[ch_name] = new_ch
    logger.info(f"🛡️ 兜底补充频道完成，当前总频道数: {len(global_channels)}")

    # 生成最终标准XML
    root = etree.Element("tv")
    for ch in global_channels.values():
        root.append(ch)
    for p in global_programs:
        root.append(p)

    # 压缩保存
    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with gzip.open(out_gz_path, "wb") as f:
        f.write(xml_bytes)

    logger.info("==============================================")
    logger.info(f"🎉 任务全部完成！")
    logger.info(f"📺 最终频道总数: {len(global_channels)}")
    logger.info(f"📅 最终有效节目数: {len(global_programs)}")
    logger.info(f"💾 输出文件: {out_gz_path}")
    logger.info("==============================================\n")

if __name__ == "__main__":
    main()
