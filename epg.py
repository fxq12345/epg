import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 详细日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 【核心】扩展繁体映射 + 台名兜底匹配（专治央视4/5问题）
F2S_MAP = {
    "臺": "台", "衛": "卫", "視": "视", "體": "体", "育": "育",
    "綜": "综", "藝": "艺", "戲": "戏", "劇": "剧", "央視": "央视",
    "新聞": "新闻", "財經": "财经", "體育": "体育", "電影": "电影",
    "少兒": "少儿", "紀錄": "纪录", "國際": "国际", "軍事": "军事",
    "農業": "农业", "科教": "科教", "戲曲": "戏曲", "音樂": "音乐",
    "少兒": "少儿", "動畫": "动画", "影視": "影视", "綜藝": "综艺",
    "央視4": "CCTV4", "央視5": "CCTV5", "央視5+": "CCTV5PLUS",
    "CCTV-4": "CCTV4", "CCTV-5": "CCTV5", "CCTV4K": "CCTV4K"
}

def simple_f2s(text):
    if not text:
        return text
    for f, s in F2S_MAP.items():
        text = text.replace(f, s)
    return text

# 台名兜底映射（解决ID不匹配问题）
CHANNEL_ALIAS = {
    "cctv4": ["cctv4", "cctv-4", "央视4", "央视国际", "CCTV4中文国际"],
    "cctv5": ["cctv5", "cctv-5", "央视5", "央视体育", "CCTV5体育"],
}

# 自动时间区间
now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==================== 下载 ====================
def fetch(url, index):
    try:
        logging.info(f"[{index}] 下载源: {url[:60]}...")
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logging.warning(f"[{index}] 下载失败 状态码:{r.status_code}")
            return None, None, False
        content = r.content
        fmt = detect_format(content, url)
        logging.info(f"[{index}] 下载成功 格式:{fmt}")
        return content, fmt, True
    except Exception as e:
        logging.warning(f"[{index}] 下载异常:{str(e)[:50]}")
        return None, None, False

# ==================== 格式判断 ====================
def detect_format(content, url):
    if content.startswith(b'\x1f\x8b'):
        return "xml"
    if b'<?xml' in content[:100] or b'<tv' in content[:100]:
        return "xml"
    try:
        if content.startswith(b'{') or content.startswith(b'['):
            json.loads(content.decode('utf-8', 'ignore')[:200])
            return "json"
    except:
        pass
    return "unknown"

# ==================== 解析 XML（修复繁体央视4/5 + 台名兜底） ====================
def parse_xml(content, index):
    channels = {}
    programs = []
    try:
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        s = content.decode('utf-8', 'ignore')
        root = etree.fromstring(s.encode('utf-8'))

        # 频道处理：先繁简转换再清理ID + 台名兜底
        for ch in root.xpath("//channel"):
            raw_cid = ch.get("id")
            if not raw_cid:
                continue
            raw_name = ch.findtext("display-name", "")
            # 先对ID和名称做繁简转换
            fixed_cid = simple_f2s(raw_cid)
            fixed_name = simple_f2s(raw_name)
            # 台名兜底匹配：如果名称是央视4/5，强制统一ID
            clean_cid = re.sub(r'[^a-zA-Z0-9_-]', '', fixed_cid).lower()
            clean_name = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5]', '', fixed_name).lower()

            # 央视4/5强制归一化ID
            if any(kw in clean_cid or kw in clean_name for kw in ["cctv4", "央视4", "国际"]):
                clean_cid = "cctv4"
            elif any(kw in clean_cid or kw in clean_name for kw in ["cctv5", "央视5", "体育"]):
                clean_cid = "cctv5"

            # 更新频道ID和名称
            ch.set("id", clean_cid)
            name_node = ch.find("display-name")
            if name_node is not None:
                name_node.text = fixed_name
            channels[clean_cid] = ch
            logging.debug(f"[{index}] 频道归一化 {raw_cid} → {clean_cid} | 名称:{fixed_name}")

        # 节目处理：同样转换频道ID + 台名兜底
        for p in root.xpath("//programme"):
            st = parse_program_time(p.get("start"))
            if not st or not (start_cutoff <= st <= end_cutoff):
                continue

            raw_p_cid = p.get("channel")
            fixed_p_cid = simple_f2s(raw_p_cid)
            clean_cid = re.sub(r'[^a-zA-Z0-9_-]', '', fixed_p_cid).lower()

            # 节目ID也做央视4/5归一化
            if "cctv4" in clean_cid or "央视4" in clean_cid or "国际" in clean_cid:
                clean_cid = "cctv4"
            elif "cctv5" in clean_cid or "央视5" in clean_cid or "体育" in clean_cid:
                clean_cid = "cctv5"

            p.set("channel", clean_cid)

            # 节目名称转简体
            title_node = p.find("title")
            if title_node is not None and title_node.text:
                title_node.text = simple_f2s(title_node.text)

            programs.append(p)

        logging.info(f"[{index}] 解析完成 频道:{len(channels)} 节目:{len(programs)}")
        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.error(f"[{index}] XML解析异常:{str(e)}")
        return {}, [], 0, 0

# ==================== 解析 JSON（百川兼容+繁简） ====================
def parse_json(content, index):
    channels = {}
    programs = []
    try:
        data = json.loads(content.decode('utf-8', 'ignore'))
        for item in data:
            tvid = item.get("tvid") or item.get("id")
            name = item.get("name")
            plist = item.get("list", [])
            if not tvid or not name:
                continue

            # 繁体修复 + 央视4/5归一化
            fixed_tvid = simple_f2s(tvid)
            clean_tvid = re.sub(r'[^a-zA-Z0-9_-]', '', fixed_tvid).lower()
            clear_name = simple_f2s(name)
            clean_name = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5]', '', clear_name).lower()

            # 强制归一化央视4/5
            if any(kw in clean_tvid or kw in clean_name for kw in ["cctv4", "央视4", "国际"]):
                cid = "cctv4"
            elif any(kw in clean_tvid or kw in clean_name for kw in ["cctv5", "央视5", "体育"]):
                cid = "cctv5"
            else:
                cid = clean_tvid

            if cid not in channels:
                ch = etree.Element("channel", id=cid)
                etree.SubElement(ch, "display-name").text = clear_name
                channels[cid] = ch

            for prog in plist:
                t = prog.get("time")
                title = simple_f2s(prog.get("program", ""))
                try:
                    bt = datetime.combine(today, datetime.strptime(t, "%H:%M").time())
                    while bt < start_cutoff:
                        bt += timedelta(days=1)
                    while bt > end_cutoff:
                        bt -= timedelta(days=1)
                    et = bt + timedelta(minutes=30)
                    p = etree.Element("programme")
                    p.set("start", bt.strftime("%Y%m%d%H%M%S 0"))
                    p.set("stop", et.strftime("%Y%m%d%H%M%S 0"))
                    p.set("channel", cid)
                    etree.SubElement(p, "title").text = title
                    programs.append(p)
                except:
                    continue
        logging.info(f"[{index}] JSON解析完成 频道:{len(channels)} 节目:{len(programs)}")
        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.error(f"[{index}] JSON解析异常:{str(e)}")
        return {}, [], 0, 0

# ==================== 时间解析 ====================
def parse_program_time(ts):
    if not ts:
        return None
    try:
        p = ts.split()[0]
        if len(p) >= 14:
            return datetime.strptime(p[:14], "%Y%m%d%H%M%S")
    except:
        pass
    return None

# ==================== 读取配置 ====================
def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.error("不存在 config.txt")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

# ==================== 主程序 ====================
def main():
    urls = read_config()
    if not urls:
        logging.error("无数据源链接")
        return

    all_ch = {}
    all_prog = []

    for i, url in enumerate(urls, 1):
        c, fmt, ok = fetch(url, i)
        if not ok or not c:
            continue
        if fmt == "xml":
            chs, progs, _, _ = parse_xml(c, i)
        elif fmt == "json":
            chs, progs, _, _ = parse_json(c, i)
        else:
            logging.warning(f"[{i}] 不支持的格式跳过")
            continue

        for cid, ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        all_prog.extend(progs)

    if all_ch and all_prog:
        root = etree.Element("tv")
        for ch in all_ch.values():
            root.append(ch)
        for p in all_prog:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out, "wb") as f:
            f.write(xml_bytes)
        logging.info(f"✅ 生成完毕 | 总频道:{len(all_ch)} | 总节目:{len(all_prog)}")
    else:
        logging.warning("⚠️ 无有效数据生成")

if __name__ == "__main__":
    main()
