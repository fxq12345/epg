import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
LOG_FILE = "epg_log.txt"

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a,b)
    return text

# ==================== 核心：CCTV4严格等于匹配 ====================
def unified_name(raw_name):
    n = f2s(raw_name).strip()
    lower_n = n.lower()

    # 【1】先匹配CCTV4K，避免被误判
    if "cctv4k" in lower_n or "央视4k" in lower_n:
        return "CCTV4K"

    # 【2】CCTV4必须严格等于，杜绝CCTV4AME/4K等干扰
    if lower_n == "cctv4" or lower_n == "央视4" or n == "中央电视台-4":
        return "CCTV4"

    # 【3】其他频道正常匹配
    if lower_n == "cctv5" or lower_n == "央视5":
        return "CCTV5"
    if "cctv5+" in lower_n or "5+体育" in lower_n:
        return "CCTV5+"
    if "浙江卫视" in n:
        return "浙江卫视"
    if "山东" in n and "体育" in n and "休闲" not in n:
        return "山东体育"
    if "山东卫视" in n:
        return "山东卫视"
    if "山东齐鲁" in n or "齐鲁频道" in n:
        return "山东齐鲁"
    if "山东生活" in n:
        return "山东生活"
    if "山东少儿" in n:
        return "山东少儿"

    return n

# 时间区间
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start = today - timedelta(days=DAYS_BEFORE)
end = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent":"Mozilla/5.0"}

def fetch(url, i):
    logging.info(f"【第{i}条】抓取: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logging.error(f"【第{i}条】失败 码:{r.status_code}")
            return None, None, False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:100]) else "json"
        logging.info(f"【第{i}条】成功 格式:{fmt}")
        return c, fmt, True
    except Exception as e:
        logging.error(f"【第{i}条】异常:{str(e)}")
        return None, None, False

def parse_time(s):
    try:
        return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
    except:
        return None

def parse_xml(content, i):
    if content.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
            content = f.read()
    root = etree.fromstring(content)
    chs = {}
    progs = []
    have_sd = False

    for ch in root.xpath("//channel"):
        raw = ch.findtext("display-name", "")
        un = unified_name(raw)
        ch.set("id", un)
        if ch.find("display-name"):
            ch.find("display-name").text = un
        chs[un] = ch
        if un == "山东体育":
            have_sd = True
            logging.info(f"【第{i}条】找到山东体育: {raw}")

    # 放开日期过滤，保留全部历史节目
    for p in root.xpath("//programme"):
        st = parse_time(p.get("start", ""))
        rawid = p.get("channel", "")
        un = unified_name(rawid)
        p.set("channel", un)
        t = p.find("title")
        if t:
            t.text = f2s(t.text)
        progs.append(p)

    logging.info(f"【第{i}条】解析完毕 频道:{len(chs)} 节目:{len(progs)}")
    if not have_sd:
        logging.warning(f"【第{i}条】本条无山东体育")
    return chs, progs

def parse_json(content, i):
    data = json.loads(content)
    chs = {}
    progs = []
    have_sd = False

    for item in data:
        name = item.get("name", "")
        plist = item.get("list", [])
        un = unified_name(name)
        if un not in chs:
            ch = etree.Element("channel", id=un)
            etree.SubElement(ch, "display-name").text = un
            chs[un] = ch
        if un == "山东体育":
            have_sd = True
            logging.info(f"【第{i}条】找到山东体育: {name}")

        for prog in plist:
            t = prog.get("time", "")
            title = f2s(prog.get("program", ""))
            try:
                bt = datetime.combine(today, datetime.strptime(t, "%H:%M").time())
                et = bt + timedelta(minutes=30)
                p = etree.Element("programme")
                p.set("start", bt.strftime("%Y%m%d%H%M%S 0"))
                p.set("stop", et.strftime("%Y%m%d%H%M%S 0"))
                p.set("channel", un)
                etree.SubElement(p, "title").text = title
                progs.append(p)
            except:
                pass

    logging.info(f"【第{i}条】解析完毕 频道:{len(chs)} 节目:{len(progs)}")
    if not have_sd:
        logging.warning(f"【第{i}条】本条无山东体育")
    return chs, progs

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.error("无config.txt")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    logging.info(f"读取源总数: {len(lines)}")
    return lines

def main():
    urls = read_config()
    all_ch = {}
    all_prog = []

    for i, url in enumerate(urls, 1):
        c, fmt, ok = fetch(url, i)
        if not ok:
            continue
        chs, progs = parse_xml(c, i) if fmt == "xml" else parse_json(c, i)
        
        for cid, ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        all_prog.extend(progs)

    # 兜底：保证频道一定存在
    if "山东体育" not in all_ch:
        logging.info("🔥 强制兜底：手动添加【山东体育】频道占位")
        sd_ch = etree.Element("channel", id="山东体育")
        etree.SubElement(sd_ch, "display-name").text = "山东体育"
        all_ch["山东体育"] = sd_ch
    if "CCTV4" not in all_ch:
        logging.info("🔥 强制兜底：手动添加【CCTV4】频道占位")
        c4_ch = etree.Element("channel", id="CCTV4")
        etree.SubElement(c4_ch, "display-name").text = "CCTV4"
        all_ch["CCTV4"] = c4_ch

    logging.info("==========汇总==========")
    logging.info(f"总频道: {len(all_ch)}")
    logging.info(f"总节目: {len(all_prog)}")
    logging.info("✅ CCTV4 已严格匹配，杜绝干扰")
    logging.info("✅ 山东体育 已强制写入列表")

    root = etree.Element("tv")
    for ch in all_ch.values():
        root.append(ch)
    for p in all_prog:
        root.append(p)
    xml = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with gzip.open(out, "wb") as f:
        f.write(xml)
    logging.info(f"✅ 生成完成: {out}")

if __name__ == "__main__":
    main()
