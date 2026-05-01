import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io
import locale

# --- 时区强制统一 彻底解决1970 ---
try:
    locale.setlocale(locale.LC_TIME, 'zh_CN.UTF-8')
except:
    pass

# --- 日志配置 ---
LOG_FILE = "epg_update.log"

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
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

# 繁转简
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a,b)
    return text

# ==================== 频道标准化 ====================
def unified_name(raw_name):
    n = f2s(raw_name).strip()
    lower_n = n.lower()
    
    if "cctv4k" in lower_n or "央视4k" in lower_n:
        return "CCTV4K"
    if lower_n == "cctv4" or lower_n == "央视4" or n == "中央电视台-4":
        return "CCTV4"
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

# ==================== 标准北京时间获取 彻底修复1970 ====================
def get_cn_time():
    return datetime.now().astimezone().replace(tzinfo=None)

now = get_cn_time()
today = datetime(now.year, now.month, now.day)
all_days = [today + timedelta(days=d) for d in range(-DAYS_BEFORE, DAYS_AFTER+1)]

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

# ==================== XML解析 + 补全日期 ====================
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

    temp_map = {}
    for p in root.xpath("//programme"):
        rawid = p.get("channel", "")
        un = unified_name(rawid)
        p.set("channel", un)
        t = p.find("title")
        title = f2s(t.text) if t else ""
        st_str = p.get("start","")
        try:
            st_time = datetime.strptime(st_str[:14], "%Y%m%d%H%M%S").time()
            key = (un, st_time.strftime("%H:%M"))
            temp_map[key] = title
        except:
            continue

    for base_day in all_days:
        for (chan, tm), title in temp_map.items():
            try:
                bt = datetime.combine(base_day, datetime.strptime(tm, "%H:%M").time())
                et = bt + timedelta(minutes=30)
                new_p = etree.Element("programme")
                new_p.set("start", bt.strftime("%Y%m%d%H%M%S 0"))
                new_p.set("stop", et.strftime("%Y%m%d%H%M%S 0"))
                new_p.set("channel", chan)
                etree.SubElement(new_p, "title").text = title
                progs.append(new_p)
            except:
                continue

    logging.info(f"【第{i}条】XML频道:{len(chs)} 节目:{len(progs)}")
    return chs, progs

# ==================== JSON解析 + 补全日期 ====================
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

        for base_day in all_days:
            for prog in plist:
                t = prog.get("time", "")
                title = f2s(prog.get("program", ""))
                try:
                    bt = datetime.combine(base_day, datetime.strptime(t, "%H:%M").time())
                    et = bt + timedelta(minutes=30)
                    p = etree.Element("programme")
                    p.set("start", bt.strftime("%Y%m%d%H%M%S 0"))
                    p.set("stop", et.strftime("%Y%m%d%H%M%S 0"))
                    p.set("channel", un)
                    etree.SubElement(p, "title").text = title
                    progs.append(p)
                except Exception:
                    continue

    logging.info(f"【第{i}条】JSON频道:{len(chs)} 节目:{len(progs)}")
    return chs, progs

# ==================== 强力去重 解决文件超大 ====================
def dedupe_programs(progs):
    seen = set()
    unique = []
    for p in progs:
        key = (
            p.get("channel", "").strip(),
            p.get("start", "").strip(),
            p.get("stop", "").strip()
        )
        if key not in seen:
            seen.add(key)
            unique.append(p)
    logging.info(f"节目去重瘦身：{len(progs)} → {len(unique)}")
    return unique

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.error("无config.txt")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    logging.info(f"读取源总数: {len(lines)}")
    return lines

def main():
    # 清空旧垃圾
    if os.path.exists(os.path.join(OUTPUT_DIR, OUTPUT_FILE)):
        os.remove(os.path.join(OUTPUT_DIR, OUTPUT_FILE))

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

    # 兜底频道
    if "山东体育" not in all_ch:
        sd_ch = etree.Element("channel", id="山东体育")
        etree.SubElement(sd_ch, "display-name").text = "山东体育"
        all_ch["山东体育"] = sd_ch
        
    if "CCTV4" not in all_ch:
        c4_ch = etree.Element("channel", id="CCTV4")
        etree.SubElement(c4_ch, "display-name").text = "CCTV4"
        all_ch["CCTV4"] = c4_ch

    # 强力去重瘦身
    all_prog = dedupe_programs(all_prog)

    logging.info("==========汇总==========")
    logging.info(f"总频道: {len(all_ch)}")
    logging.info(f"最终有效节目: {len(all_prog)}")
    
    root = etree.Element("tv")
    for ch in all_ch.values():
        root.append(ch)
    for p in all_prog:
        root.append(p)
        
    xml = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with gzip.open(out, "wb") as f:
        f.write(xml)
    logging.info(f"✅ 瘦身版EPG生成完成: {out}")

if __name__ == "__main__":
    main()
