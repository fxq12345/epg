import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io

# 配置
LOG_FILE = "epg_update.log"
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
DAYS_BEFORE = 7
DAYS_AFTER = 7
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 繁转简映射
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺","頻":"频","廣":"广","東":"东"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a, b)
    return text.strip()

# 频道标准化（适配酷9的ID匹配规则）
def unified_name(raw_name):
    n = f2s(raw_name).strip()
    lower_n = n.lower()
    if "cctv4k" in lower_n: return "CCTV4K"
    if lower_n in ("cctv4","央视4","cctv-4"): return "CCTV4"
    if lower_n in ("cctv5","央视5","cctv-5"): return "CCTV5"
    if "cctv5+" in lower_n or "cctv5plus" in lower_n: return "CCTV5+"
    if "浙江卫视" in n: return "浙江卫视"
    if "山东" in n and "体育" in n and "休闲" not in n: return "山东体育"
    if "山东卫视" in n: return "山东卫视"
    if "山东齐鲁" in n or "齐鲁频道" in n: return "山东齐鲁"
    if "cctv1" in lower_n or "央视1" in lower_n: return "CCTV1"
    if "cctv2" in lower_n or "央视2" in lower_n: return "CCTV2"
    if "cctv3" in lower_n or "央视3" in lower_n: return "CCTV3"
    if "cctv6" in lower_n or "央视6" in lower_n: return "CCTV6"
    if "cctv7" in lower_n or "央视7" in lower_n: return "CCTV7"
    if "cctv8" in lower_n or "央视8" in lower_n: return "CCTV8"
    return n

# 北京时间基准
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start_day = today - timedelta(days=DAYS_BEFORE)
end_day = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"}

def fetch(url, i):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logging.warning(f"[{i}] 请求失败，状态码：{r.status_code}")
            return None, None, False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:200]) else "json"
        return c, fmt, True
    except Exception as e:
        logging.error(f"[{i}] 请求异常：{str(e)}")
        return None, None, False

# XML解析（修复时间戳，按源日期生成节目单，不跨日期重复）
def parse_xml(content, i):
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        except:
            pass
    try:
        root = etree.fromstring(content)
    except Exception as e:
        logging.error(f"[{i}] XML解析失败：{str(e)}")
        return {}, []
    chs = {}
    progs = []
    # 处理频道
    for ch in root.xpath("//channel"):
        raw = ch.findtext("display-name", "").strip()
        if not raw: continue
        un = unified_name(raw)
        ch.set("id", un)
        if ch.find("display-name") is not None:
            ch.find("display-name").text = un
        chs[un] = ch
    # 处理节目单（直接读取源里的日期，不跨日期生成）
    for p in root.xpath("//programme"):
        rawid = p.get("channel", "").strip()
        if not rawid: continue
        un = unified_name(rawid)
        t = p.find("title")
        title = f2s(t.text) if t is not None else ""
        st_str = p.get("start", "")
        if len(st_str) < 14: continue
        try:
            # 直接解析源里的完整时间，不再和all_days组合
            st_dt = datetime.strptime(st_str[:14], "%Y%m%d%H%M%S")
            st_dt = st_dt.replace(tzinfo=None)
            if not (start_day <= st_dt <= end_day):
                continue
            stop_str = p.get("stop", "")
            if len(stop_str) >= 14:
                et_dt = datetime.strptime(stop_str[:14], "%Y%m%d%H%M%S")
                et_dt = et_dt.replace(tzinfo=None)
            else:
                et_dt = st_dt + timedelta(minutes=30)
            p_new = etree.Element("programme")
            p_new.set("start", st_dt.strftime("%Y%m%d%H%M%S +0800"))
            p_new.set("stop", et_dt.strftime("%Y%m%d%H%M%S +0800"))
            p_new.set("channel", un)
            etree.SubElement(p_new, "title").text = title
            progs.append(p_new)
        except Exception as e:
            logging.debug(f"节目解析失败：{st_str} - {str(e)}")
            continue
    return chs, progs

# JSON解析（按源日期生成节目单，不跨日期重复）
def parse_json(content, i):
    try:
        data = json.loads(content)
    except Exception as e:
        logging.error(f"[{i}] JSON解析失败：{str(e)}")
        return {}, []
    chs = {}
    progs = []
    # 适配你提供的JSON格式：{"channel_name":"cctv1","date":"2026-05-01","epg_data":[{"start":"20:03","end":"20:33","title":""}]}
    if isinstance(data, list):
        for item in data:
            name = item.get("channel_name", "") or item.get("name", "")
            date_str = item.get("date", "")
            plist = item.get("epg_data", []) or item.get("list", [])
            if not name or not date_str or not plist: continue
            un = unified_name(name)
            if un not in chs:
                ch = etree.Element("channel", id=un)
                etree.SubElement(ch, "display-name").text = un
                chs[un] = ch
            try:
                base_day = datetime.strptime(date_str, "%Y-%m-%d")
                if not (start_day <= base_day <= end_day):
                    continue
            except:
                continue
            for prog in plist:
                start_str = prog.get("start", "")
                end_str = prog.get("end", "")
                title = f2s(prog.get("title", ""))
                if not start_str or not end_str: continue
                try:
                    st_time = datetime.strptime(start_str, "%H:%M").time()
                    et_time = datetime.strptime(end_str, "%H:%M").time()
                    st_dt = datetime.combine(base_day, st_time)
                    et_dt = datetime.combine(base_day, et_time)
                    if not (start_day <= st_dt <= end_day):
                        continue
                    p = etree.Element("programme")
                    p.set("start", st_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("stop", et_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("channel", un)
                    etree.SubElement(p, "title").text = title
                    progs.append(p)
                except Exception as e:
                    logging.debug(f"节目解析失败：{start_str} - {str(e)}")
                    continue
    return chs, progs

# 强力去重（按频道+开始时间+结束时间去重，解决重叠）
def dedupe(progs):
    seen = set()
    u = []
    for p in progs:
        key = (p.get("channel"), p.get("start"), p.get("stop"))
        if key not in seen:
            seen.add(key)
            u.append(p)
    return u

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.warning("未找到config.txt")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    if os.path.exists(out_path):
        os.remove(out_path)
    urls = read_config()
    if not urls:
        logging.error("config.txt为空，退出")
        return
    all_ch = {}
    all_prog = []
    for i, url in enumerate(urls, 1):
        c, fmt, ok = fetch(url, i)
        if not ok:
            continue
        if fmt == "xml":
            chs, progs = parse_xml(c, i)
        else:
            chs, progs = parse_json(c, i)
        for cid, ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        all_prog.extend(progs)
    # 兜底频道
    required = ["CCTV1", "CCTV2", "CCTV3", "CCTV4", "CCTV5", "CCTV5+", "CCTV6", "CCTV7", "CCTV8", "浙江卫视", "山东体育", "山东卫视", "山东齐鲁"]
    for name in required:
        if name not in all_ch:
            ch = etree.Element("channel", id=name)
            etree.SubElement(ch, "display-name").text = name
            all_ch[name] = ch
    # 去重
    all_prog = dedupe(all_prog)
    # 生成XML（强制UTF-8声明，兼容酷9）
    root = etree.Element("tv")
    for ch in all_ch.values():
        root.append(ch)
    for p in all_prog:
        root.append(p)
    xml = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml)
    logging.info(f"✅ 完成：频道数{len(all_ch)} 节目数{len(all_prog)}")

if __name__ == "__main__":
    main()
