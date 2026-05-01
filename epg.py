import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io

# 日志
LOG_FILE = "epg_update.log"
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
        logging.StreamHandler()
    ]
)

# 繁转简
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items(): text=text.replace(a,b)
    return text

# 频道标准化
def unified_name(raw_name):
    n = f2s(raw_name).strip()
    lower_n = n.lower()
    if "cctv4k" in lower_n: return "CCTV4K"
    if lower_n in ("cctv4","央视4"): return "CCTV4"
    if lower_n in ("cctv5","央视5"): return "CCTV5"
    if "cctv5+" in lower_n: return "CCTV5+"
    if "浙江卫视" in n: return "浙江卫视"
    if "山东" in n and "体育" in n and "休闲" not in n: return "山东体育"
    if "山东卫视" in n: return "山东卫视"
    if "山东齐鲁" in n or "齐鲁频道" in n: return "山东齐鲁"
    return n

# 北京时间基准（关键：GitHub也正确）
now = datetime.now()
today = datetime(now.year, now.month, now.day)
all_days = [today+timedelta(days=d) for d in range(-DAYS_BEFORE, DAYS_AFTER+1)]

HEADERS = {"User-Agent":"Mozilla/5.0"}

def fetch(url, i):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code!=200: return None,None,False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:100]) else "json"
        return c,fmt,True
    except:
        return None,None,False

# XML解析（强制+0800）
def parse_xml(content, i):
    if content.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
            content = f.read()
    root = etree.fromstring(content)
    chs = {}
    progs = []
    for ch in root.xpath("//channel"):
        raw = ch.findtext("display-name", "")
        un = unified_name(raw)
        ch.set("id", un)
        if ch.find("display-name"): ch.find("display-name").text = un
        chs[un] = ch

    temp_map = {}
    for p in root.xpath("//programme"):
        rawid = p.get("channel", "")
        un = unified_name(rawid)
        t = p.find("title")
        title = f2s(t.text) if t else ""
        st_str = p.get("start","")
        try:
            st_time = datetime.strptime(st_str[:14], "%Y%m%d%H%M%S").time()
            temp_map[(un, st_time.strftime("%H:%M"))] = title
        except:
            continue

    for base_day in all_days:
        for (chan, tm), title in temp_map.items():
            try:
                bt = datetime.combine(base_day, datetime.strptime(tm, "%H:%M").time())
                et = bt + timedelta(minutes=30)
                p = etree.Element("programme")
                p.set("start", bt.strftime("%Y%m%d%H%M%S +0800"))
                p.set("stop", et.strftime("%Y%m%d%H%M%S +0800"))
                p.set("channel", chan)
                etree.SubElement(p, "title").text = title
                progs.append(p)
            except:
                continue
    return chs, progs

# JSON解析（强制+0800）
def parse_json(content, i):
    data = json.loads(content)
    chs = {}
    progs = []
    for item in data:
        name = item.get("name", "")
        plist = item.get("list", [])
        un = unified_name(name)
        if un not in chs:
            ch = etree.Element("channel", id=un)
            etree.SubElement(ch, "display-name").text = un
            chs[un] = ch
        for base_day in all_days:
            for prog in plist:
                t = prog.get("time", "")
                title = f2s(prog.get("program", ""))
                try:
                    bt = datetime.combine(base_day, datetime.strptime(t, "%H:%M").time())
                    et = bt + timedelta(minutes=30)
                    p = etree.Element("programme")
                    p.set("start", bt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("stop", et.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("channel", un)
                    etree.SubElement(p, "title").text = title
                    progs.append(p)
                except:
                    continue
    return chs, progs

# 强力去重（只留唯一）
def dedupe(progs):
    seen=set()
    u=[]
    for p in progs:
        key=(p.get("channel"), p.get("start"), p.get("stop"))
        if key not in seen:
            seen.add(key)
            u.append(p)
    return u

def read_config():
    if not os.path.exists(CONFIG_FILE): return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    out_path=os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    if os.path.exists(out_path): os.remove(out_path)

    urls=read_config()
    all_ch={}
    all_prog=[]

    for i,url in enumerate(urls,1):
        c,fmt,ok=fetch(url,i)
        if not ok: continue
        chs,progs=parse_xml(c,i) if fmt=="xml" else parse_json(c,i)
        for cid,ch in chs.items():
            if cid not in all_ch: all_ch[cid]=ch
        all_prog.extend(progs)

    # 兜底
    if "山东体育" not in all_ch:
        ch=etree.Element("channel",id="山东体育")
        etree.SubElement(ch,"display-name").text="山东体育"
        all_ch["山东体育"]=ch
    if "CCTV4" not in all_ch:
        ch=etree.Element("channel",id="CCTV4")
        etree.SubElement(ch,"display-name").text="CCTV4"
        all_ch["CCTV4"]=ch

    all_prog=dedupe(all_prog)

    root=etree.Element("tv")
    for ch in all_ch.values(): root.append(ch)
    for p in all_prog: root.append(p)

    xml=etree.tostring(root,encoding="utf-8",xml_declaration=True)
    with gzip.open(out_path,"wb") as f:
        f.write(xml)
    logging.info(f"✅ 完成：频道{len(all_ch)} 节目{len(all_prog)}")

if __name__=="__main__":
    main()
