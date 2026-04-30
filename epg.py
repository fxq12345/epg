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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 【核心：酷九按名称识别，所以强制修正display-name】
NAME_FIX = {
    # CCTV三台
    "cctv4": "CCTV4", "央视4": "CCTV4", "央視4": "CCTV4", "国际台": "CCTV4",
    "cctv5": "CCTV5", "央视5": "CCTV5", "央視5": "CCTV5", "体育": "CCTV5",
    "cctv5+": "CCTV5+", "央视5+": "CCTV5+", "央視5+": "CCTV5+", "5+": "CCTV5+",
    # 浙江卫视
    "浙江卫视": "浙江卫视", "ZhejiangTV": "浙江卫视",
    # 山东体育（关键：强制改成酷九识别的"山东体育"）
    "山东体育休闲": "山东体育", "山东体育hd": "山东体育", "山东体育高清": "山东体育",
    # 山东齐鲁/卫视
    "山东齐鲁": "山东齐鲁", "齐鲁频道": "山东齐鲁", "山东卫视": "山东卫视"
}

# 繁简转换
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a,b)
    return text

# 时间区间
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start = today - timedelta(days=DAYS_BEFORE)
end = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent":"Mozilla/5.0"}

def fetch(url,i):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200: return None, None, False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:100]) else "json"
        return c, fmt, True
    except:
        return None, None, False

def parse_time(s):
    try:
        return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
    except:
        return None

def fix_display_name(raw_name):
    raw_name = f2s(raw_name)
    # 匹配修正名称
    for k,v in NAME_FIX.items():
        if k in raw_name:
            return v
    return raw_name

def parse_xml(content,i):
    if content.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
            content = f.read()
    root = etree.fromstring(content)
    chs = {}
    progs = []

    for ch in root.xpath("//channel"):
        raw_id = ch.get("id","")
        raw_name = ch.findtext("display-name","")
        fixed_name = fix_display_name(raw_name)
        
        # 用修正后的名称作为ID，同时强制修改display-name
        ch.set("id", fixed_name)
        if ch.find("display-name") is not None:
            ch.find("display-name").text = fixed_name
        chs[fixed_name] = ch

    for p in root.xpath("//programme"):
        st = parse_time(p.get("start",""))
        if not st or not (start <= st <= end): continue
        raw_id = p.get("channel","")
        # 节目也按修正后的名称匹配
        fixed_id = fix_display_name(raw_id)
        p.set("channel", fixed_id)
        title = p.find("title")
        if title is not None:
            title.text = f2s(title.text)
        progs.append(p)
    return chs, progs

def parse_json(content,i):
    data = json.loads(content)
    chs = {}
    progs = []
    for item in data:
        tvid = item.get("tvid") or item.get("id")
        name = item.get("name","")
        plist = item.get("list",[])
        if not tvid: continue
        
        fixed_name = fix_display_name(name)
        if fixed_name not in chs:
            ch = etree.Element("channel", id=fixed_name)
            etree.SubElement(ch, "display-name").text = fixed_name
            chs[fixed_name] = ch

        for prog in plist:
            t = prog.get("time","")
            title = f2s(prog.get("program",""))
            try:
                bt = datetime.combine(today, datetime.strptime(t,"%H:%M").time())
                while bt < start: bt += timedelta(days=1)
                while bt > end: bt -= timedelta(days=1)
                et = bt + timedelta(minutes=30)
                p = etree.Element("programme")
                p.set("start", bt.strftime("%Y%m%d%H%M%S 0"))
                p.set("stop", et.strftime("%Y%m%d%H%M%S 0"))
                p.set("channel", fixed_name)
                etree.SubElement(p, "title").text = title
                progs.append(p)
            except:
                pass
    return chs, progs

def read_config():
    if not os.path.exists(CONFIG_FILE): return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    urls = read_config()
    all_ch = {}
    all_prog = []

    for i,url in enumerate(urls,1):
        c,fmt,ok = fetch(url,i)
        if not ok: continue
        chs,progs = parse_xml(c,i) if fmt=="xml" else parse_json(c,i)
        
        # 频道：保留第一个（按你排好的源顺序，保住CCTV三台）
        for cid,ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        # 节目：全部合并，覆盖浙江卫视乱档
        all_prog.extend(progs)

    if all_ch and all_prog:
        root = etree.Element("tv")
        for ch in all_ch.values(): root.append(ch)
        for p in all_prog: root.append(p)
        xml = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out, "wb") as f:
            f.write(xml)
        logging.info(f"✅ 生成完成 | 频道:{len(all_ch)} 节目:{len(all_prog)}")
    else:
        logging.warning("⚠️ 无有效数据")

if __name__=="__main__":
    main()
