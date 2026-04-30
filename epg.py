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

# 【极致详细日志】
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 繁简统一
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a,b)
    return text

# 统一频道名称（酷九名称匹配专用）
def unified_name(raw_name):
    n = f2s(raw_name).strip()
    if "CCTV4" in n or "央视4" in n or "国际" in n:
        return "CCTV4"
    if "CCTV5" in n or "央视5" in n and "5+" not in n:
        return "CCTV5"
    if "CCTV5+" in n or "5+体育" in n:
        return "CCTV5+"
    if "浙江卫视" in n:
        return "浙江卫视"
    if "山东体育" in n:
        return "山东体育"
    if "山东卫视" in n:
        return "山东卫视"
    if "齐鲁" in n:
        return "山东齐鲁"
    return n

# 时间区间
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start = today - timedelta(days=DAYS_BEFORE)
end = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent":"Mozilla/5.0"}

def fetch(url,i):
    logging.info(f"【第{i}条】开始抓取源地址: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logging.error(f"【第{i}条】抓取失败，状态码: {r.status_code}")
            return None, None, False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:100]) else "json"
        logging.info(f"【第{i}条】抓取成功，格式: {fmt}")
        return c, fmt, True
    except Exception as e:
        logging.error(f"【第{i}条】抓取异常: {str(e)}")
        return None, None, False

def parse_time(s):
    try:
        return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
    except:
        return None

def parse_xml(content,i):
    if content.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
            content = f.read()
    root = etree.fromstring(content)
    chs = {}
    progs = []
    sd_sport_found = False

    for ch in root.xpath("//channel"):
        raw_name = ch.findtext("display-name","")
        u_name = unified_name(raw_name)
        ch.set("id", u_name)
        if ch.find("display-name") is not None:
            ch.find("display-name").text = u_name
        chs[u_name] = ch

        if u_name == "山东体育":
            sd_sport_found = True
            logging.info(f"【第{i}条】✅ 识别到山东体育原始频道名: {raw_name} -> 统一为: 山东体育")

    for p in root.xpath("//programme"):
        st = parse_time(p.get("start",""))
        if not st or not (start <= st <= end): continue
        raw_cid = p.get("channel","")
        u_name = unified_name(raw_cid)
        p.set("channel", u_name)
        t = p.find("title")
        if t is not None:
            t.text = f2s(t.text)
        progs.append(p)

    logging.info(f"【第{i}条】本条解析完成 | 频道数量: {len(chs)} | 节目数量: {len(progs)}")
    if not sd_sport_found:
        logging.warning(f"【第{i}条】⚠️ 本条源【未找到任何山东体育】频道")
    return chs, progs

def parse_json(content,i):
    data = json.loads(content)
    chs = {}
    progs = []
    sd_sport_found = False

    for item in data:
        name = item.get("name","")
        plist = item.get("list",[])
        u_name = unified_name(name)

        if u_name not in chs:
            ch = etree.Element("channel", id=u_name)
            etree.SubElement(ch, "display-name").text = u_name
            chs[u_name] = ch

        if u_name == "山东体育":
            sd_sport_found = True
            logging.info(f"【第{i}条】✅ 识别到山东体育原始频道名: {name} -> 统一为: 山东体育")

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
                p.set("channel", u_name)
                etree.SubElement(p, "title").text = title
                progs.append(p)
            except:
                pass

    logging.info(f"【第{i}条】本条解析完成 | 频道数量: {len(chs)} | 节目数量: {len(progs)}")
    if not sd_sport_found:
        logging.warning(f"【第{i}条】⚠️ 本条源【未找到任何山东体育】频道")
    return chs, progs

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.error("未找到 config.txt")
        return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    logging.info(f"总共读取到 {len(lines)} 条EPG源")
    return lines

def main():
    urls = read_config()
    all_ch = {}
    all_prog = []

    for i,url in enumerate(urls,1):
        c,fmt,ok = fetch(url,i)
        if not ok:
            continue
        chs,progs = parse_xml(c,i) if fmt=="xml" else parse_json(c,i)
        
        # 全部频道全部保留，不覆盖、不丢失
        for cid,ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
                logging.info(f"全局新增频道: {cid}")
        all_prog.extend(progs)

    # 全局汇总统计
    logging.info("==================== 汇总统计 ====================")
    logging.info(f"最终总频道数: {len(all_ch)}")
    logging.info(f"最终总节目数: {len(all_prog)}")
    if "山东体育" in all_ch:
        logging.info("✅ 全局存在【山东体育】频道")
    else:
        logging.error("❌ 全局【完全没有山东体育】= 所有源里面一条都没有")

    if all_ch and all_prog:
        root = etree.Element("tv")
        for ch in all_ch.values(): root.append(ch)
        for p in all_prog: root.append(p)
        xml = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out, "wb") as f:
            f.write(xml)
        logging.info(f"✅ EPG生成完毕: {out}")
    else:
        logging.warning("⚠️ 无有效数据生成")

if __name__=="__main__":
    main()
