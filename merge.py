import os
import gzip
import re
import time
import signal
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests
from lxml import etree
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom

# 10分钟强制退出
signal.signal(signal.SIGALRM, lambda s,f:os._exit(0))
signal.alarm(600)

# 配置
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
LOCAL_WEIFANG = os.path.join(OUTPUT_DIR, "weifang.xml")

# 潍坊频道（与电视完全一致）
WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_MAP = {"周一":"w1","周二":"w2","周三":"w3","周四":"w4","周五":"w5","周六":"w6","周日":"w7"}

# 日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE,encoding='utf-8'),logging.StreamHandler()])

# 潍坊抓取
def time_to_xmltv(d, t):
    try:
        hh,mm = t.split(':')
        return datetime.combine(d, datetime.min.time().replace(hour=int(hh),minute=int(mm))).strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_html(url):
    try:
        r = requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=10)
        r.encoding='utf-8'
        return r.text if "节目" in r.text else ""
    except:
        return ""

def crawl_one_day(url):
    html = get_html(url)
    res = []
    try:
        soup = BeautifulSoup(html,"html.parser")
        for i in soup.find_all(["div","li"]):
            m = re.search(r'(\d+:\d+)\s*(.+)',i.get_text(strip=True))
            if m:
                t,title = m.groups()
                if len(title)>1 and "广告" not in title:
                    res.append((t,title))
    except:
        pass
    return sorted(list(set(res)),key=lambda x:x[0])

def make_weifang_xml():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        root = ET.Element("tv")
        for name,_ in WEIFANG_CHANNELS:
            ch = ET.SubElement(root,"channel",id=name)
            ET.SubElement(ch,"display-name",lang="zh").text=name
        mon = datetime.now() - timedelta(days=datetime.now().weekday())
        for idx,(name,base) in enumerate(WEIFANG_CHANNELS):
            for d,(wn,ws) in enumerate(WEEK_MAP.items()):
                day = mon + timedelta(days=d)
                progs = crawl_one_day(f"{base}/{ws}")
                for i,(t,title) in enumerate(progs):
                    st = time_to_xmltv(day,t)
                    et = time_to_xmltv(day,progs[i+1][0] if i<len(progs)-1 else (datetime.strptime(t,"%H:%M")+timedelta(minutes=30)).strftime("%H:%M"))
                    if st and et:
                        p = ET.SubElement(root,"programme",start=st,stop=et,channel=name)
                        ET.SubElement(p,"title",lang="zh").text=title
                time.sleep(0.5)
        with open(LOCAL_WEIFANG,"wb") as f:
            f.write(minidom.parseString(ET.tostring(root,"utf-8")).toprettyxml(indent="  ",encoding="utf-8"))
    except:
        with open(LOCAL_WEIFANG,"w",encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>')

# 合并网络源
def merge_all():
    channels = set()
    all_ch = []
    all_pg = []
    os.makedirs(OUTPUT_DIR,exist_ok=True)

    # 读取网络源
    urls = []
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE,"r",encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    # 抓取
    def get_url(u):
        try:
            r = requests.get(u,timeout=15)
            if u.endswith(".gz"):
                c = gzip.decompress(r.content).decode("utf-8")
            else:
                c = r.text
            c = re.sub(r'[\x00-\x1F]','',c).replace("& ","&amp; ")
            return etree.fromstring(c.encode("utf-8"))
        except:
            return None

    with ThreadPoolExecutor(5) as e:
        res = [e.submit(get_url,u) for u in urls]
        for f in as_completed(res):
            tree = f.result()
            if tree:
                for ch in tree.xpath("//channel"):
                    cid = ch.get("id")
                    if cid and cid not in channels:
                        channels.add(cid)
                        all_ch.append(ch)
                for p in tree.xpath("//programme"):
                    if p.get("channel") in channels:
                        all_pg.append(p)

    # 加入潍坊
    try:
        if os.path.exists(LOCAL_WEIFANG):
            with open(LOCAL_WEIFANG,"r",encoding="utf-8") as f:
                t = etree.fromstring(f.read().encode("utf-8"))
                for ch in t.xpath("//channel"):
                    cid = ch.get("id")
                    if cid and cid not in channels:
                        channels.add(cid)
                        all_ch.append(ch)
                for p in t.xpath("//programme"):
                    if p.get("channel") in channels:
                        all_pg.append(p)
    except:
        pass

    # 输出
    root = etree.fromstring(b'<?xml version="1.0" encoding="UTF-8"?><tv/>')
    for ch in all_ch: root.append(ch)
    for p in all_pg: root.append(p)
    xml = etree.tostring(root,encoding="utf-8",pretty_print=True).decode("utf-8")
    with open(os.path.join(OUTPUT_DIR,"epg.xml"),"w",encoding="utf-8") as f:
        f.write(xml)
    with gzip.open(os.path.join(OUTPUT_DIR,"epg.gz"),"wb") as f:
        f.write(xml.encode("utf-8"))

# 主程序
if __name__ == "__main__":
    try:
        make_weifang_xml()
    except:
        pass
    try:
        merge_all()
    except:
        pass
