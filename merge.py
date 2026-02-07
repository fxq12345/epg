import os
import gzip
import re
import time
import signal
import requests
from lxml import etree
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 10分钟强制杀死
signal.signal(signal.SIGALRM, lambda s,f: os._exit(0))
signal.alarm(600)

# 配置
OUTPUT = "output"
os.makedirs(OUTPUT, exist_ok=True)

# 潍坊频道（和你电视完全一致）
WF = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]
WEEK = {"周一":"w1","周二":"w2","周三":"w3","周四":"w4","周五":"w5","周六":"w6","周日":"w7"}

# 抓取潍坊
def gen_wf():
    try:
        root = ET.Element("tv")
        for n, _ in WF:
            c = ET.SubElement(root, "channel", id=n)
            ET.SubElement(c, "display-name").text = n
        mon = datetime.now() - timedelta(days=datetime.now().weekday())
        for idx, (name, u) in enumerate(WF):
            for d, (wn, ws) in enumerate(WEEK.items()):
                day = mon + timedelta(days=d)
                try:
                    r = requests.get(f"{u}/{ws}", headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
                    r.encoding = "utf-8"
                    soup = BeautifulSoup(r.text, "html.parser")
                    for i in soup.find_all(["div","li"]):
                        m = re.search(r"(\d+:\d+)\s*(.+)", i.get_text(strip=True))
                        if m:
                            t, title = m.groups()
                            if len(title) < 2 or "广告" in title:
                                continue
                            hh, mm = t.split(":")
                            dt = datetime.combine(day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                            ts = dt.strftime("%Y%m%d%H%M%S +0800")
                            p = ET.SubElement(root, "programme", start=ts, stop=ts, channel=name)
                            ET.SubElement(p, "title").text = title
                    time.sleep(0.5)
                except:
                    continue
        with open(os.path.join(OUTPUT, "weifang.xml"), "wb") as f:
            f.write(ET.tostring(root, encoding="utf-8"))
    except:
        with open(os.path.join(OUTPUT, "weifang.xml"), "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0"?><tv></tv>')

# 合并网络源+潍坊
def merge():
    all_channels = []
    all_programs = []
    # 加载网络源
    if os.path.exists("config.txt"):
        with open("config.txt","r",encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip() and l.startswith("http")]
        def get_xml(u):
            try:
                r = requests.get(u, timeout=10)
                c = gzip.decompress(r.content).decode() if u.endswith(".gz") else r.text
                c = re.sub(r"[\x00-\x1F]","",c).replace("& ","&amp; ")
                return etree.fromstring(c.encode())
            except:
                return None
        with ThreadPoolExecutor(5) as e:
            res = [e.submit(get_xml, u) for u in urls]
            for r in res:
                x = r.result()
                if x is not None:
                    for ch in x.xpath("//channel"):
                        all_channels.append(ch)
                    for p in x.xpath("//programme"):
                        all_programs.append(p)
    # 加载潍坊
    try:
        with open(os.path.join(OUTPUT,"weifang.xml"),"r",encoding="utf-8") as f:
            x = etree.fromstring(f.read().encode())
            for ch in x.xpath("//channel"):
                all_channels.append(ch)
            for p in x.xpath("//programme"):
                all_programs.append(p)
    except:
        pass
    # 输出总文件
    root = etree.Element("tv")
    for ch in all_channels:
        root.append(ch)
    for p in all_programs:
        root.append(p)
    xml = etree.tostring(root, encoding="utf-8", pretty_print=True).decode()
    with open(os.path.join(OUTPUT,"epg.xml"),"w",encoding="utf-8") as f:
        f.write(xml)
    with gzip.open(os.path.join(OUTPUT,"epg.gz"),"wb") as f:
        f.write(xml.encode())

if __name__ == "__main__":
    try:
        gen_wf()
    except:
        pass
    try:
        merge()
    except:
        pass
