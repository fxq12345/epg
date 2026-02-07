import os
import gzip
import re
import time
import signal
import requests
from lxml import etree
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 10分钟强制终止
signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm(600)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 潍坊四个频道（和你电视完全一致）
WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_DAY = ["w1","w2","w3","w4","w5","w6","w7"]

# ======================================
# 抓取潍坊节目，生成 weifang.xml
# ======================================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        # 先写频道信息
        for ch_name, _ in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name

        # 抓取一周节目
        today = datetime.now()
        for day_idx in range(7):
            current_day = today + timedelta(days=day_idx)
            day_str = WEEK_DAY[day_idx]
            for ch_name, base_url in WEIFANG_CHANNELS:
                try:
                    url = f"{base_url}/{day_str}"
                    resp = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
                    resp.encoding = "utf-8"
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for item in soup.find_all(["div", "li", "p"]):
                        txt = item.get_text(strip=True)
                        match = re.match(r"(\d{1,2}:\d{2})\s*(.+)", txt)
                        if not match:
                            continue
                        time_str, title = match.groups()
                        if len(title) < 2 or "广告" in title:
                            continue
                        # 时间格式化
                        try:
                            hh, mm = time_str.split(":")
                            dt = datetime.combine(current_day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                            start = dt.strftime("%Y%m%d%H%M%S +0800")
                            stop = (dt + timedelta(minutes=30)).strftime("%Y%m%d%H%M%S +0800")
                            prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
                            t = etree.SubElement(prog, "title")
                            t.text = title
                        except:
                            continue
                    time.sleep(0.4)
                except:
                    continue

        # 保存潍坊独立文件
        wf_path = os.path.join(OUTPUT_DIR, "weifang.xml")
        with open(wf_path, "wb") as f:
            f.write(etree.tostring(root, encoding="utf-8", pretty_print=True))
        return wf_path
    except:
        wf_path = os.path.join(OUTPUT_DIR, "weifang.xml")
        with open(wf_path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>')
        return wf_path

# ======================================
# 合并：网络源 + 潍坊 → 输出 epg.xml / epg.gz
# ======================================
def merge_all(weifang_file):
    all_channels = []
    all_programs = []

    # 1. 加载网络源
    if os.path.exists("config.txt"):
        with open("config.txt", "r", encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip() and l.startswith("http")]
        def fetch_url(u):
            try:
                r = requests.get(u, timeout=10)
                if u.endswith(".gz"):
                    content = gzip.decompress(r.content).decode("utf-8", "ignore")
                else:
                    content = r.text
                content = re.sub(r"[\x00-\x1F]", "", content).replace("& ", "&amp; ")
                return etree.fromstring(content.encode("utf-8"))
            except:
                return None
        with ThreadPoolExecutor(5) as executor:
            results = [executor.submit(fetch_url, u) for u in urls]
            for res in results:
                tree = res.result()
                if tree is not None:
                    for node in tree:
                        if node.tag == "channel":
                            all_channels.append(node)
                        elif node.tag == "programme":
                            all_programs.append(node)

    # 2. 加载潍坊（强制加入，必合并）
    try:
        with open(weifang_file, "r", encoding="utf-8") as f:
            wf_tree = etree.fromstring(f.read().encode("utf-8"))
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
    except:
        pass

    # 3. 写入总文件
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for pg in all_programs:
        final_root.append(pg)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True).decode("utf-8")
    # 保存主文件
    with open(os.path.join(OUTPUT_DIR, "epg.xml"), "w", encoding="utf-8") as f:
        f.write(xml_str)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str.encode("utf-8"))

# ======================================
# 主入口
# ======================================
if __name__ == "__main__":
    try:
        wf_file = crawl_weifang()
        merge_all(wf_file)
    except:
        pass
