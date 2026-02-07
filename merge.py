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

# 潍坊四个频道
WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]
WEEK_DAY = ["w1", "w2", "w3", "w4", "w5", "w6", "w7"]

# ======================================
# 抓取潍坊
# ======================================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, _ in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name

        today = datetime.now()
        for day_idx in range(7):
            current_day = today + timedelta(days=day_idx)
            day_str = WEEK_DAY[day_idx]
            for ch_name, base_url in WEIFANG_CHANNELS:
                try:
                    url = f"{base_url}/{day_str}"
                    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
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
# 单源抓取（带统计）
# ======================================
def fetch_one_source(u):
    try:
        r = requests.get(u, timeout=12)
        if u.endswith(".gz"):
            content = gzip.decompress(r.content).decode("utf-8", "ignore")
        else:
            content = r.text
        content = re.sub(r"[\x00-\x1F]", "", content).replace("& ", "&amp; ")
        tree = etree.fromstring(content.encode("utf-8"))

        ch_count = len(tree.xpath("//channel"))
        pg_count = len(tree.xpath("//programme"))
        return (True, tree, ch_count, pg_count)
    except Exception as e:
        return (False, None, 0, 0)

# ======================================
# 合并（带日志统计）
# ======================================
def merge_all(weifang_file):
    all_channels = []
    all_programs = []
    total_ch = 0
    total_pg = 0
    success = 0
    fail = 0

    if os.path.exists("config.txt"):
        with open("config.txt", "r", encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

        print("=" * 60)
        print("开始抓取 EPG 源（每条源统计）")
        print("=" * 60)

        with ThreadPoolExecutor(5) as executor:
            results = [(u, executor.submit(fetch_one_source, u)) for u in urls]
            for u, fut in results:
                ok, tree, ch, pg = fut.result()
                if ok and tree is not None:
                    success += 1
                    total_ch += ch
                    total_pg += pg
                    print(f"✅ {u[:50]}... 成功 | 频道 {ch} | 节目 {pg}")
                    for node in tree:
                        if node.tag == "channel":
                            all_channels.append(node)
                        elif node.tag == "programme":
                            all_programs.append(node)
                else:
                    fail += 1
                    print(f"❌ {u[:50]}... 失败")

        print("=" * 60)
        print(f"汇总：成功 {success} 个 | 失败 {fail} 个 | 总频道 {total_ch} | 总节目 {total_pg}")
        print("=" * 60)

    # 加入潍坊
    try:
        with open(weifang_file, "r", encoding="utf-8") as f:
            wf_tree = etree.fromstring(f.read().encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
    except:
        print("⚠️ 潍坊源加载失败，已跳过")

    # 输出最终文件
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for pg in all_programs:
        final_root.append(pg)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True).decode("utf-8")
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
