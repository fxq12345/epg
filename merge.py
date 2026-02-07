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
MAX_RETRY = 2  # 失败重试次数

# ====================== 潍坊抓取（失败仅提示一行，不中断） ======================
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
                    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
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
                    time.sleep(0.3)
                except Exception as e:
                    continue

        wf_path = os.path.join(OUTPUT_DIR, "weifang.xml")
        with open(wf_path, "wb") as f:
            f.write(etree.tostring(root, encoding="utf-8", pretty_print=True))
        return wf_path
    except:
        # 潍坊整体抓取失败，只返回空文件，不抛错
        wf_path = os.path.join(OUTPUT_DIR, "weifang.xml")
        with open(wf_path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>')
        return wf_path

# ====================== 单源抓取 + 失败重试 ======================
def fetch_with_retry(u, max_retry=MAX_RETRY):
    for attempt in range(1, max_retry + 1):
        try:
            r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code not in (200, 206):
                time.sleep(1)
                continue

            if u.endswith(".gz"):
                content = gzip.decompress(r.content).decode("utf-8", "ignore")
            else:
                content = r.text

            content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content).replace("& ", "&amp; ")
            tree = etree.fromstring(content.encode("utf-8"))
            ch = len(tree.xpath("//channel"))
            pg = len(tree.xpath("//programme"))
            if ch > 0 and pg > 0:
                return (True, tree, ch, pg, attempt)
        except:
            time.sleep(1)
            continue
    return (False, None, 0, 0, max_retry)

# ====================== 合并主逻辑 ======================
def merge_all(weifang_file):
    all_channels = []
    all_programs = []
    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    print("=" * 60)
    print("EPG 源抓取统计（失败自动重试）")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=6) as executor:
        future_map = {executor.submit(fetch_with_retry, u): u for u in urls}
        for fut in future_map:
            u = future_map[fut]
            ok, tree, ch, pg, retry_cnt = fut.result()
            if ok:
                success_cnt += 1
                total_ch += ch
                total_pg += pg
                log_retry = f"[重试{retry_cnt-1}次]" if retry_cnt > 1 else ""
                print(f"✅ {u[:55]}... {log_retry}成功 | 频道 {ch:>4} | 节目 {pg:>6}")
                for node in tree:
                    if node.tag == "channel":
                        all_channels.append(node)
                    elif node.tag == "programme":
                        all_programs.append(node)
            else:
                fail_cnt += 1

    if fail_cnt > 0:
        print(f"❌ 共 {fail_cnt} 个源经{MAX_RETRY}次重试后仍失败，已跳过")

    print("=" * 60)
    print(f"汇总：成功 {success_cnt} 个 | 失败 {fail_cnt} 个 | 总频道 {total_ch} | 总节目 {total_pg}")
    print("=" * 60)

    # ====================== 潍坊加载：失败仅提示一行，不中断 ======================
    try:
        with open(weifang_file, "r", encoding="utf-8") as f:
            wf_tree = etree.fromstring(f.read().encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        # 只有有数据才显示成功
        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}")
            for node in wf_tree:
                if node.tag in ("channel", "programme"):
                    all_channels.append(node) if node.tag == "channel" else all_programs.append(node)
        else:
            # 潍坊无数据，仅精简提示
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except:
        # 潍坊读取异常，仅精简提示
        print("⚠️ 潍坊本地源抓取失败，已跳过")

    # 输出最终合并文件
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for p in all_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True).decode("utf-8")
    with open(os.path.join(OUTPUT_DIR, "epg.xml"), "w", encoding="utf-8") as f:
        f.write(xml_str)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str.encode("utf-8"))

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_file = crawl_weifang()
        merge_all(wf_file)
    except:
        pass
