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

# 潍坊四个频道（图标不变）
WEIFANG_CHANNELS = [
    (
        "潍坊新闻频道", 
        "https://m.tvsou.com/epg/db502561",
        "https://picsum.photos/seed/weifang-news/200/120"
    ),
    (
        "潍坊经济生活频道", 
        "https://m.tvsou.com/epg/47a9d24a",
        "https://picsum.photos/seed/weifang-econ/200/120"
    ),
    (
        "潍坊科教频道", 
        "https://m.tvsou.com/epg/d131d3d1",
        "https://picsum.photos/seed/weifang-sci/200/120"
    ),
    (
        "潍坊公共频道", 
        "https://m.tvsou.com/epg/c06f0cc0",
        "https://picsum.photos/seed/weifang-public/200/120"
    )
]

# ====================== 核心修复：网站真实 7 天后缀 ======================
WEEK_DAY = ["w0", "w1", "w2", "w3", "w4", "w5", "w6"]
MAX_RETRY = 2

# ====================== 潍坊单频道单天抓取 ======================
def crawl_weifang_single(ch_name, base_url, day_str, current_day):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            url = f"{base_url}/{day_str}"
            # 加请求头防屏蔽
            resp = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36",
                "Referer": "https://www.bing.com/"
            }, timeout=8)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")
            
            program_list = []
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
                    prog_time = datetime.combine(current_day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                    program_list.append((prog_time, title))
                except:
                    continue

            # 按时间排序（关键修复）
            program_list = sorted(program_list, key=lambda x: x[0])
            
            precise_programs = []
            for i in range(len(program_list)):
                start_time, title = program_list[i]
                if i == len(program_list) - 1:
                    stop_time = start_time + timedelta(minutes=60)
                else:
                    stop_time = program_list[i+1][0]
                start = start_time.strftime("%Y%m%d%H%M%S +0800")
                stop = stop_time.strftime("%Y%m%d%H%M%S +0800")
                precise_programs.append((start, stop, title))
            
            time.sleep(0.3)
            return precise_programs
        except:
            time.sleep(1)
            continue
    return []

# ====================== 潍坊抓取：7 天 weifang.gz ======================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, base_url, icon_url in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name
            icon = etree.SubElement(ch, "icon", src=icon_url)

        # 今天起连续 7 天（0~6）
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        for day_idx in range(7):
            current_day = today + timedelta(days=day_idx)
            day_str = WEEK_DAY[day_idx]
            for ch_name, base_url, _ in WEIFANG_CHANNELS:
                programs = crawl_weifang_single(ch_name, base_url, day_str, current_day)
                for start, stop, title in programs:
                    prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
                    t = etree.SubElement(prog, "title")
                    t.text = title

        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        xml_content = etree.tostring(root, encoding="utf-8", pretty_print=True)
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_content)
        return wf_path
    except:
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
        return wf_path

# ====================== 单源抓取重试 ======================
def fetch_with_retry(u, max_retry=MAX_RETRY):
    for attempt in range(1, max_retry + 1):
        try:
            r = requests.get(u, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36"
            })
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

# ====================== 合并输出 epg.gz ======================
def merge_all(weifang_gz_file):
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
    print("EPG 抓取合并（7天完整版）")
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
        print(f"❌ 共 {fail_cnt} 个源失败，已跳过")

    print("=" * 60)
    print(f"汇总：成功 {success_cnt} | 失败 {fail_cnt} | 总频道 {total_ch} | 总节目 {total_pg}")
    print("=" * 60)

    # 合并潍坊7天数据
    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源(7天)：频道 {wf_ch} | 节目 {wf_pg}")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
        else:
            print("⚠️ 潍坊源无数据，已跳过")
    except:
        print("⚠️ 潍坊源读取失败")

    # 最终输出
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for p in all_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str)

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_gz = crawl_weifang()
        merge_all(wf_gz)
        print("\n🎉 完成：output/epg.gz（7天数据）")
    except Exception as e:
        pass
