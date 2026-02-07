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

# 潍坊四个频道（新增图标链接，酷9直接解析）
WEIFANG_CHANNELS = [
    (
        "潍坊新闻频道", 
        "https://m.tvsou.com/epg/db502561",
        "https://picsum.photos/seed/weifang-news/200/120"  # 新闻频道图标（稳定图床）
    ),
    (
        "潍坊经济生活频道", 
        "https://m.tvsou.com/epg/47a9d24a",
        "https://picsum.photos/seed/weifang-econ/200/120"   # 经济生活频道图标
    ),
    (
        "潍坊科教频道", 
        "https://m.tvsou.com/epg/d131d3d1",
        "https://picsum.photos/seed/weifang-sci/200/120"    # 科教频道图标
    ),
    (
        "潍坊公共频道", 
        "https://m.tvsou.com/epg/c06f0cc0",
        "https://picsum.photos/seed/weifang-public/200/120" # 公共频道图标
    )
]
WEEK_DAY = ["w1", "w2", "w3", "w4", "w5", "w6", "w7"]
MAX_RETRY = 2  # 失败重试次数

# ====================== 潍坊单频道单天抓取（带重试+精准时间） ======================
def crawl_weifang_single(ch_name, base_url, day_str, current_day):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            url = f"{base_url}/{day_str}"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")
            
            program_list = []
            # 提取所有节目时间+名称
            for item in soup.find_all(["div", "li", "p"]):
                txt = item.get_text(strip=True)
                match = re.match(r"(\d{1,2}:\d{2})\s*(.+)", txt)
                if not match:
                    continue
                time_str, title = match.groups()
                if len(title) < 2 or "广告" in title:
                    continue
                # 解析时间
                hh, mm = time_str.split(":")
                prog_time = datetime.combine(current_day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                program_list.append((prog_time, title))
            
            # 计算每个节目的stop时间（用下一个节目的start作为当前的stop）
            precise_programs = []
            for i in range(len(program_list)):
                start_time, title = program_list[i]
                # 最后一个节目的stop默认+60分钟（避免空值）
                if i == len(program_list) - 1:
                    stop_time = start_time + timedelta(minutes=60)
                else:
                    stop_time = program_list[i+1][0]
                # 转成必应格式时间戳
                start = start_time.strftime("%Y%m%d%H%M%S +0800")
                stop = stop_time.strftime("%Y%m%d%H%M%S +0800")
                precise_programs.append((start, stop, title))
            
            time.sleep(0.3)
            return precise_programs  # 成功返回精准节目列表
        except:
            time.sleep(1)
            continue
    return []  # 重试失败返回空

# ====================== 潍坊整体抓取（带重试+精准时间+酷9图标） ======================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, base_url, icon_url in WEIFANG_CHANNELS:  # 新增icon_url参数
            ch = etree.SubElement(root, "channel", id=ch_name)
            # 频道名称（酷9识别用）
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name
            # 新增<icon>标签（酷9自动解析图标）
            icon = etree.SubElement(ch, "icon", src=icon_url)

        today = datetime.now()
        for day_idx in range(7):
            current_day = today + timedelta(days=day_idx)
            day_str = WEEK_DAY[day_idx]
            for ch_name, base_url, _ in WEIFANG_CHANNELS:
                # 调用带重试+精准时间的单频道抓取
                programs = crawl_weifang_single(ch_name, base_url, day_str, current_day)
                # 写入精准节目
                for start, stop, title in programs:
                    prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
                    t = etree.SubElement(prog, "title")
                    t.text = title

        wf_path = os.path.join(OUTPUT_DIR, "weifang.xml")
        with open(wf_path, "wb") as f:
            f.write(etree.tostring(root, encoding="utf-8", pretty_print=True))
        return wf_path
    except:
        # 整体失败返回空文件
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

    # ====================== 潍坊加载：失败仅提示 ======================
    try:
        with open(weifang_file, "r", encoding="utf-8") as f:
            wf_tree = etree.fromstring(f.read().encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}（时间精准匹配+酷9图标）")
            for node in wf_tree:
                if node.tag in ("channel", "programme"):
                    all_channels.append(node) if node.tag == "channel" else all_programs.append(node)
        else:
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except:
        print("⚠️ 潍坊本地源抓取失败，已跳过")

    # 输出最终文件
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
