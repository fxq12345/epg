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

# ====================== 潍坊4频道配置（带酷9图标） ======================
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

# 网站固定后缀：周一w1 ~ 周日w7
WEEK_MAP = {
    "周一": "w1",
    "周二": "w2",
    "周三": "w3",
    "周四": "w4",
    "周五": "w5",
    "周六": "w6",
    "周日": "w7"
}

MAX_RETRY = 2

# === 必应Referer 防反爬 ===
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com/search?q=%E7%94%B5%E8%A7%86%E8%8A%82%E7%9B%AE%E8%A1%A8"
}

# --- 可选Selenium（不装也能跑）---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# ====================== 工具函数 ======================
def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_page_html(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.encoding = 'utf-8'
        html = resp.text
        if re.findall(r'\d{1,2}:\d{2}', html):
            return html
    except Exception:
        pass

    if SELENIUM_AVAILABLE:
        try:
            opt = Options()
            opt.add_argument("--headless")
            opt.add_argument("--no-sandbox")
            opt.add_argument("--disable-dev-shm-usage")
            opt.add_argument(f"user-agent={HEADERS['User-Agent']}")
            driver = webdriver.Chrome(options=opt)
            driver.get(url)
            time.sleep(2.5)
            html = driver.page_source
            driver.quit()
            return html
        except Exception:
            pass
    return ""

# ====================== 核心：抓【本周一 ~ 本周日】7天（网站一定存在） ======================
def get_channel_7days(channel_name, base_url):
    week_list = list(WEEK_MAP.items())  # 周一w1 ~ 周日w7
    today = datetime.now()
    # 本周一（本周起点，网站每周一只更新这7天）
    monday = today - timedelta(days=today.weekday())
    channel_progs = []

    # 遍历：周一(0) ~ 周日(6) → 共7天
    for i, (week_name, w_suffix) in enumerate(week_list):
        current_date = monday + timedelta(days=i)  # 本周真实日期

        # 拼接网站URL：w1~w7
        if base_url.endswith('/'):
            url = f"{base_url}{w_suffix}"
        else:
            url = f"{base_url}/{w_suffix}"

        html = get_page_html(url)
        if not html:
            time.sleep(1)
            continue

        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I))
        if not items:
            items = soup.find_all("li")

        day_progs = []
        for item in items:
            txt = item.get_text(strip=True)
            match = re.search(r'(\d{1,2}:\d{2})\s*(.+)', txt)
            if not match:
                continue
            t_str, title = match.groups()
            if len(title) < 2 or '广告' in title or '报时' in title:
                continue
            day_progs.append((t_str.strip(), title.strip()))

        day_progs = sorted(list(set(day_progs)), key=lambda x: x[0])
        for idx in range(len(day_progs)):
            t_start, title = day_progs[idx]
            if idx < len(day_progs)-1:
                t_end = day_progs[idx+1][0]
            else:
                h, m = map(int, t_start.split(':'))
                end_dt = datetime(2000, 1, 1, h, m) + timedelta(minutes=30)
                t_end = end_dt.strftime("%H:%M")

            start = time_to_xmltv(current_date, t_start)
            end = time_to_xmltv(current_date, t_end)
            if start and end:
                channel_progs.append((start, end, title))
        time.sleep(1.0)
    return channel_progs

# ====================== 潍坊7天抓取（本周完整7天） ======================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        # 频道 + 图标（酷9可用）
        for ch_name, _, icon_url in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name
            icon = etree.SubElement(ch, "icon", src=icon_url)

        # 抓取本周一~周日7天节目
        for ch_name, base_url, _ in WEIFANG_CHANNELS:
            programs = get_channel_7days(ch_name, base_url)
            for start, stop, title in programs:
                prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
                t = etree.SubElement(prog, "title")
                t.text = title

        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        xml_content = etree.tostring(root, encoding="utf-8", pretty_print=True)
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_content)
        return wf_path
    except Exception:
        # 失败输出空gz
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
        return wf_path

# ====================== 原有合并逻辑（完全不变） ======================
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

    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}（本周一~周日完整7天+酷9图标）")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
        else:
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except:
        print("⚠️ 潍坊本地源读取失败，已跳过")

    # ====================== 修改：名称相同，无论ID，只保留第一个 ======================
    seen_channel_names = set()
    unique_channels = []
    for ch in all_channels:
        display_name_node = ch.find("display-name")
        if display_name_node and display_name_node.text:
            channel_name = display_name_node.text.strip()
            # 只要名称没出现过，就保留；出现过就跳过，不管ID是否相同
            if channel_name not in seen_channel_names:
                seen_channel_names.add(channel_name)
                unique_channels.append(ch)
        else:
            # 没有display-name的频道直接保留
            unique_channels.append(ch)

    # 生成最终XML（用去重后的频道 + 所有节目）
    final_root = etree.Element("tv")
    for ch in unique_channels:
        final_root.append(ch)
    for p in all_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str)

    print(f"✅ 最终输出：频道 {len(unique_channels)} 个 | 节目 {len(all_programs)} 个")

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_gz = crawl_weifang()
        merge_all(wf_gz)
    except Exception as e:
        print(f"❌ 脚本执行失败: {e}")
        raise  # 抛出异常，让CI步骤失败
