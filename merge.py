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

# ====================== 核心：抓【本周一 ~ 本周日】7天 ======================
def get_channel_7days(channel_name, base_url):
    week_list = list(WEEK_MAP.items())
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    channel_progs = []

    for i, (week_name, w_suffix) in enumerate(week_list):
        current_date = monday + timedelta(days=i)

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

        # 当天内部去重
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

# ====================== 潍坊7天抓取 ======================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, _, icon_url in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name
            icon = etree.SubElement(ch, "icon", src=icon_url)

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
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
        return wf_path

# ====================== 抓取 + 重试 ======================
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

# ====================== 酷9专用：只对【完全相同的大写名称】去重 ======================
def merge_all(weifang_gz_file):
    existed_channel_upper = set()
    existed_program_keys = set()

    final_channels = []
    final_programs = []

    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    print("=" * 60)
    print("EPG 抓取统计：只对【完全相同名称】去重")
    print("CCTV1 / CCTV1高清 / CCTV-1 / CCTV-1标清 全部保留")
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

                for ch_node in tree.xpath("//channel"):
                    display_name = ""
                    dn_elem = ch_node.find("display-name")
                    if dn_elem is not None and dn_elem.text:
                        display_name = dn_elem.text.strip()

                    if not display_name:
                        continue

                    upper_name = display_name.upper()

                    # 只有【完全一样】才去重
                    if upper_name not in existed_channel_upper:
                        existed_channel_upper.add(upper_name)
                        if dn_elem is not None:
                            dn_elem.text = upper_name
                        final_channels.append(ch_node)

                for prog_node in tree.xpath("//programme"):
                    ch_id = prog_node.get("channel", "")
                    start = prog_node.get("start", "")
                    stop = prog_node.get("stop", "")

                    ch_upper = ""
                    for c in final_channels:
                        dn = c.find("display-name")
                        if dn is not None and c.get("id") == ch_id:
                            ch_upper = dn.text.strip()
                            break
                    if not ch_upper:
                        continue

                    key = (ch_upper, start, stop)
                    if key not in existed_program_keys:
                        existed_program_keys.add(key)
                        final_programs.append(prog_node)
            else:
                fail_cnt += 1

    if fail_cnt > 0:
        print(f"❌ 共 {fail_cnt} 个源失败，已跳过")

    print("=" * 60)
    print(f"去重前：频道 {total_ch}  节目 {total_pg}")
    print(f"去重后：频道 {len(final_channels)}  节目 {len(final_programs)}")
    print("（仅完全同名才合并，高清标清全部保留）")
    print("=" * 60)

    # ------------- 潍坊本地源优先 -------------
    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))

        wf_channels = wf_tree.xpath("//channel")
        wf_progs = wf_tree.xpath("//programme")

        if wf_channels and wf_progs:
            print("📺 潍坊本地4个频道（优先保留）")
            for wf_ch in wf_channels:
                wf_dn = wf_ch.find("display-name")
                wf_name = wf_dn.text.strip() if (wf_dn is not None and wf_dn.text) else ""
                if not wf_name:
                    continue

                wf_upper = wf_name.upper()
                if wf_dn is not None:
                    wf_dn.text = wf_upper

                for idx, exist_ch in enumerate(final_channels):
                    exist_dn = exist_ch.find("display-name")
                    exist_upper = exist_dn.text.strip() if (exist_dn is not None and exist_dn.text) else ""
                    if exist_upper == wf_upper:
                        final_channels.pop(idx)
                        existed_channel_upper.discard(exist_upper)
                        break

                if wf_upper not in existed_channel_upper:
                    existed_channel_upper.add(wf_upper)
                    final_channels.append(wf_ch)

            for wf_prog in wf_progs:
                ch_id = wf_prog.get("channel", "")
                start = wf_prog.get("start", "")
                stop = wf_prog.get("stop", "")

                ch_upper = ""
                for c in final_channels:
                    dn = c.find("display-name")
                    if dn is not None and c.get("id") == ch_id:
                        ch_upper = dn.text.strip()
                        break
                if not ch_upper:
                    continue

                key = (ch_upper, start, stop)
                if key not in existed_program_keys:
                    existed_program_keys.add(key)
                    final_programs.append(wf_prog)
    except:
        print("⚠️ 潍坊本地源读取失败")

    final_root = etree.Element("tv")
    for ch in final_channels:
        final_root.append(ch)
    for p in final_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str)

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_gz = crawl_weifang()
        merge_all(wf_gz)
        print("\n🎉 生成完成：output/epg.gz（高清标清全保留）")
    except Exception as e:
        print("\n❌ 执行出错：", e)
