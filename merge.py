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

# ====================== 潍坊4频道配置 ======================
WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_MAP = {
    "周一": "w1", "周二": "w2", "周三": "w3", "周四": "w4",
    "周五": "w5", "周六": "w6", "周日": "w7"
}

MAX_RETRY = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com"
}

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
        if re.findall(r'\d{1,2}:\d{2}', resp.text):
            return resp.text
    except:
        pass
    return ""

# ====================== 抓取潍坊7天 ======================
def get_channel_7days(channel_name, base_url):
    week_list = list(WEEK_MAP.items())
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    channel_progs = []

    for i, (week_name, w_suffix) in enumerate(week_list):
        current_date = monday + timedelta(days=i)
        url = f"{base_url}/{w_suffix}" if not base_url.endswith('/') else f"{base_url}{w_suffix}"
        html = get_page_html(url)
        if not html:
            time.sleep(1)
            continue

        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I)) or soup.find_all("li")
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
                t_end = (datetime(2000,1,1,h,m)+timedelta(minutes=30)).strftime("%H:%M")

            start = time_to_xmltv(current_date, t_start)
            end = time_to_xmltv(current_date, t_end)
            if start and end:
                channel_progs.append((start, end, title))
        time.sleep(1)
    return channel_progs

def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, _ in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name", lang="zh")
            dn.text = ch_name

        for ch_name, base_url in WEIFANG_CHANNELS:
            progs = get_channel_7days(ch_name, base_url)
            for s, e, t in progs:
                p = etree.SubElement(root, "programme", start=s, stop=e, channel=ch_name)
                te = etree.SubElement(p, "title", lang="zh")
                te.text = t

        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        xml_content = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_content)
        print(f"✅ 潍坊EPG已保存")
        return wf_path
    except Exception as e:
        print(f"❌ 潍坊抓取失败: {e}")
        empty = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        p = os.path.join(OUTPUT_DIR, "weifang.gz")
        with gzip.open(p, "wb") as f:
            f.write(empty)
        return p

# ====================== 抓取上游源 ======================
def fetch_with_retry(u):
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code not in (200, 206):
                time.sleep(1)
                continue
            c = gzip.decompress(r.content).decode("utf-8","ignore") if u.endswith(".gz") else r.text
            c = re.sub(r'[\x00-\x1f]', '', c).replace("& ", "&amp; ")
            tree = etree.fromstring(c.encode("utf-8"))
            ch = len(tree.xpath("//channel"))
            pg = len(tree.xpath("//programme"))
            if ch>0 and pg>0:
                return True, tree
        except:
            time.sleep(1)
    return False, None

def merge_all(weifang_gz):
    if not os.path.exists("config.txt"):
        print("❌ 无config.txt")
        return

    urls = [l.strip() for l in open("config.txt","utf-8") if l.strip().startswith("http")]
    if not urls:
        print("❌ 无有效URL")
        return

    print("开始抓取所有源...")
    all_trees = []
    with ThreadPoolExecutor(max_workers=6) as exec:
        res = {exec.submit(fetch_with_retry, u):u for u in urls}
        for f in res:
            ok, t = f.result()
            if ok:
                all_trees.append(t)

    # 读取潍坊
    try:
        with gzip.open(weifang_gz,"rb") as f:
            wf_tree = etree.fromstring(f.read())
            all_trees.append(wf_tree)
    except:
        pass

    # ====================== 超级轻量去重（兼容所有播放器） ======================
    final = etree.Element("tv")
    seen_channel_id = set()
    seen_program_key = set()

    for tree in all_trees:
        for node in tree:
            if node.tag == "channel":
                cid = node.get("id","")
                if cid and cid not in seen_channel_id:
                    seen_channel_id.add(cid)
                    final.append(node)

            elif node.tag == "programme":
                c = node.get("channel","")
                s = node.get("start","")
                e = node.get("stop","")
                key = (c, s, e)
                if c and s and e and key not in seen_program_key:
                    seen_program_key.add(key)
                    final.append(node)

    # 输出
    out = os.path.join(OUTPUT_DIR, "epg.gz")
    xml = etree.tostring(final, encoding="utf-8", xml_declaration=True)
    with gzip.open(out,"wb") as f:
        f.write(xml)

    size_mb = os.path.getsize(out)/1024/1024
    print(f"✅ 合并完成！文件大小：{size_mb:.2f}MB")
    print(f"✅ 频道：{len(seen_channel_id)}  节目：{len(seen_program_key)}")
    print("📁 输出：" + out)

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf = crawl_weifang()
        merge_all(wf)
        print("🎉 全部完成，所有播放器通用！")
    except Exception as e:
        print(f"❌ 失败：{e}")
