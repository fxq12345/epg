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

# 10分钟超时保护
signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm(600)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 潍坊4频道（图标链接固定、适合酷9直接解析）
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

WEEK_DAY = ["w1", "w2", "w3", "w4", "w5", "w6", "w7"]
MAX_RETRY = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ====================== 单频道单日抓取（精准起止时间） ======================
def crawl_weifang_single(ch_name, base_url, day_str, current_day):
    for attempt in range(MAX_RETRY):
        try:
            url = f"{base_url}/{day_str}"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            programs = []
            for item in soup.find_all(text=True):
                txt = item.strip()
                match = re.match(r"(\d{1,2}:\d{2})\s+(.+)", txt)
                if not match:
                    continue
                time_str, title = match.groups()
                if len(title) < 2 or "广告" in title:
                    continue
                try:
                    hh, mm = time_str.split(":")
                    pt = datetime.combine(current_day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                    programs.append((pt, title))
                except:
                    continue

            # 生成起止时间
            out = []
            for i in range(len(programs)):
                s, t = programs[i]
                if i == len(programs) - 1:
                    e = s + timedelta(minutes=60)
                else:
                    e = programs[i+1][0]
                out.append((
                    s.strftime("%Y%m%d%H%M%S +0800"),
                    e.strftime("%Y%m%d%H%M%S +0800"),
                    t
                ))
            time.sleep(0.3)
            return out
        except Exception:
            time.sleep(1)
    return []

# ====================== 潍坊整7天抓取，输出 weifang.gz ======================
def crawl_weifang():
    wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
    try:
        root = etree.Element("tv")
        # 先写频道信息
        for name, _, icon in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = name
            ico = etree.SubElement(ch, "icon", src=icon)

        today = datetime.now()
        for d in range(7):
            day = today + timedelta(days=d)
            wday = WEEK_DAY[d]
            for name, url, _ in WEIFANG_CHANNELS:
                progs = crawl_weifang_single(name, url, wday, day)
                for s, e, t in progs:
                    p = etree.SubElement(root, "programme", start=s, stop=e, channel=name)
                    tit = etree.SubElement(p, "title")
                    tit.text = t

        # 带标准XML头写入gz
        xml_head = b'<?xml version="1.0" encoding="utf-8"?>\n'
        xml_body = etree.tostring(root, encoding="utf-8", pretty_print=True)
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_head + xml_body)
        return wf_path
    except Exception:
        empty = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty)
        return wf_path

# ====================== 远程源抓取（带重试） ======================
def fetch_one(u):
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(u, headers=HEADERS, timeout=12)
            if r.status_code not in (200, 206):
                time.sleep(1)
                continue
            # 解压gz
            if u.endswith(".gz"):
                raw = gzip.decompress(r.content)
            else:
                raw = r.content
            # 清洗非法字符
            txt = raw.decode("utf-8", "ignore")
            txt = re.sub(r"[\x00-\x1F]", "", txt)
            txt = txt.replace("& ", "&amp; ")
            tree = etree.fromstring(txt.encode("utf-8"))
            ch = len(tree.xpath("//channel"))
            pg = len(tree.xpath("//programme"))
            if ch > 0 or pg > 0:
                return (True, tree, ch, pg)
        except Exception:
            time.sleep(1)
    return (False, None, 0, 0)

# ====================== 多源合并 + 并入潍坊 ======================
def merge_all(wf_gz):
    channels = []
    programs = []
    if not os.path.exists("config.txt"):
        print("⚠️ 未找到 config.txt，仅使用潍坊源")
        urls = []
    else:
        with open("config.txt", "r", encoding="utf-8") as f:
            urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    print("=" * 60)
    print("开始抓取远程EPG源")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=6) as pool:
        res = list(pool.map(fetch_one, urls))

    ok_cnt = 0
    total_ch = 0
    total_pg = 0
    for ok, tree, ch, pg in res:
        if ok:
            ok_cnt += 1
            total_ch += ch
            total_pg += pg
            for node in tree:
                if node.tag == "channel":
                    channels.append(node)
                elif node.tag == "programme":
                    programs.append(node)

    fail = len(urls) - ok_cnt
    print(f"远程源：成功 {ok_cnt} 个，失败 {fail} 个 | 频道 {total_ch} 节目 {total_pg}")

    # 合并潍坊源
    try:
        with gzip.open(wf_gz, "rb") as f:
            wf_raw = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_raw.encode("utf-8"))
            wch = len(wf_tree.xpath("//channel"))
            wpg = len(wf_tree.xpath("//programme"))
            if wch > 0 or wpg > 0:
                print(f"潍坊源：频道 {wch} 节目 {wpg}")
                for node in wf_tree:
                    if node.tag == "channel":
                        channels.append(node)
                    elif node.tag == "programme":
                        programs.append(node)
    except Exception:
        print("⚠️ 潍坊源读取失败，已跳过")

    # 最终只输出 epg.gz
    final = etree.Element("tv")
    for c in channels:
        final.append(c)
    for p in programs:
        final.append(p)

    xml_out = b'<?xml version="1.0" encoding="utf-8"?>\n' + etree.tostring(final, encoding="utf-8", pretty_print=True)
    epg_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(epg_path, "wb") as f:
        f.write(xml_out)
    print(f"✅ 合并完成：{epg_path}")

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_gz = crawl_weifang()
        merge_all(wf_gz)
    except Exception as e:
        print(f"❌ 整体异常：{e}")
        # 至少保证输出一个合法空文件
        empty = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
            f.write(empty)
