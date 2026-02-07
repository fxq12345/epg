import os
import signal
os.environ['PYTHONUNBUFFERED'] = '1'

# 10分钟强制退出
signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm(600)

try:
    import sys
    import gzip
    import re
    import time
    import requests
    from lxml import etree
    from bs4 import BeautifulSoup
    from datetime import datetime, timedelta
except:
    sys.exit()

OUTPUT = "output"
os.makedirs(OUTPUT, exist_ok=True)

# 潍坊频道
WF_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

def make_weifang():
    try:
        root = etree.Element("tv")
        for name, _ in WF_CHANNELS:
            ch = etree.SubElement(root, "channel", id=name)
            etree.SubElement(ch, "display-name").text = name

        monday = datetime.now() - timedelta(days=datetime.now().weekday())
        for idx, (name, base_url) in enumerate(WF_CHANNELS):
            for i in range(7):
                day = monday + timedelta(days=i)
                try:
                    url = f"{base_url}/w{i+1}"
                    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                    r.encoding = "utf-8"
                    soup = BeautifulSoup(r.text, "html.parser")
                    for item in soup.find_all(["div", "li"]):
                        txt = item.get_text(strip=True)
                        m = re.match(r"(\d+:\d+)\s+(.+)", txt)
                        if m:
                            t, title = m.groups()
                            if len(title) < 2 or "广告" in title:
                                continue
                            try:
                                hh, mm = t.split(":")
                                dt = datetime.combine(day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                                ts = dt.strftime("%Y%m%d%H%M%S +0800")
                                p = etree.SubElement(root, "programme", start=ts, stop=ts, channel=name)
                                etree.SubElement(p, "title").text = title
                            except:
                                continue
                    time.sleep(0.3)
                except:
                    continue
        with open(os.path.join(OUTPUT, "weifang.xml"), "wb") as f:
            f.write(etree.tostring(root, encoding="utf-8"))
    except:
        with open(os.path.join(OUTPUT, "weifang.xml"), "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0"?><tv></tv>')

def merge_all():
    try:
        all_xml = []
        if os.path.exists("config.txt"):
            with open("config.txt", "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f if l.strip().startswith("http")]
            for u in lines:
                try:
                    r = requests.get(u, timeout=8)
                    if u.endswith(".gz"):
                        c = gzip.decompress(r.content).decode("utf-8", "ignore")
                    else:
                        c = r.text
                    c = re.sub(r"[\x00-\x1F]", "", c)
                    all_xml.append(etree.fromstring(c.encode("utf-8")))
                except:
                    continue
        # 加入潍坊
        try:
            with open(os.path.join(OUTPUT, "weifang.xml"), "r", encoding="utf-8") as f:
                all_xml.append(etree.fromstring(f.read().encode("utf-8")))
        except:
            pass

        root = etree.Element("tv")
        for x in all_xml:
            for node in x:
                root.append(node)

        final = etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")
        with open(os.path.join(OUTPUT, "epg.xml"), "w", encoding="utf-8") as f:
            f.write(final)
        with gzip.open(os.path.join(OUTPUT, "epg.gz"), "wb") as f:
            f.write(final.encode("utf-8"))
    except:
        return

if __name__ == "__main__":
    try:
        make_weifang()
    except:
        pass
    try:
        merge_all()
    except:
        pass
