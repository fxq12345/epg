import gzip
import requests
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# 关键：ID 直接用 CCTV1、CCTV2 格式
CCTV_LIST = [
    ("CCTV1", "cctv1", "CCTV-1 综合"),
    ("CCTV2", "cctv2", "CCTV-2 财经"),
    ("CCTV3", "cctv3", "CCTV-3 综艺"),
    ("CCTV4", "cctv4", "CCTV-4 中文国际"),
    ("CCTV5", "cctv5", "CCTV-5 体育"),
    ("CCTV6", "cctv6", "CCTV-6 电影"),
    ("CCTV7", "cctv7", "CCTV-7 国防军事"),
    ("CCTV8", "cctv8", "CCTV-8 电视剧"),
    ("CCTV9", "cctv9", "CCTV-9 纪录"),
    ("CCTV10", "cctv10", "CCTV-10 科教"),
    ("CCTV11", "cctv11", "CCTV-11 戏曲"),
    ("CCTV12", "cctv12", "CCTV-12 社会与法"),
    ("CCTV13", "cctv13", "CCTV-13 新闻"),
    ("CCTV14", "cctv14", "CCTV-14 少儿"),
    ("CCTV15", "cctv15", "CCTV-15 音乐"),
]

def get_cctv_epg(api_code, date_str):
    url = f"http://api.cntv.cn/epg/epginfo?c={api_code}&d={date_str}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        epg_list = []
        for item in data.get("list", []):
            epg_list.append({
                "start": item.get("st"),
                "end": item.get("et"),
                "title": item.get("t")
            })
        return epg_list
    except Exception as e:
        print(f"抓取失败 {api_code} {date_str}: {e}")
        return []

def gen_7day_date():
    date_list = []
    today = datetime.now()
    for i in range(7):
        d = today + timedelta(days=i)
        date_list.append(d.strftime("%Y%m%d"))
    return date_list

def build_xmltv(all_epg):
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<tv generator-info-name="CCTV-EPG">')

    # 写入频道 ID：CCTV1、CCTV2...
    for cid, _, name in CCTV_LIST:
        xml.append(f'<channel id="{cid}">')
        xml.append(f'  <display-name lang="cn">{name}</display-name>')
        xml.append('</channel>')

    # 写入节目
    for cid, prog_list in all_epg.items():
        for p in prog_list:
            xml.append(
                f'<programme start="{p["start"]} +0800" stop="{p["end"]} +0800" channel="{cid}">'
            )
            xml.append(f'  <title lang="cn">{p["title"]}</title>')
            xml.append('</programme>')

    xml.append('</tv>')
    return "\n".join(xml)

def save_gz(xml_content, gz_path="epg.xml.gz"):
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write(xml_content)

if __name__ == "__main__":
    date_list = gen_7day_date()
    all_epg = {}

    for cid, api_code, _ in CCTV_LIST:
        print(f"正在抓取 {cid} 7天节目单...")
        all_epg[cid] = []
        for d in date_list:
            epg = get_cctv_epg(api_code, d)
            if epg:
                all_epg[cid].extend(epg)

    xml_content = build_xmltv(all_epg)
    save_gz(xml_content)
    print("✅ 已生成 epg.xml.gz ，频道ID为 CCTV1~CCTV15")
