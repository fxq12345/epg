import gzip
import requests
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 频道映射（ID是CCTV1格式，和你要求的完全一致）
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
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "list" not in data:
            print(f"⚠️ {api_code} {date_str} 接口返回无list字段")
            return []
        epg_list = []
        for item in data["list"]:
            if "st" in item and "et" in item and "t" in item:
                epg_list.append({
                    "start": item["st"],
                    "end": item["et"],
                    "title": item["t"]
                })
        print(f"✅ {api_code} {date_str} 抓取到 {len(epg_list)} 个节目")
        return epg_list
    except Exception as e:
        print(f"❌ 抓取失败 {api_code} {date_str}: {str(e)[:100]}")
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

    # 写入频道定义
    for cid, _, name in CCTV_LIST:
        xml.append(f'<channel id="{cid}">')
        xml.append(f'  <display-name lang="cn">{name}</display-name>')
        xml.append('</channel>')

    # 写入节目单（关键修复：确保有数据才写入）
    total_progs = 0
    for cid, prog_list in all_epg.items():
        if not prog_list:
            print(f"⚠️ {cid} 无节目数据，跳过写入")
            continue
        for p in prog_list:
            xml.append(
                f'<programme start="{p["start"]} +0800" stop="{p["end"]} +0800" channel="{cid}">'
            )
            xml.append(f'  <title lang="cn">{p["title"]}</title>')
            xml.append('</programme>')
            total_progs += 1

    xml.append('</tv>')
    print(f"📝 共写入 {total_progs} 个节目")
    return "\n".join(xml)

def save_gz(xml_content, xml_path="epg.xml", gz_path="epg.xml.gz"):
    # 同时保存原始XML和压缩版，方便你检查
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"✅ 已生成 {xml_path} 和 {gz_path}")

if __name__ == "__main__":
    date_list = gen_7day_date()
    all_epg = {}

    for cid, api_code, _ in CCTV_LIST:
        print(f"\n正在抓取 {cid}...")
        all_epg[cid] = []
        for d in date_list:
            epg = get_cctv_epg(api_code, d)
            if epg:
                all_epg[cid].extend(epg)

    xml_content = build_xmltv(all_epg)
    save_gz(xml_content)
    print("\n🎉 脚本执行完成，文件已生成")
