import gzip
import requests
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 频道映射（ID是CCTV1格式）
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
    """
    抓取单频道单日节目单
    接口来源：第三方稳定源
    """
    # 🔥 关键修改：更换为可用的 API 地址
    url = f"https://epg.cntv.cn/api/getEpgInfo?c={api_code}&d={date_str}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # 🔥 关键修改：新接口的数据在 'programs' 字段里
        if "programs" not in data or not data["programs"]:
            print(f"ℹ️ {api_code} {date_str} 当天暂无节目单数据")
            return []

        epg_list = []
        for item in data["programs"]:
            # 🔥 关键修改：字段名改为 startTime, endTime, name
            if "startTime" in item and "endTime" in item and "name" in item:
                epg_list.append({
                    "start": item["startTime"],
                    "end": item["endTime"],
                    "title": item["name"]
                })
        
        print(f"✅ {api_code} {date_str} 抓取到 {len(epg_list)} 个节目")
        return epg_list
    except Exception as e:
        print(f"❌ 抓取失败 {api_code} {date_str}: {str(e)[:100]}")
        return []

def gen_7day_date():
    """生成未来7天的日期列表"""
    date_list = []
    today = datetime.now()
    for i in range(7):
        d = today + timedelta(days=i)
        date_list.append(d.strftime("%Y%m%d"))
    return date_list

def build_xmltv(all_epg):
    """构建 XMLTV 格式内容"""
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<tv generator-info-name="CCTV-EPG">')

    # 写入频道定义
    for cid, _, name in CCTV_LIST:
        xml.append(f'<channel id="{cid}">')
        xml.append(f'  <display-name lang="cn">{name}</display-name>')
        xml.append('</channel>')

    # 写入节目单
    total_progs = 0
    for cid, prog_list in all_epg.items():
        if not prog_list:
            continue
        for p in prog_list:
            # 🔥 关键修改：清洗时间格式，去掉空格和冒号，符合 XMLTV 规范
            start_time = p['start'].replace(" ", "").replace(":", "")
            end_time = p['end'].replace(" ", "").replace(":", "")
            
            xml.append(
                f'<programme start="{start_time} +0800" stop="{end_time} +0800" channel="{cid}">'
            )
            xml.append(f'  <title lang="cn">{p["title"]}</title>')
            xml.append('</programme>')
            total_progs += 1

    xml.append('</tv>')
    print(f"📝 共写入 {total_progs} 个节目")
    return "\n".join(xml)

def save_gz(xml_content, xml_path="epg.xml", gz_path="epg.xml.gz"):
    """保存为 XML 和 GZ 压缩文件"""
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"✅ 已生成 {xml_path} 和 {gz_path}")

if __name__ == "__main__":
    print("🚀 开始抓取 CCTV 节目单...")
    date_list = gen_7day_date()
    all_epg = {}

    for cid, api_code, _ in CCTV_LIST:
        print(f"\n正在抓取 {cid} ({api_code})...")
        all_epg[cid] = []
        for d in date_list:
            epg = get_cctv_epg(api_code, d)
            if epg:
                all_epg[cid].extend(epg)
    
    print("\n📦 正在生成 XMLTV 文件...")
    xml_content = build_xmltv(all_epg)
    save_gz(xml_content)
    print("\n🎉 脚本执行完成！")
