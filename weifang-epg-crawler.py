import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom

# ===================== 4个潍坊频道专属配置（无后缀基础链接，适配w1-w7拼接） =====================
CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a/w2"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1/w5"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_LIST = [("周一", "w1"), ("周二", "w2"), ("周三", "w3"), ("周四", "w4"), ("周五", "w5"), ("周六", "w6"), ("周日", "w7")]
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36", "Referer": "https://m.tvsou.com/"}

# ===================== 工具函数 =====================
def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_day_program(channel_name, channel_base_url, week_name, w_suffix):
    # 拼接正确URL：基础链接 + w1-w7
    url = f"{channel_base_url}{w_suffix}"
    programs = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        # 定位节目列表（适配tvsou移动端页面结构）
        program_items = soup.find_all("div", class_=re.compile("program-item|time-item"))
        if not program_items:
            # 备选：纯文本提取（兼容不同页面布局）
            lines = soup.get_text(separator="\n").splitlines()
            for line in lines:
                line = line.strip()
                match = re.match(r"^(\d{1,2}:\d{2})\s{1,}(.+)", line)
                if match:
                    t = match.group(1).strip()
                    title = match.group(2).strip()
                    if title and len(title) > 1 and len(t) <= 6:
                        programs.append((t, title))
        else:
            # 通过HTML标签提取（更精准）
            for item in program_items:
                time_tag = item.find(class_=re.compile("time|start-time"))
                title_tag = item.find(class_=re.compile("title|program-name"))
                if time_tag and title_tag:
                    t = time_tag.get_text(strip=True)
                    title = title_tag.get_text(strip=True)
                    if t and title:
                        programs.append((t, title))
        # 去重并按时间排序
        programs = sorted(list(set(programs)), key=lambda x: x[0])
        print(f"✅ {channel_name} - {week_name} 抓取节目：{len(programs)} 条")
        return programs
    except Exception as e:
        print(f"❌ {channel_name} - {week_name} 抓取失败：{str(e)[:50]}")
        return []

# ===================== 生成潍坊4频道专属XML =====================
def build_weifang_xml(all_channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "Weifang-4Channels-Weekly-EPG")
    root.set("generator-info-name", "Weifang-EPG-Crawler")

    # 生成频道节点
    for channel_name, _ in CHANNELS:
        ch = ET.SubElement(root, "channel")
        ch.set("id", channel_name)
        ET.SubElement(ch, "display-name").text = channel_name
        ET.SubElement(ch, "icon", src="")

    # 基准日期（本周一）
    monday = datetime.now() - timedelta(days=datetime.now().weekday())

    # 写入节目
    for channel_name, week_data_list in all_channel_data.items():
        print(f"\n→ 生成 {channel_name} 节目...")
        for i, (week_name, w_suffix, progs) in enumerate(week_data_list):
            current_day = monday + timedelta(days=i)
            if not progs:
                continue
            total = len(progs)
            for idx in range(total):
                start_t, title = progs[idx]
                # 计算结束时间
                if idx < total - 1:
                    end_t = progs[idx+1][0]
                else:
                    end_dt = datetime.strptime(f"{current_day.strftime('%Y%m%d')} {start_t}", "%Y%m%d %H:%M") + timedelta(hours=1)
                    end_t = end_dt.strftime("%H:%M")
                # 转换为XMLTV时间格式
                start = time_to_xmltv(current_day, start_t)
                end = time_to_xmltv(current_day, end_t)
                if not start or not end:
                    continue
                # 生成节目节点
                prog = ET.SubElement(root, "programme", channel=channel_name, start=start, stop=end)
                ET.SubElement(prog, "title", lang="zh").text = title
                ET.SubElement(prog, "desc", lang="zh").text = title

    # 格式化XML（去除空行）
    rough = ET.tostring(root, encoding="utf-8")
    dom = minidom.parseString(rough)
    pretty = "\n".join([line for line in dom.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8").split("\n") if line.strip()])
    return pretty.encode("utf-8")

# ===================== 主函数 =====================
def main():
    print("=" * 60)
    print("    潍坊4频道专属EPG抓取器（修正版）")
    print("=" * 60)

    # 抓取所有节目
    all_channel_data = {}
    for channel_name, channel_base_url in CHANNELS:
        print(f"\n【抓取 {channel_name}】")
        week_data_list = []
        for week_name, w_suffix in WEEK_LIST:
            progs = get_day_program(channel_name, channel_base_url, week_name, w_suffix)
            week_data_list.append((week_name, w_suffix, progs))
        all_channel_data[channel_name] = week_data_list

    # 生成XML文件
    xml_content = build_weifang_xml(all_channel_data)
    with open("weifang_4channels_epg.xml", "wb") as f:
        f.write(xml_content)

    print(f"\n✅ 生成完成！文件：weifang_4channels_epg.xml")
    print("✅ 已适配4个频道的正确链接格式")

if __name__ == "__main__":
    main()
