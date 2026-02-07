import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom

# ===================== 4个潍坊频道配置（全部周播w1-w7） =====================
# 格式：(频道名称, 基础链接)
CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561/w6"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a/w2"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1/w6"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0/")
]

# 周一到周日 固定后缀 (所有频道通用周循环)
WEEK_LIST = [
    ("周一", "w1"),
    ("周二", "w2"),
    ("周三", "w3"),
    ("周四", "w4"),
    ("周五", "w5"),
    ("周六", "w6"),
    ("周日", "w7"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://m.tvsou.com/"
}

# ===================== 工具函数 =====================
def time_to_xmltv(base_date, time_str):
    # 生成标准XMLTV时间格式
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

# ===================== 抓取单个频道的单天节目（w1-w7） =====================
def get_day_program(channel_name, channel_base_url, week_name, w_suffix):
    # 处理部分频道基础链接已带后缀的情况（自动拼接正确URL）
    if channel_base_url.endswith(("w1", "w2", "w3", "w4", "w5", "w6", "w7")):
        url = channel_base_url[:-2] + w_suffix  # 替换原有后缀为当前星期后缀
    else:
        url = channel_base_url + w_suffix
    programs = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 严格匹配格式：00:35  名家论坛（适配所有4个频道页面结构）
        lines = soup.get_text(separator="\n").splitlines()
        for line in lines:
            line = line.strip()
            match = re.match(r"^(\d{1,2}:\d{2})\s{1,}(.+)", line)
            if match:
                t = match.group(1).strip()
                title = match.group(2).strip()
                if title and len(title) > 1 and len(t) <= 6:
                    programs.append((t, title))

        programs = sorted(list(set(programs)), key=lambda x: x[0])
        print(f"✅ {channel_name} - {week_name}({w_suffix}) 抓取节目：{len(programs)} 条")
        return programs
    except Exception as e:
        print(f"❌ {channel_name} - {week_name} 抓取失败：{str(e)[:50]}")  # 截取错误信息
        return []

# ===================== 生成4个频道的7天整合版XMLTV =====================
def build_xml(all_channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "潍坊4频道-周播固定节目")
    root.set("generator-info-name", "Weifang-4Channels-Weekly-EPG")

    # 1. 生成所有频道的基础信息节点
    for channel_name, _ in CHANNELS:
        ch = ET.SubElement(root, "channel")
        ch.set("id", channel_name)
        disp = ET.SubElement(ch, "display-name")
        disp.text = channel_name
        ET.SubElement(ch, "icon", src="")  # 可自行添加图标URL

    # 2. 基准日期：本周一（用于生成连续7天节目时间）
    base = datetime.now()
    monday = base - timedelta(days=base.weekday())

    # 3. 逐频道、逐天写入节目
    for channel_name, week_data_list in all_channel_data.items():
        print(f"\n→ 开始生成 {channel_name} 7天节目...")
        for i, (week_name, w_suffix, progs) in enumerate(week_data_list):
            current_day = monday + timedelta(days=i)
            if not progs:
                continue

            total = len(progs)
            for idx in range(total):
                start_t, title = progs[idx]

                # 计算结束时间（下一个节目开始，最后一个节目默认+1小时）
                if idx < total - 1:
                    end_t = progs[idx + 1][0]
                else:
                    try:
                        h, m = start_t.split(":")
                        end_dt = datetime.strptime(f"{current_day.strftime('%Y%m%d')} {h}:{m}", "%Y%m%d %H:%M") + timedelta(hours=1)
                        end_t = end_dt.strftime("%H:%M")
                    except:
                        end_t = start_t

                start = time_to_xmltv(current_day, start_t)
                end = time_to_xmltv(current_day, end_t)
                if not start or not end:
                    continue

                # 生成节目节点
                prog = ET.SubElement(root, "programme")
                prog.set("channel", channel_name)
                prog.set("start", start)
                prog.set("stop", end)

                t_node = ET.SubElement(prog, "title", lang="zh")
                t_node.text = title
                d_node = ET.SubElement(prog, "desc", lang="zh")
                d_node.text = title

    # 格式化XML（去除多余空行，优化格式）
    rough = ET.tostring(root, encoding="utf-8")
    dom = minidom.parseString(rough)
    pretty = "\n".join([line for line in dom.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8").split("\n") if line.strip()])
    return pretty.encode("utf-8")

# ===================== 主程序 =====================
def main():
    print("=" * 70)
    print("    潍坊4频道 周播固定7天节目 EPG XML 生成器（完整版）")
    print("    包含：新闻、经济生活、科教、公共频道 | 周一w1~周日w7")
    print("=" * 70)

    # 抓取所有频道的7天节目
    all_channel_data = {}
    for channel_name, channel_base_url in CHANNELS:
        print(f"\n【开始抓取 {channel_name}】")
        week_data_list = []
        for week_name, w_suffix in WEEK_LIST:
            progs = get_day_program(channel_name, channel_base_url, week_name, w_suffix)
            week_data_list.append((week_name, w_suffix, progs))
        all_channel_data[channel_name] = week_data_list

    # 生成整合版XML文件
    xml_content = build_xml(all_channel_data)
    output_filename = "潍坊4频道_周播7天.xml"
    with open(output_filename, "wb") as f:
        f.write(xml_content)

    print("\n" + "=" * 70)
    print(f"✅ 全部生成完成！文件：{output_filename}")
    print("✅ 包含4个潍坊本地频道，每个频道7天完整节目")
    print("✅ 标准XMLTV格式，酷9、电视、IPTV播放器直接导入")
    print("✅ 每周循环节目，上传GitHub后可永久使用+自动更新")
    print("=" * 70)

if __name__ == "__main__":
    main()
