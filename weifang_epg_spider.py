import os
from datetime import datetime, timedelta

# 酷9名称匹配版-潍坊频道（名称与设备完全一致）
weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道"},
    {"id": "1002", "name": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化"}
]

# 潍坊频道静态节目数据
STATIC_EPG_DATA = [
    {"channel_id": "1001", "time": "07:00", "title": "潍坊新闻早班车"},
    {"channel_id": "1001", "time": "08:00", "title": "生活帮"},
    {"channel_id": "1001", "time": "12:00", "title": "正午新闻"},
    {"channel_id": "1001", "time": "18:30", "title": "潍坊新闻联播"},
    {"channel_id": "1001", "time": "20:00", "title": "黄金剧场"},
    {"channel_id": "1002", "time": "09:00", "title": "生活百科"},
    {"channel_id": "1002", "time": "12:30", "title": "美食潍坊"},
    {"channel_id": "1002", "time": "19:00", "title": "家居风尚"},
    {"channel_id": "1003", "time": "10:00", "title": "健康大讲堂"},
    {"channel_id": "1003", "time": "15:00", "title": "公共剧场"},
    {"channel_id": "1004", "time": "08:30", "title": "科普天地"},
    {"channel_id": "1004", "time": "16:00", "title": "教育在线"}
]

def get_current_date():
    return datetime.now().date().strftime("%Y-%m-%d")

# 生成未来N天静态EPG
def generate_static_epg(days=3):
    epg_data = []
    for day_offset in range(days):
        current_date = (datetime.now() + timedelta(days=day_offset)).date().strftime("%Y-%m-%d")
        for item in STATIC_EPG_DATA:
            try:
                start_time = datetime.strptime(f"{current_date} {item['time']}", "%Y-%m-%d %H:%M")
                epg_data.append({
                    "channel_id": item["channel_id"],
                    "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                    "title": item["title"]
                })
            except ValueError:
                continue
    print(f"📊 加载静态节目数据：{len(epg_data)}条（未来{days}天）")
    return epg_data

# 生成标准XmlTV格式文件
def generate_xmltv_file(epg_data, channels):
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    tv = ET.Element("tv", {
        "source-info-name": "潍坊EPG（静态数据）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    
    # 写入频道信息
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        # 写入对应节目
        channel_epg = [prog for prog in epg_data if prog["channel_id"] == channel["id"]]
        for prog in channel_epg:
            prog_elem = ET.SubElement(tv, "programme", {
                "start": prog["start"],
                "channel": channel["id"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    # 创建输出目录并生成文件
    os.makedirs("output", exist_ok=True)
    xml_str = ET.tostring(tv, encoding="utf-8").decode("utf-8")
    xml_str = minidom.parseString(xml_declaration + xml_str).toprettyxml(indent="  ")
    xml_str = os.linesep.join([line for line in xml_str.splitlines() if line.strip()])
    output_path = "output/weifang.xml"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"🎉 潍坊EPG（静态）生成完成：{output_path}（{len(epg_data)}条节目）")

if __name__ == "__main__":
    print("="*60 + "\n潍坊EPG（静态数据）生成器启动\n" + "="*60)
    epg_data = generate_static_epg(days=3)
    generate_xmltv_file(epg_data, weifang_channels)
