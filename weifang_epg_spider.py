import os
from datetime import datetime, timedelta

# 酷9名称匹配版-潍坊频道（新增2个本地频道，名称与设备完全一致）
weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道"},
    {"id": "1002", "name": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化"},
    {"id": "1005", "name": "潍坊图文频道"},  # 新增频道
    {"id": "1006", "name": "潍坊影视娱乐"}   # 新增频道
]

# 潍坊频道静态节目数据（扩展全时段节目，每个频道每天8-10条，覆盖7天）
STATIC_EPG_DATA = [
    # 潍坊新闻综合频道（1001）
    {"channel_id": "1001", "time": "06:30", "title": "晨间天气预报", "duration": 15},
    {"channel_id": "1001", "time": "07:00", "title": "潍坊新闻早班车", "duration": 60},
    {"channel_id": "08:00", "title": "生活帮", "duration": 60},
    {"channel_id": "1001", "time": "09:00", "title": "法治在线", "duration": 45},
    {"channel_id": "1001", "time": "12:00", "title": "正午新闻", "duration": 30},
    {"channel_id": "1001", "time": "14:00", "title": "经典剧场", "duration": 120},
    {"channel_id": "1001", "time": "18:30", "title": "潍坊新闻联播", "duration": 30},
    {"channel_id": "1001", "time": "20:00", "title": "黄金剧场", "duration": 120},
    {"channel_id": "1001", "time": "22:30", "title": "晚间新闻", "duration": 20},
    
    # 潍坊经济生活（1002）
    {"channel_id": "1002", "time": "07:30", "title": "健康养生堂", "duration": 45},
    {"channel_id": "1002", "time": "09:00", "title": "生活百科", "duration": 60},
    {"channel_id": "1002", "time": "11:00", "title": "房产直通车", "duration": 30},
    {"channel_id": "1002", "time": "12:30", "title": "美食潍坊", "duration": 30},
    {"channel_id": "1002", "time": "15:00", "title": "汽车风尚", "duration": 60},
    {"channel_id": "1002", "time": "19:00", "title": "家居设计", "duration": 60},
    {"channel_id": "1002", "time": "20:30", "title": "创业故事", "duration": 45},
    {"channel_id": "1002", "time": "22:00", "title": "生活麻辣烫", "duration": 30},
    
    # 潍坊公共（1003）
    {"channel_id": "1003", "time": "08:00", "title": "农业科技", "duration": 60},
    {"channel_id": "1003", "time": "10:00", "title": "健康大讲堂", "duration": 60},
    {"channel_id": "1003", "time": "12:00", "title": "公共服务公告", "duration": 20},
    {"channel_id": "1003", "time": "15:00", "title": "公共剧场", "duration": 120},
    {"channel_id": "1003", "time": "17:30", "title": "校园风采", "duration": 30},
    {"channel_id": "1003", "time": "19:30", "title": "百姓故事", "duration": 45},
    {"channel_id": "1003", "time": "21:00", "title": "戏曲欣赏", "duration": 60},
    
    # 潍坊科教文化（1004）
    {"channel_id": "1004", "time": "08:30", "title": "科普天地", "duration": 60},
    {"channel_id": "1004", "time": "10:30", "title": "文化潍坊", "duration": 45},
    {"channel_id": "1004", "time": "12:00", "title": "读书分享会", "duration": 30},
    {"channel_id": "1004", "time": "14:00", "title": "艺术鉴赏", "duration": 60},
    {"channel_id": "1004", "time": "16:00", "title": "教育在线", "duration": 60},
    {"channel_id": "1004", "time": "19:00", "title": "书法绘画", "duration": 45},
    {"channel_id": "1004", "time": "20:30", "title": "历史讲堂", "duration": 60},
    
    # 潍坊图文频道（1005，新增）
    {"channel_id": "1005", "time": "09:00", "title": "财经资讯", "duration": 30},
    {"channel_id": "1005", "time": "11:00", "title": "旅游攻略", "duration": 45},
    {"channel_id": "1005", "time": "13:00", "title": "影视快讯", "duration": 30},
    {"channel_id": "1005", "time": "15:00", "title": "体育赛事集锦", "duration": 60},
    {"channel_id": "1005", "time": "17:00", "title": "时尚潮流", "duration": 30},
    {"channel_id": "1005", "time": "19:30", "title": "图文点播", "duration": 90},
    {"channel_id": "1005", "time": "21:30", "title": "音乐排行榜", "duration": 45},
    
    # 潍坊影视娱乐（1006，新增）
    {"channel_id": "1006", "time": "10:00", "title": "经典电影展播", "duration": 120},
    {"channel_id": "1006", "time": "14:00", "title": "电视剧场", "duration": 150},
    {"channel_id": "1006", "time": "17:30", "title": "动漫世界", "duration": 60},
    {"channel_id": "1006", "time": "19:00", "title": "热门电影", "duration": 120},
    {"channel_id": "1006", "time": "21:30", "title": "娱乐头条", "duration": 30},
    {"channel_id": "1006", "time": "22:30", "title": "午夜剧场", "duration": 120}
]

def generate_static_epg(days=7):
    epg_data = []
    # 生成“今天+未来6天”共7天数据（修正原逻辑笔误）
    for day_offset in range(days):
        current_date = (datetime.now() + timedelta(days=day_offset)).date().strftime("%Y-%m-%d")
        for item in STATIC_EPG_DATA:
            try:
                start_time = datetime.strptime(f"{current_date} {item['time']}", "%Y-%m-%d %H:%M")
                stop_time = start_time + timedelta(minutes=item["duration"])
                epg_data.append({
                    "channel_id": item["channel_id"],
                    "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                    "stop": stop_time.strftime("%Y%m%d%H%M%S +0800"),
                    "title": item["title"]
                })
            except (ValueError, KeyError):
                continue
    print(f"📊 加载静态节目数据：{len(epg_data)}条（今天+未来{days-1}天，共{days}天）")
    return epg_data

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
                "stop": prog["stop"],
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
    epg_data = generate_static_epg(days=7)
    generate_xmltv_file(epg_data, weifang_channels)
