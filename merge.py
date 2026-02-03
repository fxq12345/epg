import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
import time
from datetime import datetime, timedelta

# 酷9专用频道ID+节目数据（直接适配设备）
COOL9_CHANNELS = {
    # 潍坊频道
    "潍坊新闻综合频道": {
        "id": "1",
        "programs": [
            {"start": "07:00", "end": "08:00", "title": "潍坊新闻早班车"},
            {"start": "08:00", "end": "09:00", "title": "生活帮"},
            {"start": "12:00", "end": "12:30", "title": "正午新闻"},
            {"start": "18:30", "end": "19:00", "title": "潍坊新闻联播"},
            {"start": "20:00", "end": "22:00", "title": "黄金剧场"}
        ]
    },
    # 央视频道
    "CCTV-1": {
        "id": "10",
        "programs": [
            {"start": "07:00", "end": "09:00", "title": "朝闻天下"},
            {"start": "12:00", "end": "12:30", "title": "新闻30分"},
            {"start": "19:00", "end": "19:30", "title": "新闻联播"},
            {"start": "19:30", "end": "21:30", "title": "黄金剧场"}
        ]
    },
    # 山东卫视
    "山东卫视": {
        "id": "30",
        "programs": [
            {"start": "08:00", "end": "09:00", "title": "早间新闻"},
            {"start": "19:30", "end": "21:30", "title": "黄金剧场"}
        ]
    }
}

def generate_cool9_epg():
    # 生成酷9专用XML
    tv = ET.Element("tv", {
        "source": "酷9专用EPG",
        "date": datetime.now().strftime("%Y%m%d")
    })
    
    # 添加频道+节目
    today = datetime.now().strftime("%Y%m%d")
    for channel_name, info in COOL9_CHANNELS.items():
        # 添加频道信息
        channel_elem = ET.SubElement(tv, "channel", {"id": info["id"]})
        ET.SubElement(channel_elem, "display-name").text = channel_name
        
        # 添加节目（带有效标题）
        for prog in info["programs"]:
            # 拼接时间格式（酷9要求：YYYYMMDDHHMMSS）
            start_time = f"{today}{prog['start'].replace(':', '')}00"
            end_time = f"{today}{prog['end'].replace(':', '')}00"
            
            prog_elem = ET.SubElement(tv, "programme", {
                "start": start_time,
                "stop": end_time,
                "channel": info["id"]
            })
            ET.SubElement(prog_elem, "title").text = prog["title"]
            ET.SubElement(prog_elem, "desc").text = f"{prog['title']} - 精彩节目"
    
    # 保存为酷9识别的XML文件
    os.makedirs("output", exist_ok=True)
    xml_str = ET.tostring(tv, encoding="utf-8").decode("utf-8")
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    final_xml = xml_declaration + xml_str
    
    with open("output/cool9_epg.xml", "w", encoding="utf-8") as f:
        f.write(final_xml)
    print("🎉 酷9专用EPG生成完成：output/cool9_epg.xml（含有效节目数据）")

if __name__ == "__main__":
    print("="*60 + "\n酷9专用EPG生成工具启动\n" + "="*60)
    generate_cool9_epg()
    print("="*60)
