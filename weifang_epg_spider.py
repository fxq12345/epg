import requests
import os
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom

# 潍坊频道配置（替换为可用的EPG源链接，示例为公开XML格式源）
weifang_channels = [
    {"id": "SDWF-SDWF1", "name": "潍坊新闻综合频道", "url": "https://epg.example.com/weifang1.xml"},
    {"id": "SDWF-SDWF3", "name": "潍坊生活频道", "url": "https://epg.example.com/weifang3.xml"},
    {"id": "SDWF-SDWF2", "name": "潍坊公共频道", "url": "https://epg.example.com/weifang2.xml"},
    {"id": "SDWF-SDWF4", "name": "潍坊科教频道", "url": "https://epg.example.com/weifang4.xml"}
]

def fetch_epg_source(url):
    """读取公开EPG源的XML数据"""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return ET.fromstring(response.content)
    except Exception as e:
        print(f"❌ 获取EPG源失败：{url} | 错误：{str(e)}")
        return None

def generate_xmltv_file(channels):
    """整合潍坊频道的EPG数据并生成XML文件"""
    tv = ET.Element("tv", {
        "source-info-url": "公开EPG源",
        "source-info-name": "潍坊本地EPG",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })

    # 整合每个频道的EPG数据
    for channel in channels:
        # 添加频道信息
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        
        # 读取该频道的EPG节目数据
        root = fetch_epg_source(channel["url"])
        if root:
            # 提取节目信息并添加
            for programme in root.findall(".//programme"):
                prog_elem = ET.SubElement(tv, "programme", {
                    "start": programme.get("start"),
                    "stop": programme.get("stop"),
                    "channel": channel["id"]
                })
                title_elem = programme.find(".//title")
                if title_elem:
                    ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = title_elem.text

    # 保存文件
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    xml_str = os.linesep.join([line for line in xml_str.splitlines() if line.strip()])
    output_path = "output/weifang.xml"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    
    # 统计节目数量
    programme_count = len(tv.findall(".//programme"))
    print(f"🎉 潍坊EPG生成完成：{output_path}（{programme_count}条节目）")

if __name__ == "__main__":
    print("="*60 + "\n潍坊EPG整合工具启动\n" + "="*60)
    generate_xmltv_file(weifang_channels)
