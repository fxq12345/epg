import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
from datetime import datetime

# 有效EPG源（你提供的5个+本地潍坊文件）
EPG_SOURCES = [
    "https://epg.27481716.xyz/epg.xml",
    "https://e.erw.cc/all.xml",
    "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
    "http://epg.51zmt.top:8000/e.xml",
    "https://raw.githubusercontent.com/fanmingming/live/main/e.xml",
    "output/weifang.xml"
]

channels = {}
programmes = []

def fetch_epg_source(source_path):
    try:
        print(f"📥 处理: {source_path}")
        start_time = datetime.now()
        # 处理本地文件
        if os.path.exists(source_path):
            with open(source_path, "r", encoding="utf-8") as f:
                xml_content = f.read()
            root = ET.fromstring(xml_content)
            print(f"✅ 读取本地文件: {source_path} | 耗时: {(datetime.now()-start_time).total_seconds():.2f}s")
            return root
        # 处理网络源
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        response = requests.get(source_path, headers=headers, timeout=20)
        response.raise_for_status()
        if source_path.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                xml_content = f.read().decode("utf-8")
        else:
            xml_content = response.text
        root = ET.fromstring(xml_content)
        print(f"✅ 抓取网络源: {source_path} | 耗时: {(datetime.now()-start_time).total_seconds():.2f}s")
        return root
    except Exception as e:
        print(f"❌ 处理失败: {source_path} | 错误: {str(e)}")
        return None

def parse_epg(root, source_path):
    for channel in root.findall(".//channel"):
        channel_id = channel.get("id")
        if not channel_id: continue
        if channel_id not in channels:
            display_name = channel.findtext(".//display-name", default="未知频道")
            channels[channel_id] = {"id": channel_id, "name": display_name}
            if "潍坊" in display_name:
                print(f"📌 新增潍坊频道：{display_name}（ID：{channel_id}）")
            else:
                print(f"➕ 新增频道：{display_name}（ID：{channel_id}）")
        else:
            print(f"🔄 频道已存在：{channel.findtext('.//display-name', default='未知频道')}")
    for programme in root.findall(".//programme"):
        channel_id = programme.get("channel")
        if channel_id in channels:
            programmes.append({
                "channel_id": channel_id,
                "start": programme.get("start", ""),
                "stop": programme.get("stop", ""),
                "title": programme.findtext(".//title[@lang='zh']", default="未知节目")
            })

def generate_final_epg():
    tv = ET.Element("tv", {
        "source-info-name": "综合EPG源",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    for channel_id, chan_info in channels.items():
        chan_elem = ET.SubElement(tv, "channel", {"id": channel_id})
        ET.SubElement(chan_elem, "display-name").text = chan_info["name"]
    for prog in programmes:
        prog_elem = ET.SubElement(tv, "programme", {"start": prog["start"], "stop": prog["stop"], "channel": prog["channel_id"]})
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    os.makedirs("output", exist_ok=True)
    xml_str = ET.tostring(tv, encoding="utf-8", xml_declaration=True)
    from xml.dom import minidom
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="  ")
    with open("output/final_epg_complete.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"\n🎉 EPG生成完成：output/final_epg_complete.xml（{len(channels)}个频道，{len(programmes)}个节目）")

if __name__ == "__main__":
    print("="*60 + "\nEPG合并工具启动\n" + "="*60)
    start_total = datetime.now()
    for source in EPG_SOURCES:
        print(f"\n{'='*40} 处理源：{source} {'='*40}")
        root = fetch_epg_source(source)
        if root:
            parse_epg(root, source)
    if channels and programmes:
        generate_final_epg()
    print(f"\n⏱️  总耗时：{(datetime.now()-start_total).total_seconds():.2f}秒")
