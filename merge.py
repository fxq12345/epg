import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
import time
from datetime import datetime, timedelta

# 从config.txt加载EPG源
EPG_SOURCES = []

def load_epg_sources(config_path="config.txt"):
    if not os.path.exists(config_path):
        print(f"⚠️  配置文件{config_path}不存在，仅加载本地潍坊源")
        return ["output/weifang.xml"]
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        network_sources = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        network_sources.append("output/weifang.xml")
        print(f"✅ 从{config_path}加载{len(network_sources)-1}个网络源 + 1个本地源")
        return network_sources
    except Exception as e:
        print(f"⚠️  读取配置文件失败：{str(e)}，仅加载本地潍坊源")
        return ["output/weifang.xml"]

EPG_SOURCES = load_epg_sources()
channels = {}
programmes = []

# 潍坊静态节目数据（用于补全其他源的当天节目）
STATIC_PROG_DATA = [
    {"channel_name": "潍坊新闻综合频道", "time": "07:00", "title": "潍坊新闻早班车", "duration": 60},
    {"channel_name": "潍坊新闻综合频道", "time": "08:00", "title": "生活帮", "duration": 60},
    {"channel_name": "潍坊新闻综合频道", "time": "12:00", "title": "正午新闻", "duration": 30},
    {"channel_name": "潍坊新闻综合频道", "time": "18:30", "title": "潍坊新闻联播", "duration": 30},
    {"channel_name": "潍坊新闻综合频道", "time": "20:00", "title": "黄金剧场", "duration": 120},
    {"channel_name": "潍坊经济生活", "time": "09:00", "title": "生活百科", "duration": 60},
    {"channel_name": "潍坊经济生活", "time": "12:30", "title": "美食潍坊", "duration": 30},
    {"channel_name": "潍坊经济生活", "time": "19:00", "title": "家居风尚", "duration": 60},
    {"channel_name": "潍坊公共", "time": "10:00", "title": "健康大讲堂", "duration": 60},
    {"channel_name": "潍坊公共", "time": "15:00", "title": "公共剧场", "duration": 120},
    {"channel_name": "潍坊科教文化", "time": "08:30", "title": "科普天地", "duration": 60},
    {"channel_name": "潍坊科教文化", "time": "16:00", "title": "教育在线", "duration": 60}
]

def fetch_epg_source(source_path):
    try:
        print(f"📥 处理: {source_path}")
        start_time = datetime.now()
        if os.path.exists(source_path):
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    xml_content = f.read()
                if not xml_content.strip() or xml_content.strip() == "<tv></tv>":
                    print(f"⚠️  本地文件为空，跳过处理：{source_path}")
                    return None
                root = ET.fromstring(xml_content)
                parse_time = (datetime.now() - start_time).total_seconds()
                print(f"✅ 读取本地文件(UTF-8)：{source_path} | 耗时: {parse_time:.2f}s")
                return root
            except Exception as e:
                print(f"⚠️  本地文件处理失败：{source_path} | 错误: {str(e)}")
                return None
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        max_retries = 2
        response = None
        for retry in range(max_retries):
            try:
                response = requests.get(source_path, headers=headers, timeout=20)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    print(f"⚠️  网络源重试{retry+1}/{max_retries}：{str(e)}")
                    time.sleep(3)
                else:
                    raise e
        if source_path.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                xml_content = f.read().decode("utf-8")
        else:
            xml_content = response.text
        if not xml_content.strip() or not xml_content.startswith("<?xml"):
            print(f"⚠️  网络源数据无效，跳过：{source_path}")
            return None
        root = ET.fromstring(xml_content)
        # 校验当天节目数量
        today = datetime.now().date().strftime("%Y%m%d")
        today_prog_count = len(root.findall(f".//programme[starts-with(@start,'{today}')]"))
        if today_prog_count < 5:
            print(f"⚠️  当天节目数量过少（仅{today_prog_count}条），跳过该源：{source_path}")
            return None
        parse_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ 抓取网络源: {source_path} | 耗时: {parse_time:.2f}s")
        return root
    except Exception as e:
        print(f"❌ 处理失败: {source_path} | 错误: {str(e)}")
        return None

def parse_epg(root, source_path):
    for channel in root.findall(".//channel"):
        channel_name = channel.findtext(".//display-name", default="未知频道").strip()
        if not channel_name:
            continue
        channel_id = channel.get("id")
        if not channel_id or not channel_id.isdigit():
            import random
            channel_id = f"wf_{random.randint(1005, 9999)}"
        if channel_name not in channels:
            channels[channel_name] = {"name": channel_name, "id": channel_id}
            if "潍坊" in channel_name:
                print(f"📌 新增潍坊频道：{channel_name}（ID：{channel_id}）")
            elif "山东" in channel_name or "央视" in channel_name or "卫视" in channel_name:
                print(f"➕ 新增优先频道：{channel_name}（ID：{channel_id}）")
            else:
                print(f"➕ 新增普通频道：{channel_name}（ID：{channel_id}）")
        else:
            print(f"🔄 频道已存在：{channel_name}（ID：{channels[channel_name]['id']}）")
    for programme in root.findall(".//programme"):
        prog_channel_id = programme.get("channel")
        prog_channel_name = None
        for name, info in channels.items():
            if info["id"] == prog_channel_id:
                prog_channel_name = name
                break
        if not prog_channel_name:
            continue
        programmes.append({
            "channel_name": prog_channel_name,
            "start": programme.get("start", ""),
            "stop": programme.get("stop", ""),
            "title": programme.findtext(".//title[@lang='zh']", default="未知节目").strip()
        })

def fill_missing_today_programs():
    today = datetime.now().date().strftime("%Y%m%d")
    for channel_name in channels.keys():
        has_today = any(p["start"].startswith(today) for p in programmes if p["channel_name"] == channel_name)
        if not has_today:
            for prog in STATIC_PROG_DATA:
                if prog["channel_name"] == channel_name:
                    start = datetime.strptime(f"{today} {prog['time']}", "%Y%m%d %H:%M")
                    programmes.append({
                        "channel_name": channel_name,
                        "start": start.strftime("%Y%m%d%H%M%S +0800"),
                        "stop": (start + timedelta(minutes=prog["duration"])).strftime("%Y%m%d%H%M%S +0800"),
                        "title": prog["title"]
                    })
            print(f"🔧 补全频道当天节目：{channel_name}")

def generate_final_epg():
    sorted_channel_names = []
    sorted_channel_names.extend([name for name in channels.keys() if "潍坊" in name])
    sorted_channel_names.extend([name for name in channels.keys() if "山东" in name and name not in sorted_channel_names])
    sorted_channel_names.extend([name for name in channels.keys() if "央视" in name and name not in sorted_channel_names])
    sorted_channel_names.extend([name for name in channels.keys() if "卫视" in name and name not in sorted_channel_names])
    sorted_channel_names.extend([name for name in channels.keys() if name not in sorted_channel_names])
    
    tv = ET.Element("tv", {
        "source-info-name": "综合EPG源（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    
    for channel_name in sorted_channel_names:
        channel_info = channels[channel_name]
        chan_elem = ET.SubElement(tv, "channel", {"id": channel_info["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel_name
    programmes.sort(key=lambda x: x["start"])
    for prog in programmes:
        prog_channel_id = channels[prog["channel_name"]]["id"]
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "stop": prog["stop"],
            "channel": prog_channel_id
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    os.makedirs("output", exist_ok=True)
    xml_str = ET.tostring(tv, encoding="utf-8").decode("utf-8")
    from xml.dom import minidom
    xml_str = minidom.parseString(xml_declaration + xml_str).toprettyxml(indent="  ")
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    
    with open("output/final_epg_complete.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"\n🎉 EPG生成完成：output/final_epg_complete.xml（{len(channels)}个频道，{len(programmes)}个节目）")

if __name__ == "__main__":
    print("="*60 + "\nEPG合并工具（酷9名称匹配版）启动\n" + "="*60)
    start_total = datetime.now()
    for source in EPG_SOURCES:
        print(f"\n{'='*40} 处理源：{source} {'='*40}")
        root = fetch_epg_source(source)
        if root:
            parse_epg(root, source)
    if channels and programmes:
        fill_missing_today_programs()
        generate_final_epg()
    else:
        print("\n❌ 未获取到有效EPG数据！")
    total_time = (datetime.now() - start_total).total_seconds()
    print(f"\n⏱️  总耗时：{total_time:.2f} 秒")
    print("="*60)
