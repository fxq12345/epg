import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
import time
from datetime import datetime

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

def fetch_epg_source(source_path):
    try:
        print(f"📥 处理: {source_path}")
        start_time = datetime.now()
        # 处理本地文件
        if os.path.exists(source_path):
            try:
                # 统一用UTF-8读取（避免编码冲突）
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
        # 处理网络源：重试+数据校验
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
        # 校验网络源数据
        if source_path.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                xml_content = f.read().decode("utf-8")
        else:
            xml_content = response.text
        if not xml_content.strip() or not xml_content.startswith("<?xml"):
            print(f"⚠️  网络源数据无效，跳过：{source_path}")
            return None
        root = ET.fromstring(xml_content)
        parse_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ 抓取网络源: {source_path} | 耗时: {parse_time:.2f}s")
        return root
    except Exception as e:
        print(f"❌ 处理失败: {source_path} | 错误: {str(e)}")
        return None

def parse_epg(root, source_path):
    for channel in root.findall(".//channel"):
        channel_id = channel.get("id")
        if not channel_id:
            continue
        if not channel_id.isdigit():
            import random
            channel_id = str(random.randint(1005, 9999))
        if channel_id not in channels:
            display_name = channel.findtext(".//display-name", default="未知频道")
            channels[channel_id] = {"id": channel_id, "name": display_name}
            if "潍坊" in display_name:
                print(f"📌 新增潍坊频道：{display_name}（ID：{channel_id}）")
            elif "山东" in display_name or "央视" in display_name or "卫视" in display_name:
                print(f"➕ 新增优先频道：{display_name}（ID：{channel_id}）")
            else:
                print(f"➕ 新增普通频道：{display_name}（ID：{channel_id}）")
        else:
            print(f"🔄 频道已存在：{channel.findtext('.//display-name')}（ID：{channel_id}）")
    for programme in root.findall(".//programme"):
        channel_id = programme.get("channel")
        if channel_id and channel_id.isdigit() and channel_id in channels:
            programmes.append({
                "channel_id": channel_id,
                "start": programme.get("start", ""),
                "stop": programme.get("stop", ""),
                "title": programme.findtext(".//title[@lang='zh']", default="未知节目")
            })

def generate_final_epg():
    # 频道排序（潍坊→山东→央视→卫视→其他）
    sorted_channels = []
    sorted_channels.extend([c for c in channels.values() if c["id"] in ["1001", "1002", "1003", "1004"]])
    sorted_channels.extend([c for c in channels.values() if "山东" in c["name"] and c["id"] not in ["1001", "1002", "1003", "1004"]])
    sorted_channels.extend([c for c in channels.values() if "央视" in c["name"] and c not in sorted_channels])
    sorted_channels.extend([c for c in channels.values() if "卫视" in c["name"] and c not in sorted_channels])
    sorted_channels.extend([c for c in channels.values() if c not in sorted_channels])
    
    # 生成UTF-8编码的XML（兼容酷9）
    tv = ET.Element("tv", {
        "source-info-name": "综合EPG源（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    # 添加XML声明（指定UTF-8编码）
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    
    for chan_info in sorted_channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": chan_info["id"]})
        ET.SubElement(chan_elem, "display-name").text = chan_info["name"]
    for prog in programmes:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "stop": prog["stop"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    os.makedirs("output", exist_ok=True)
    # 生成XML内容（UTF-8）
    xml_str = ET.tostring(tv, encoding="utf-8").decode("utf-8")
    # 拼接声明+内容，格式化
    from xml.dom import minidom
    xml_str = minidom.parseString(xml_declaration + xml_str).toprettyxml(indent="  ")
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    
    with open("output/final_epg_complete.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"\n🎉 EPG生成完成：output/final_epg_complete.xml（{len(channels)}个频道，{len(programmes)}个节目）")

if __name__ == "__main__":
    print("="*60 + "\nEPG合并工具（编码修复版）启动\n" + "="*60)
    start_total = datetime.now()
    for source in EPG_SOURCES:
        print(f"\n{'='*40} 处理源：{source} {'='*40}")
        root = fetch_epg_source(source)
        if root:
            parse_epg(root, source)
    if channels and programmes:
        generate_final_epg()
    else:
        print("\n❌ 未获取到有效EPG数据！")
    total_time = (datetime.now() - start_total).total_seconds()
    print(f"\n⏱️  总耗时：{total_time:.2f} 秒")
    print("="*60)
