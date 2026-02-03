import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
from datetime import datetime

# 初始化EPG源列表（从config.txt读取网络源 + 本地潍坊源）
EPG_SOURCES = []

def load_epg_sources(config_path="config.txt"):
    """从配置文件读取网络EPG源"""
    if not os.path.exists(config_path):
        print(f"⚠️  配置文件{config_path}不存在，仅加载本地潍坊源")
        return ["output/weifang.xml"]
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        # 过滤注释和空行，获取有效链接
        network_sources = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        # 追加本地潍坊源
        network_sources.append("output/weifang.xml")
        print(f"✅ 从{config_path}加载{len(network_sources)-1}个网络源 + 1个本地源")
        return network_sources
    except Exception as e:
        print(f"⚠️  读取配置文件失败：{str(e)}，仅加载本地潍坊源")
        return ["output/weifang.xml"]

# 加载EPG源
EPG_SOURCES = load_epg_sources()

channels = {}
programmes = []

def fetch_epg_source(source_path):
    try:
        print(f"📥 处理: {source_path}")
        start_time = datetime.now()
        # 处理本地文件（酷9适配：GBK编码读取 + 空文件检测）
        if os.path.exists(source_path):
            try:
                # 优先用GBK读取，兼容酷9格式
                with open(source_path, "r", encoding="gbk") as f:
                    xml_content = f.read()
                # 检测空文件（仅含<tv></tv>或无内容）
                if not xml_content.strip() or xml_content.strip() == "<tv></tv>":
                    print(f"⚠️  本地文件为空，跳过处理：{source_path}")
                    return None
                root = ET.fromstring(xml_content)
                parse_time = (datetime.now() - start_time).total_seconds()
                print(f"✅ 读取本地文件(GBK)：{source_path} | 耗时: {parse_time:.2f}s")
                return root
            except UnicodeDecodeError:
                # 兼容UTF-8格式的备用方案
                with open(source_path, "r", encoding="utf-8") as f:
                    xml_content = f.read()
                # 检测空文件
                if not xml_content.strip() or xml_content.strip() == "<tv></tv>":
                    print(f"⚠️  本地文件为空，跳过处理：{source_path}")
                    return None
                root = ET.fromstring(xml_content)
                parse_time = (datetime.now() - start_time).total_seconds()
                print(f"✅ 读取本地文件(UTF-8)：{source_path} | 耗时: {parse_time:.2f}s")
                return root
            except Exception as e:
                print(f"⚠️  本地文件处理失败（不影响其他源）: {source_path} | 错误: {str(e)}")
                return None
        # 处理网络源（原有逻辑不变）
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        response = requests.get(source_path, headers=headers, timeout=20)
        response.raise_for_status()
        if source_path.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                xml_content = f.read().decode("utf-8")
        else:
            xml_content = response.text
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
        # 酷9适配：强制频道ID为纯数字（若为字母格式，自动转换为随机纯数字）
        if not channel_id.isdigit():
            import random
            channel_id = str(random.randint(1005, 9999))  # 避免与潍坊频道ID冲突
        if channel_id not in channels:
            display_name = channel.findtext(".//display-name", default="未知频道")
            channels[channel_id] = {"id": channel_id, "name": display_name}
            if "潍坊" in display_name:
                print(f"📌 新增潍坊频道：{display_name}（酷9适配ID：{channel_id}）")
            elif "山东" in display_name or "央视" in display_name or "卫视" in display_name:
                print(f"➕ 新增优先频道：{display_name}（酷9适配ID：{channel_id}）")
            else:
                print(f"➕ 新增普通频道：{display_name}（酷9适配ID：{channel_id}）")
        else:
            print(f"🔄 频道已存在：{channel.findtext('.//display-name', default='未知频道')}（ID：{channel_id}）")
    for programme in root.findall(".//programme"):
        channel_id = programme.get("channel")
        # 酷9适配：过滤非数字ID的节目
        if channel_id and channel_id.isdigit() and channel_id in channels:
            programmes.append({
                "channel_id": channel_id,
                "start": programme.get("start", ""),
                "stop": programme.get("stop", ""),
                "title": programme.findtext(".//title[@lang='zh']", default="未知节目")
            })

def generate_final_epg():
    # 酷9适配：频道排序（潍坊→山东→央视→卫视→其他）
    sorted_channels = []
    # 1. 潍坊频道（ID：1001-1004）
    sorted_channels.extend([c for c in channels.values() if c["id"] in ["1001", "1002", "1003", "1004"]])
    # 2. 山东本地频道（名称含"山东"）
    sorted_channels.extend([c for c in channels.values() if "山东" in c["name"] and c["id"] not in ["1001", "1002", "1003", "1004"]])
    # 3. 央视频道（名称含"央视"）
    sorted_channels.extend([c for c in channels.values() if "央视" in c["name"] and c not in sorted_channels])
    # 4. 卫视频道（名称含"卫视"）
    sorted_channels.extend([c for c in channels.values() if "卫视" in c["name"] and c not in sorted_channels])
    # 5. 其他频道
    sorted_channels.extend([c for c in channels.values() if c not in sorted_channels])
    
    tv = ET.Element("tv", {
        "source-info-name": "综合EPG源（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800"),
        "generator-info-name": "EPGMerge-Ku9"
    })
    # 添加排序后的频道
    for chan_info in sorted_channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": chan_info["id"]})
        ET.SubElement(chan_elem, "display-name").text = chan_info["name"]
    # 添加节目
    for prog in programmes:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "stop": prog["stop"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    os.makedirs("output", exist_ok=True)
    xml_str = ET.tostring(tv, encoding="gbk", xml_declaration=True)  # 酷9适配：GBK编码
    from xml.dom import minidom
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="  ")
    # 去除多余空行（避免设备解析异常）
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    
    with open("output/final_epg_complete.xml", "w", encoding="gbk") as f:
        f.write(xml_str)
    print(f"\n🎉 EPG生成完成（酷9适配）：output/final_epg_complete.xml（{len(channels)}个频道，{len(programmes)}个节目）")

if __name__ == "__main__":
    print("="*60 + "\nEPG合并工具（酷9适配）启动\n" + "="*60)
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
