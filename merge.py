import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
from datetime import datetime

# 配置：EPG源列表（包含潍坊本地EPG源）
EPG_SOURCES = [
    "https://epg.27481716.xyz/epg.xml",
    "https://e.erw.cc/all.xml",
    "https://raw.githubusercontent.com/kule31/xmlgz/main/all.xml.gz",
    "http://epg.51zmt.top:8000/e.xml",
    "https://raw.githubusercontent.com/fanmingming/live/main/e.xml",
    "output/weifang.xml"  # 潍坊本地EPG源（需先运行爬虫生成）
]

# 全局存储：频道和节目数据（仅去重，无任何过滤）
channels = {}  # key: channel_id（唯一标识，避免重复）
programmes = []  # 所有节目数据


def fetch_epg_source(url):
    """抓取单个EPG源（支持普通XML和GZIP压缩XML）"""
    try:
        print(f"📥 开始抓取: {url}")
        start_time = datetime.now()
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()

        # 处理GZIP压缩文件
        if url.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                xml_content = f.read().decode("utf-8")
        else:
            xml_content = response.text

        # 解析XML根节点
        root = ET.fromstring(xml_content)
        parse_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ 成功抓取: {url} | 耗时: {parse_time:.2f}s")
        return root

    except Exception as e:
        print(f"❌ 抓取失败: {url} | 错误: {str(e)}")
        return None


def parse_epg(root, source_url):
    """解析EPG数据（无任何过滤，仅按channel_id去重）"""
    # 1. 合并所有频道（仅去重，不筛选）
    for channel in root.findall(".//channel"):
        channel_id = channel.get("id")
        if not channel_id:
            continue  # 跳过无ID的无效频道
        
        if channel_id not in channels:
            # 提取频道名称和URL（无默认过滤）
            display_name = channel.findtext(".//display-name", default="未知频道")
            channel_url = channel.findtext(".//url", default=source_url)
            channels[channel_id] = {
                "id": channel_id,
                "name": display_name,
                "url": channel_url
            }
            # 标记潍坊频道（方便确认是否抓取成功）
            if "潍坊" in display_name:
                print(f"📌 新增潍坊频道：{display_name}（ID：{channel_id}）")
            else:
                print(f"➕ 新增频道：{display_name}（ID：{channel_id}）")
        else:
            # 频道已存在，跳过重复
            display_name = channel.findtext(".//display-name", default="未知频道")
            print(f"🔄 频道已存在（去重）：{display_name}（ID：{channel_id}）")

    # 2. 合并所有节目（无任何过滤，仅关联有效频道）
    for programme in root.findall(".//programme"):
        channel_id = programme.get("channel")
        if channel_id in channels:
            # 提取节目核心信息（保留原始数据，不筛选）
            prog_data = {
                "channel_id": channel_id,
                "start": programme.get("start", ""),
                "stop": programme.get("stop", ""),
                "title": programme.findtext(".//title[@lang='zh']", default=programme.findtext(".//title", default="未知节目"))
            }
            programmes.append(prog_data)
            # 可选：打印节目示例（注释后可加快运行速度）
            # print(f"📺 节目：{channels[channel_id]['name']} - {prog_data['title']}（{prog_data['start']}）")


def generate_final_epg():
    """生成最终EPG文件（包含所有频道和节目，无任何过滤）"""
    # 创建XML根节点（符合XMLTV标准）
    tv = ET.Element("tv", {
        "source-info-url": "多源EPG合并（无过滤）",
        "source-info-name": "综合EPG源（完整数据）",
        "generator-info-name": "EPG自动合并工具",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })

    # 添加所有频道（无过滤，按ID顺序排列）
    for channel_id, chan_info in channels.items():
        chan_elem = ET.SubElement(tv, "channel", {"id": channel_id})
        ET.SubElement(chan_elem, "display-name").text = chan_info["name"]
        ET.SubElement(chan_elem, "url").text = chan_info["url"]

    # 添加所有节目（无过滤，保留原始时间和标题）
    for prog in programmes:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "stop": prog["stop"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]

    # 确保输出目录存在
    os.makedirs("output", exist_ok=True)
    # 格式化XML（便于阅读，去除多余空行）
    xml_str = ET.tostring(tv, encoding="utf-8", xml_declaration=True)
    from xml.dom import minidom
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="  ")
    xml_str = os.linesep.join([line for line in xml_str.splitlines() if line.strip()])  # 去除空行

    # 保存最终文件
    output_path = "output/final_epg_complete.xml"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)

    # 输出统计信息
    print("\n" + "="*60)
    print("🎉 EPG文件生成完成！")
    print(f"📊 统计信息：")
    print(f"   - 总频道数：{len(channels)} 个（含潍坊、国内、外国频道）")
    print(f"   - 总节目数：{len(programmes)} 个")
    print(f"   - 输出文件：{output_path}")
    print("="*60)


if __name__ == "__main__":
    print("="*60)
    print("🚀 EPG多源合并工具（无任何过滤版）")
    print("="*60 + "\n")
    start_total = datetime.now()

    # 1. 遍历所有EPG源，抓取并解析
    for source in EPG_SOURCES:
        print(f"\n{'='*40} 处理源：{source} {'='*40}")
        root = fetch_epg_source(source)
        if root:
            parse_epg(root, source)

    # 2. 生成最终完整EPG文件
    if channels and programmes:
        generate_final_epg()
    else:
        print("\n❌ 未获取到有效EPG数据，请检查源地址或网络连接！")

    # 输出总耗时
    total_time = (datetime.now() - start_total).total_seconds()
    print(f"\n⏱️  总运行时间：{total_time:.2f} 秒")
    print("="*60)
