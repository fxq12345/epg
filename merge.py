import requests
import gzip
from lxml import etree
import os

def read_config(config_file='config.txt'):
    """从配置文件读取EPG源链接"""
    sources = []
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 跳过注释和空行
                if line and not line.startswith('#'):
                    sources.append(line)
        print(f"✅ 从 {config_file} 读取到 {len(sources)} 个EPG源")
        return sources
    except Exception as e:
        print(f"❌ 读取配置文件出错: {e}")
        return []

def fetch_and_parse_epg(url):
    """抓取并解析单个EPG源"""
    try:
        print(f"正在尝试抓取: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # 处理gzip压缩文件
        if url.endswith('.gz'):
            try:
                data = gzip.decompress(response.content)
            except gzip.BadGzipFile:
                print(f"⚠️  {url} 不是有效的gzip文件，跳过该源")
                return None
        else:
            data = response.content

        # 解析XML
        root = etree.fromstring(data)
        return root

    except Exception as e:
        print(f"❌ 处理 {url} 时出错: {e}，跳过该源")
        return None

def merge_epg_sources(sources):
    """合并多个EPG源"""
    # 创建基础XML结构
    tv = etree.Element("tv", {"generator-info-name": "EPG Merger"})
    channel_ids = set()
    programme_ids = set()

    for source in sources:
        if source is None:
            continue

        # 合并频道信息
        for channel in source.findall(".//channel"):
            channel_id = channel.get("id")
            if channel_id not in channel_ids:
                channel_ids.add(channel_id)
                tv.append(channel)

        # 合并节目信息
        for programme in source.findall(".//programme"):
            prog_id = f"{programme.get('channel')}-{programme.get('start')}-{programme.get('stop')}"
            if prog_id not in programme_ids:
                programme_ids.add(prog_id)
                tv.append(programme)

    return tv

def main():
    print("=== 开始EPG合并 ===")

    # 1. 读取配置文件
    EPG_SOURCES = read_config()
    if not EPG_SOURCES:
        print("❌ 没有可用的EPG源，退出程序")
        return

    # 2. 抓取所有EPG源
    epg_sources = [fetch_and_parse_epg(url) for url in EPG_SOURCES]
    epg_sources = [src for src in epg_sources if src is not None]

    if not epg_sources:
        print("⚠️  没有有效的EPG源，生成基础EPG文件")
        tv = etree.Element("tv", {"generator-info-name": "EPG Merger"})
    else:
        # 合并EPG源
        tv = merge_epg_sources(epg_sources)
        print(f"✅ 成功合并 {len(epg_sources)} 个EPG源")
        print(f"📺 共 {len(tv.findall('.//channel'))} 个频道，{len(tv.findall('.//programme'))} 个节目")

    # 3. 生成最终XML
    xml_str = etree.tostring(tv, encoding='utf-8', pretty_print=True, xml_declaration=True).decode('utf-8')

    # 4. 确保output目录存在
    os.makedirs('output', exist_ok=True)

    # 5. 保存为未压缩的XML文件
    with open('output/epg.xml', 'w', encoding='utf-8') as f:
        f.write(xml_str)
    print("✅ EPG文件已保存为 output/epg.xml")

    # 6. 保存为gzip压缩文件
    with gzip.open('output/epg.gz', 'wb') as f:
        f.write(xml_str.encode('utf-8'))
    print("✅ 压缩版EPG文件已保存为 output/epg.gz")

if __name__ == "__main__":
    main()
