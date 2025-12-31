import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

def transform2_zh_hans(string):
    """繁体中文转简体中文"""
    cc = OpenCC("t2s")
    new_str = cc.convert(string)
    return new_str

async def fetch_epg(url):
    """异步获取EPG数据"""
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    print(f"{url} HTTP请求失败，状态码：{response.status}")
                    return None
                
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None

def parse_epg(epg_content):
    """解析EPG XML数据"""
    if not epg_content:
        return {}, defaultdict(list)
    
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"XML解析错误: {e}")
        print(f"问题内容前500字符: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    # 解析频道信息
    for channel in root.findall('channel'):
        channel_id = channel.get('id')
        if channel_id:
            channel_id = transform2_zh_hans(channel_id)
        else:
            continue  # 跳过没有ID的频道
        
        channel_display_names = []
        for name in channel.findall('display-name'):
            if name.text is not None:
                display_name = name.text.strip()
                if display_name:  # 跳过空字符串
                    display_name = transform2_zh_hans(display_name)
                    lang = name.get('lang', 'zh')
                    channel_display_names.append([display_name, lang])
        
        if channel_display_names:
            channels[channel_id] = channel_display_names

    # 解析节目信息
    for programme in root.findall('programme'):
        channel_id = programme.get('channel')
        if not channel_id:
            continue  # 跳过没有频道ID的节目
        
        channel_id = transform2_zh_hans(channel_id)
        
        # 解析开始和结束时间
        start_time_str = programme.get('start')
        stop_time_str = programme.get('stop')
        if not start_time_str or not stop_time_str:
            continue  # 跳过没有时间的节目
        
        try:
            channel_start = datetime.strptime(
                re.sub(r'\s+', '', start_time_str), "%Y%m%d%H%M%S%z")
            channel_stop = datetime.strptime(
                re.sub(r'\s+', '', stop_time_str), "%Y%m%d%H%M%S%z")
            channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
            channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)
        except ValueError as e:
            print(f"时间解析错误: {e}, start: {start_time_str}, stop: {stop_time_str}")
            continue
        
        # 创建新的programme元素
        channel_elem = ET.Element('programme', attrib={
            "channel": channel_id,
            "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
            "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
        })
        
        # 处理标题
        for title in programme.findall('title'):
            if title.text is not None:
                channel_title = title.text.strip()
                if channel_title:  # 跳过空标题
                    langattr = title.get('lang', 'zh')
                    if langattr in ['zh', 'zh_TW', 'zh_HK']:
                        channel_title = transform2_zh_hans(channel_title)
                    
                    title_elem = ET.SubElement(channel_elem, 'title')
                    title_elem.text = channel_title
                    if langattr:
                        title_elem.set('lang', langattr)
        
        # 处理描述
        for desc in programme.findall('desc'):
            if desc.text is not None:
                channel_desc = desc.text.strip()
                if channel_desc:  # 跳过空描述
                    langattr = desc.get('lang', 'zh')
                    if langattr in ['zh', 'zh_TW', 'zh_HK']:
                        channel_desc = transform2_zh_hans(channel_desc)
                    
                    desc_elem = ET.SubElement(channel_elem, 'desc')
                    desc_elem.text = channel_desc
                    if langattr:
                        desc_elem.set('lang', langattr)
        
        # 处理其他元素（category, icon等）
        for elem in programme:
            if elem.tag not in ['title', 'desc']:
                new_elem = ET.SubElement(channel_elem, elem.tag, attrib=elem.attrib)
                if elem.text is not None:
                    new_elem.text = elem.text
        
        programmes[channel_id].append(channel_elem)

    return channels, programmes

async def process_epg_sources(epg_urls):
    """处理所有EPG源"""
    all_channels = {}
    all_programmes = defaultdict(list)
    
    print("Fetching EPG data...")
    epg_contents = []
    
    # 异步获取所有EPG数据
    async def fetch_with_progress(url):
        content = await fetch_epg(url)
        return url, content
    
    tasks = [fetch_with_progress(url) for url in epg_urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    # 处理每个EPG源
    for i, (url, epg_content) in enumerate(results, 1):
        print(f"Processing EPG source... {i}/{len(epg_urls)}")
        
        if epg_content is None:
            print(f"跳过 {url}，获取数据失败")
            continue
        
        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        
        # 合并频道信息
        for channel_id, display_names in channels.items():
            if channel_id not in all_channels:
                all_channels[channel_id] = display_names
            else:
                # 添加不重复的显示名称
                existing_names = {name[0] for name in all_channels[channel_id]}
                for display_name, lang in display_names:
                    if display_name not in existing_names:
                        all_channels[channel_id].append([display_name, lang])
                        existing_names.add(display_name)
        
        # 合并节目信息
        for channel_id, prog_list in programmes.items():
            all_programmes[channel_id].extend(prog_list)
    
    print("Finished parsing all EPG sources")
    return all_channels, all_programmes

def merge_epg(channels, programmes):
    """合并EPG数据并生成最终XML"""
    # 创建根元素
    tv = ET.Element('tv')
    
    # 添加频道信息
    for channel_id, display_names in channels.items():
        channel_elem = ET.SubElement(tv, 'channel', id=channel_id)
        for display_name, lang in display_names:
            display_elem = ET.SubElement(channel_elem, 'display-name')
            display_elem.text = display_name
            if lang:
                display_elem.set('lang', lang)
    
    # 添加节目信息
    for channel_id, prog_list in programmes.items():
        for programme in prog_list:
            tv.append(programme)
    
    # 美化XML输出
    rough_string = ET.tostring(tv, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ", encoding='utf-8')
    
    return pretty_xml

async def main():
    """主函数"""
    # 从配置文件读取EPG源
    config_file = "config.txt"
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            epg_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    else:
        # 使用默认EPG源
        epg_urls = [
            "https://epg.112114.xyz/pp.xml",
            "https://epg.51zmt.top:8001/e.xml",
            "http://e.env.cc/all.xml",
            "https://epg.pw/xmltv/epg_CN.xml",
            "https://epg.pw/xmltv/epg_HK.xml",
            "https://epg.pw/xmltv/epg_TW.xml"
        ]
    
    if not epg_urls:
        print("没有可用的EPG源")
        return
    
    print(f"找到 {len(epg_urls)} 个EPG源")
    
    # 处理EPG源
    all_channels, all_programmes = await process_epg_sources(epg_urls)
    
    if not all_channels:
        print("没有成功解析到任何频道信息")
        return
    
    print(f"共解析到 {len(all_channels)} 个频道")
    total_programmes = sum(len(progs) for progs in all_programmes.values())
    print(f"共解析到 {total_programmes} 个节目")
    
    # 合并并生成最终EPG
    print("Merging EPG data...")
    merged_epg = merge_epg(all_channels, all_programmes)
    
    # 保存到文件
    output_file = "merged_epg.xml"
    with open(output_file, 'wb') as f:
        f.write(merged_epg)
    
    print(f"EPG数据已保存到 {output_file}")
    
    # 压缩文件（可选）
    compressed_file = "merged_epg.xml.gz"
    with open(output_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"压缩文件已保存到 {compressed_file}")
    print("EPG合并完成！")

if __name__ == "__main__":
    asyncio.run(main())
