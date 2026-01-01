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
import os  # 这里导入os
from tqdm import tqdm

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

def transform2_zh_hans(string):
    """繁体中文转简体中文"""
    if not string:
        return string
    cc = OpenCC("t2s")
    new_str = cc.convert(string)
    return new_str

async def fetch_epg(url):
    """异步获取EPG数据"""
    timeout = aiohttp.ClientTimeout(total=60)  # 60秒超时
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            print(f"正在获取: {url}")
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"{url} HTTP请求失败，状态码：{response.status}")
                    return None
                
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    print(f"{url} 获取成功 (gzip压缩)，大小: {len(compressed_data)} 字节")
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    content = await response.text(encoding='utf-8')
                    print(f"{url} 获取成功，大小: {len(content)} 字符")
                    return content
    except Exception as e:
        print(f"{url} 请求错误: {type(e).__name__}: {e}")
    return None

def safe_parse_time(time_str):
    """安全解析时间字符串"""
    if not time_str:
        return None
    
    try:
        # 清理空格
        time_str = re.sub(r'\s+', '', time_str)
        
        # 尝试解析
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
        
        # 转换到北京时间
        dt = dt.astimezone(TZ_UTC_PLUS_8)
        
        return dt
    except Exception as e:
        # 静默失败，不打印日志避免刷屏
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
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    # 解析频道信息
    channel_count = 0
    for channel in root.findall('channel'):
        channel_count += 1
        channel_id = channel.get('id')
        if not channel_id:
            continue
        
        channel_id = transform2_zh_hans(channel_id)
        channel_display_names = []
        
        for name in channel.findall('display-name'):
            if name.text is not None:
                display_name = name.text.strip()
                if display_name:
                    display_name = transform2_zh_hans(display_name)
                    lang = name.get('lang', 'zh')
                    channel_display_names.append([display_name, lang])
        
        if channel_display_names:
            channels[channel_id] = channel_display_names

    print(f"解析到 {len(channels)} 个频道")

    # 解析节目信息
    programme_count = 0
    success_count = 0
    
    for programme in root.findall('programme'):
        programme_count += 1
        if programme_count % 10000 == 0:
            print(f"正在解析第 {programme_count} 个节目...")
        
        channel_id = programme.get('channel')
        if not channel_id:
            continue
        
        channel_id = transform2_zh_hans(channel_id)
        
        # 解析时间
        start_dt = safe_parse_time(programme.get('start'))
        stop_dt = safe_parse_time(programme.get('stop'))
        
        if not start_dt or not stop_dt:
            continue
        
        success_count += 1
        
        # 创建新的programme元素
        channel_elem = ET.Element('programme', attrib={
            "channel": channel_id,
            "start": start_dt.strftime("%Y%m%d%H%M%S %z"),
            "stop": stop_dt.strftime("%Y%m%d%H%M%S %z")
        })
        
        # 处理标题
        for title in programme.findall('title'):
            if title.text is not None:
                channel_title = title.text.strip()
                if channel_title:
                    langattr = title.get('lang', 'zh')
                    if langattr in ['zh', 'zh_TW', 'zh_HK']:
                        channel_title = transform2_zh_hans(channel_title)
                    
                    title_elem = ET.SubElement(channel_elem, 'title')
                    title_elem.text = channel_title
                    title_elem.set('lang', langattr)
        
        # 处理描述
        for desc in programme.findall('desc'):
            if desc.text is not None:
                channel_desc = desc.text.strip()
                if channel_desc:
                    langattr = desc.get('lang', 'zh')
                    if langattr in ['zh', 'zh_TW', 'zh_HK']:
                        channel_desc = transform2_zh_hans(channel_desc)
                    
                    desc_elem = ET.SubElement(channel_elem, 'desc')
                    desc_elem.text = channel_desc
                    desc_elem.set('lang', langattr)
        
        # 处理其他元素
        for elem in programme:
            if elem.tag not in ['title', 'desc']:
                new_elem = ET.SubElement(channel_elem, elem.tag, attrib=elem.attrib)
                if elem.text is not None:
                    new_elem.text = elem.text
        
        programmes[channel_id].append(channel_elem)

    print(f"节目解析完成: 共 {programme_count} 个节目，成功 {success_count} 个")
    return channels, programmes

async def process_epg_sources(epg_urls):
    """处理所有EPG源"""
    print(f"开始处理 {len(epg_urls)} 个EPG源")
    
    all_channels = {}
    all_programmes = defaultdict(list)
    
    # 异步获取所有EPG数据
    tasks = []
    for url in epg_urls:
        task = asyncio.create_task(fetch_epg(url))
        tasks.append(task)
    
    # 使用tqdm显示进度
    results = []
    for task in tqdm_asyncio.as_completed(tasks, desc="获取EPG源", total=len(tasks)):
        result = await task
        results.append(result)
    
    # 处理每个EPG源
    for i, epg_content in enumerate(results):
        if epg_content is None:
            print(f"EPG源 {i+1} 获取失败，跳过")
            continue
        
        print(f"处理EPG源 {i+1}/{len(epg_urls)}")
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
    
    return all_channels, all_programmes

def merge_epg(channels, programmes):
    """合并EPG数据并生成最终XML"""
    print("开始合并EPG数据...")
    
    # 创建根元素
    tv = ET.Element('tv')
    
    # 添加生成时间注释
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    comment = ET.Comment(f' Generated by myEPG at {gen_time} ')
    tv.insert(0, comment)
    
    # 添加频道信息
    print(f"添加 {len(channels)} 个频道...")
    for channel_id, display_names in channels.items():
        channel_elem = ET.SubElement(tv, 'channel', id=channel_id)
        for display_name, lang in display_names:
            display_elem = ET.SubElement(channel_elem, 'display-name')
            display_elem.text = display_name
            if lang:
                display_elem.set('lang', lang)
    
    # 添加节目信息
    total_programmes = sum(len(p) for p in programmes.values())
    print(f"添加 {total_programmes} 个节目...")
    for channel_id, prog_list in programmes.items():
        for programme in prog_list:
            tv.append(programme)
    
    # 美化XML输出
    print("生成XML文件...")
    rough_string = ET.tostring(tv, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ", encoding='utf-8')
    
    return pretty_xml

async def main():
    """主函数"""
    print("=== EPG合并程序开始 ===")
    
    # 从配置文件读取EPG源
    config_file = "config.txt"
    if os.path.exists(config_file):  # 这里使用全局的os模块
        with open(config_file, 'r', encoding='utf-8') as f:
            epg_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    else:
        # 使用默认EPG源
        epg_urls = [
            "https://epg.27481716.xyz/epg.xml",
            "https://e.erw.cc/all.xml",
            "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
            "http://epg.51zmt.top:8000/e.xml",
            "https://raw.githubusercontent.com/fanmingming/live/main/e.xml"
        ]
    
    print(f"使用 {len(epg_urls)} 个EPG源:")
    for url in epg_urls:
        print(f"  - {url}")
    
    # 处理EPG源
    all_channels, all_programmes = await process_epg_sources(epg_urls)
    
    if not all_channels:
        print("错误：没有成功解析到任何频道信息")
        return
    
    print(f"\n解析结果统计:")
    print(f"  频道数量: {len(all_channels)}")
    total_programmes = sum(len(p) for p in all_programmes.values())
    print(f"  节目数量: {total_programmes}")
    
    # 合并并生成最终EPG
    merged_epg = merge_epg(all_channels, all_programmes)
    
    # 保存到文件
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    output_file = os.path.join(output_dir, "epg.xml")
    with open(output_file, 'wb') as f:
        f.write(merged_epg)
    
    print(f"\n✅ EPG数据已保存到 {output_file}")
    
    # 压缩文件
    compressed_file = os.path.join(output_dir, "epg.gz")
    with open(output_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"✅ 压缩文件已保存到 {compressed_file}")
    
    # 显示文件大小
    xml_size = os.path.getsize(output_file) / 1024 / 1024
    gz_size = os.path.getsize(compressed_file) / 1024 / 1024
    print(f"📊 文件大小: epg.xml: {xml_size:.2f} MB, epg.gz: {gz_size:.2f} MB")
    
    print("\n🎉 EPG合并完成！")

if __name__ == "__main__":
    # 设置事件循环策略，避免在GitHub Actions中出错
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
