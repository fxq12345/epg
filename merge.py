import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os  # 全局导入

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

def transform2_zh_hans(string):
    if not string:
        return string
    cc = OpenCC("t2s")
    return cc.convert(string)

async def fetch_epg(url):
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                if url.endswith('.gz'):
                    data = await response.read()
                    return gzip.decompress(data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except Exception:
        return None

def parse_epg(epg_content):
    if not epg_content:
        return {}, defaultdict(list)
    
    try:
        root = ET.fromstring(epg_content)
    except:
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = channel.get('id')
        if not channel_id:
            continue
        channel_id = transform2_zh_hans(channel_id)
        names = []
        for name in channel.findall('display-name'):
            if name.text:
                display_name = transform2_zh_hans(name.text.strip())
                if display_name:
                    lang = name.get('lang', 'zh')
                    names.append([display_name, lang])
        if names:
            channels[channel_id] = names

    for programme in root.findall('programme'):
        channel_id = programme.get('channel')
        if not channel_id:
            continue
        channel_id = transform2_zh_hans(channel_id)
        
        start = programme.get('start')
        stop = programme.get('stop')
        if not start or not stop:
            continue
        
        try:
            start = re.sub(r'\s+', '', start)
            stop = re.sub(r'\s+', '', stop)
            start_dt = datetime.strptime(start, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            stop_dt = datetime.strptime(stop, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
        except:
            continue
        
        prog_elem = ET.Element('programme', attrib={
            "channel": channel_id,
            "start": start_dt.strftime("%Y%m%d%H%M%S %z"),
            "stop": stop_dt.strftime("%Y%m%d%H%M%S %z")
        })
        
        for title in programme.findall('title'):
            if title.text:
                text = title.text.strip()
                if text:
                    lang = title.get('lang', 'zh')
                    if lang in ['zh', 'zh_TW', 'zh_HK']:
                        text = transform2_zh_hans(text)
                    elem = ET.SubElement(prog_elem, 'title')
                    elem.text = text
                    elem.set('lang', lang)
        
        for desc in programme.findall('desc'):
            if desc.text:
                text = desc.text.strip()
                if text:
                    lang = desc.get('lang', 'zh')
                    if lang in ['zh', 'zh_TW', 'zh_HK']:
                        text = transform2_zh_hans(text)
                    elem = ET.SubElement(prog_elem, 'desc')
                    elem.text = text
                    elem.set('lang', lang)
        
        programmes[channel_id].append(prog_elem)

    return channels, programmes

async def main():
    print("开始EPG合并...")
    
    # 读取配置文件 - 使用全局os模块
    config_file = "config.txt"
    if os.path.exists(config_file):  # 这里不会有UnboundLocalError
        with open(config_file, 'r') as f:
            epg_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    else:
        epg_urls = [
            "https://epg.27481716.xyz/epg.xml",
            "https://e.erw.cc/all.xml",
            "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
            "http://epg.51zmt.top:8000/e.xml",
            "https://raw.githubusercontent.com/fanmingming/live/main/e.xml"
        ]
    
    print(f"处理 {len(epg_urls)} 个EPG源")
    
    # 获取所有EPG数据
    tasks = [fetch_epg(url) for url in epg_urls]
    results = await asyncio.gather(*tasks)
    
    all_channels = {}
    all_programmes = defaultdict(list)
    
    for i, content in enumerate(results):
        if not content:
            continue
        print(f"解析源 {i+1}/{len(epg_urls)}")
        channels, programmes = parse_epg(content)
        
        for cid, names in channels.items():
            if cid not in all_channels:
                all_channels[cid] = names
        
        for cid, progs in programmes.items():
            all_programmes[cid].extend(progs)
    
    if not all_channels:
        print("没有数据")
        return
    
    # 生成XML
    tv = ET.Element('tv')
    for cid, names in all_channels.items():
        chan = ET.SubElement(tv, 'channel', id=cid)
        for name, lang in names:
            elem = ET.SubElement(chan, 'display-name')
            elem.text = name
            if lang:
                elem.set('lang', lang)
    
    for progs in all_programmes.values():
        for prog in progs:
            tv.append(prog)
    
    # 保存文件
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 保存XML
    xml_str = ET.tostring(tv, encoding='utf-8')
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ", encoding='utf-8')
    
    xml_file = os.path.join(output_dir, "epg.xml")
    with open(xml_file, 'wb') as f:
        f.write(xml_pretty)
    
    # 保存压缩文件
    gz_file = os.path.join(output_dir, "epg.gz")
    with open(xml_file, 'rb') as f_in:
        with gzip.open(gz_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"完成！文件已保存到 {xml_file} 和 {gz_file}")

if __name__ == "__main__":
    asyncio.run(main())
