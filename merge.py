import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from aiohttp import TCPConnector
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from concurrent.futures import ThreadPoolExecutor

# ================= 配置区 =================
MAX_CONCURRENT_REQUESTS = 20    # 最大并发请求
MAX_WORKERS = 8                  # 解析线程数
RETRY_COUNT = 3                  # 重试次数
RETRY_DELAY = 2                  # 重试间隔秒
# ==========================================

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
FOREIGN_KEYWORDS = ["BBC", "CNN", "FOX", "HBO", "Netflix", "欧美", "美国", "英国", "法国", "德国"]

# ============ EPG 预处理规则 ============
def _adjust_timezone(programme, from_offset, to_offset):
    for attr in ('start', 'stop'):
        val = programme.get(attr, '')
        if from_offset in val:
            programme.set(attr, val.replace(from_offset, to_offset))

def _make_tz_rule(channel_keyword, from_offset, to_offset):
    def rule(channel_name, programme):
        if channel_keyword in channel_name:
            _adjust_timezone(programme, from_offset, to_offset)
    return rule

PREPROCESS_RULES = [
    ("kuke31/xmlgz", _make_tz_rule("天映经典", "+0800", "+0900")),
]

def preprocess_epg(url, epg_content):
    matched_rules = [rule for keyword, rule in PREPROCESS_RULES if keyword in url]
    if not matched_rules:
        return epg_content

    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError:
        return epg_content

    channel_names = {}
    for channel in root.findall('channel'):
        cid = channel.get('id', '')
        names = [n.text for n in channel.findall('display-name') if n.text]
        channel_names[cid] = ' '.join(names)

    for programme in root.findall('programme'):
        cid = programme.get('channel', '')
        name_str = channel_names.get(cid, cid)
        for rule in matched_rules:
            rule(name_str, programme)

    return ET.tostring(root, encoding='unicode')
# ============ 预处理结束 ============

def transform2_zh_hans(string):
    cc = OpenCC("t2s")
    return cc.convert(string)

# ========== 带重试机制的拉取函数 ==========
async def fetch_epg_with_retry(url, session, semaphore):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            async with semaphore:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
                }
                async with session.get(url, timeout=15) as response:
                    response.raise_for_status()
                    if url.endswith('.gz'):
                        compressed_data = await response.read()
                        return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                    else:
                        return await response.text(encoding='utf-8')
        except Exception as e:
            if attempt == RETRY_COUNT:
                print(f"❌ 最终拉取失败 {url[:50]}...：{e}")
                return None
            print(f"⚠️  拉取失败（尝试 {attempt}/{RETRY_COUNT}），{RETRY_DELAY}秒后重试：{url[:50]}")
            await asyncio.sleep(RETRY_DELAY)

# ========== 线程池解析（CPU 密集） ==========
def parse_epg_task(epg_content):
    if not epg_content:
        return {}, defaultdict(list)
    
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"XML解析错误：{e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)
    now = datetime.now(TZ_UTC_PLUS_8)
    valid_start = now - timedelta(days=7)
    valid_end = now + timedelta(days=7)
    valid_channels = set()

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = t_name[:-2] if t_name.endswith('高清') else t_name
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if channel_id and channel_id not in [c[0] for c in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        channel_name = next((d[0] for d in channels.get(channel_id, [])), channel_id)
        if any(kw in channel_name for kw in FOREIGN_KEYWORDS):
            continue

        try:
            start_str = re.sub(r'\s+', '', programme.get('start'))
            stop_str = re.sub(r'\s+', '', programme.get('stop'))
            channel_start = datetime.strptime(start_str, "%Y%m%d%H%M%S%z")
            channel_stop = datetime.strptime(stop_str, "%Y%m%d%H%M%S%z")
            channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
            channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)
        except Exception as e:
            continue

        if valid_start <= channel_start <= valid_end and valid_start <= channel_stop <= valid_end:
            valid_channels.add(channel_id)
            channel_elem = ET.Element(
                'programme',
                attrib={
                    "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                    "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
                    "channel": channel_id
                }
            )
            for title in programme.findall('title'):
                if title.text is None: continue
                langattr = title.get('lang') or 'zh'
                channel_title = transform2_zh_hans(title.text.strip())
                title_elem = ET.SubElement(channel_elem, 'title', lang=langattr)
                title_elem.text = channel_title
            for desc in programme.findall('desc'):
                if desc.text is None: continue
                langattr = desc.get('lang') or 'zh'
                channel_desc = transform2_zh_hans(desc.text.strip())
                desc_elem = ET.SubElement(channel_elem, 'desc', lang=langattr)
                desc_elem.text = channel_desc
            programmes[channel_id].append(channel_elem)

    channels = {k: v for k, v in channels.items() if k in valid_channels}
    return channels, programmes

# ========== 写入XML ==========
def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    root = ET.Element('tv', attrib={'date': datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")})
    for cid in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": cid})
        for name, lang in channels_names.get(cid, []):
            ET.SubElement(channel_elem, 'display-name', lang=lang).text = name
    for cid in channels_id:
        for prog in programmes.get(cid, []):
            root.append(prog)
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))

# ========== 压缩为.gz ==========
def force_compress_gz(input_filename, output_filename):
    if os.path.exists(output_filename):
        try:
            os.remove(output_filename)
        except Exception:
            pass
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"✅ 生成新EPG：{output_filename}")

# ========== 读取配置 ==========
def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        print("❌ 未找到config.txt")
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    print(f"📥 读取到 {len(urls)} 个EPG源")
    return urls

# ========== 主函数 ==========
async def main():
    urls = get_urls()
    if not urls:
        return

    # 1. 异步高并发拉取 + 自动重试3次
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        tasks = [fetch_epg_with_retry(url, session, semaphore) for url in urls]
        epg_contents = await tqdm_asyncio.gather(*tasks, desc="📥 多线程拉取EPG（重试3次）")

    # 2. 线程池解析
    parsed_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        parse_tasks = [executor.submit(parse_epg_task, content) for content in epg_contents if content]
        for future in tqdm_asyncio.as_completed(parse_tasks, desc="🔗 多线程解析EPG"):
            parsed_results.append(future.result())

    # 3. 合并结果
    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)

    for channels, programmes in parsed_results:
        for cid, display_names in channels.items():
            if len(programmes[cid]) == 0:
                continue
            map_id = next((all_channels_map[dn[0]] for dn in display_names if dn[0] in all_channels_map), cid)
            if map_id not in all_channel_id:
                all_channel_id.add(map_id)
                all_channel_names[map_id] = display_names
                all_programmes[map_id] = programmes[cid]
                for dn, lang in display_names:
                    all_channels_map[dn] = map_id
            else:
                if len(all_programmes[map_id]) < len(programmes[cid]):
                    all_programmes[map_id] = programmes[cid]
                for dn, lang in display_names:
                    if dn not in all_channels_map:
                        all_channel_names[map_id].append([dn, lang])
                        all_channels_map[dn] = map_id

    # 4. 写入并压缩
    xml_file = 'output/epg.xml'
    write_to_xml(all_channel_id, all_channel_names, all_programmes, xml_file)
    force_compress_gz(xml_file, 'output/epg.gz')
    print("\n🎉 多线程EPG合并完成（含3次重试）！")

if __name__ == '__main__':
    asyncio.run(main())
