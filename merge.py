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

# ================= 极速配置 =================
MAX_CONCURRENT_REQUESTS = 15  # 拉取并发
MAX_PARSE_WORKERS = 12        # 解析线程数拉满（关键）
PARSE_TIMEOUT = 120         # 单源解析超时2分钟
# ============================================

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

# ========== 带重试的拉取 ==========
async def fetch_epg_with_retry(url, session, semaphore):
    for attempt in range(1, 4):
        try:
            async with semaphore:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                async with session.get(url, timeout=15) as response:
                    response.raise_for_status()
                    if url.endswith('.gz'):
                        return gzip.decompress(await response.read()).decode('utf-8', errors='ignore')
                    return await response.text(encoding='utf-8')
        except Exception as e:
            if attempt == 3:
                print(f"❌ 最终失败 {url[:40]}...")
                return None
            await asyncio.sleep(1)
            print(f"⚠️  重试 {attempt}：{url[:40]}...")

# ========== 单源解析函数 ==========
def parse_single_epg(content):
    if not content:
        return {}, defaultdict(list)
    
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)
    now = datetime.now(TZ_UTC_PLUS_8)
    valid_start = now - timedelta(days=7)
    valid_end = now + timedelta(days=7)

    for channel in root.findall('channel'):
        cid = transform2_zh_hans(channel.get('id'))
        names = [transform2_zh_hans(n.text) for n in channel.findall('display-name') if n.text]
        if cid not in names:
            names.append(cid)
        channels[cid] = [[n, 'zh'] for n in names]

    for prog in root.findall('programme'):
        cid = transform2_zh_hans(prog.get('channel'))
        name = next((d[0] for d in channels.get(cid, [])), cid)
        if any(kw in name for kw in FOREIGN_KEYWORDS):
            continue

        try:
            start = datetime.strptime(re.sub(r'\s+', '', prog.get('start')), "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            stop = datetime.strptime(re.sub(r'\s+', '', prog.get('stop')), "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
        except Exception:
            continue

        if valid_start <= start <= valid_end and valid_start <= stop <= valid_end:
            p_elem = ET.Element('programme', attrib={"start": start.strftime("%Y%m%d%H%M%S %z"), "stop": stop.strftime("%Y%m%d%H%M%S %z"), "channel": cid})
            for title in prog.findall('title'):
                if title.text:
                    ET.SubElement(p_elem, 'title', lang='zh').text = transform2_zh_hans(title.text.strip())
            for desc in prog.findall('desc'):
                if desc.text:
                    ET.SubElement(p_elem, 'desc', lang='zh').text = transform2_zh_hans(desc.text.strip())
            programmes[cid].append(p_elem)
    
    return channels, programmes

# ========== 带超时的解析 ==========
def parse_epg_safe(content):
    try:
        return parse_single_epg(content)
    except Exception as e:
        print(f"❌ 解析超时或出错：{e}")
        return {}, defaultdict(list)

# ========== 写入与压缩 ==========
def write_to_xml(all_channels, all_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    root = ET.Element('tv', attrib={'date': datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")})
    for cid in all_channels:
        ch_elem = ET.SubElement(root, 'channel', attrib={"id": cid})
        for name, lang in all_channels[cid]:
            ET.SubElement(ch_elem, 'display-name', lang=lang).text = name
    for cid in all_programmes:
        for prog in all_programmes[cid]:
            root.append(prog)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(minidom.parseString(ET.tostring(root)).toprettyxml(indent='\t'))

def force_compress_gz(in_path, out_path):
    with open(in_path, 'rb') as f_in, gzip.open(out_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    print(f"✅ 生成：{out_path}")

# ========== 主入口 ==========
async def main():
    urls = []
    if os.path.exists('config.txt'):
        with open('config.txt', 'r', encoding='utf-8') as f:
            urls = [l.strip() for l in f if l.strip() and not l.startswith('#')]
    print(f"📥 读取到 {len(urls)} 个源")

    # 1. 拉取
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)) as session:
        epg_contents = await tqdm_asyncio.gather(*[fetch_epg_with_retry(url, session, semaphore) for url in urls], desc="📥 拉取EPG")
    
    # 过滤空内容
    valid_contents = [c for c in epg_contents if c]
    print(f"✅ 有效内容：{len(valid_contents)}/{len(urls)}")

    # 2. 多线程解析（最大12线程）
    all_channels = {}
    all_programmes = defaultdict(list)
    
    with ThreadPoolExecutor(max_workers=MAX_PARSE_WORKERS) as executor:
        futures = [executor.submit(parse_epg_safe, c) for c in valid_contents]
        results = []
        for future in tqdm_asyncio.as_completed(futures, desc="🔗 解析EPG"):
            try:
                res = future.result(timeout=PARSE_TIMEOUT)
                results.append(res)
            except Exception as e:
                print(f"❌ 解析超时：{e}")

    # 3. 合并去重
    for channels, progs in results:
        for cid in channels:
            if cid not in all_channels:
                all_channels[cid] = channels[cid]
            # 合并别名
            for name, lang in channels[cid]:
                if name not in [n[0] for n in all_channels[cid]]:
                    all_channels[cid].append([name, lang])
        for cid in progs:
            if len(all_programmes[cid]) < len(progs[cid]):
                all_programmes[cid] = progs[cid]

    # 4. 写入压缩
    xml_path = 'output/epg.xml'
    write_to_xml(all_channels, all_programmes, xml_path)
    force_compress_gz(xml_path, 'output/epg.gz')
    print("\n🎉 多线程合并完成！")

if __name__ == '__main__':
    asyncio.run(main())
