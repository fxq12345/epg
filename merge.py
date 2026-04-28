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
# 过滤国际台（可选）
FOREIGN_KEYWORDS = ["BBC", "CNN", "FOX", "HBO", "Netflix", "欧美", "美国", "英国", "法国", "德国"]

# ============ EPG 源预处理规则 ============
def _adjust_timezone(programme, from_offset, to_offset):
    """将 programme 节点的 start/stop 时区替换"""
    for attr in ('start', 'stop'):
        val = programme.get(attr, '')
        if from_offset in val:
            programme.set(attr, val.replace(from_offset, to_offset))

def _make_tz_rule(channel_keyword, from_offset, to_offset):
    """生成时区调整规则"""
    def rule(channel_name, programme):
        if channel_keyword in channel_name:
            _adjust_timezone(programme, from_offset, to_offset)
    return rule

# 预处理规则：天映经典+08→+09（延迟1小时）
PREPROCESS_RULES = [
    ("kuke31/xmlgz", _make_tz_rule("天映经典", "+0800", "+0900")),
]

def preprocess_epg(url, epg_content):
    """预处理EPG（时区矫正）"""
    matched_rules = [rule for keyword, rule in PREPROCESS_RULES if keyword in url]
    if not matched_rules:
        return epg_content

    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError:
        return epg_content

    # 建立频道ID->名称映射
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
# ============ 预处理规则结束 ============

def transform2_zh_hans(string):
    """繁体转简体"""
    cc = OpenCC("t2s")
    return cc.convert(string)

async def fetch_epg(url):
    """异步拉取EPG（支持gz压缩）"""
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except Exception as e:
        print(f"❌ 拉取失败 {url[:50]}...：{e}")
    return None

def process_display_name(display_name):
    """清洗频道名（去高清后缀）"""
    return display_name[:-2] if display_name.endswith('高清') else display_name

def parse_epg(epg_content):
    """解析EPG（14天范围+时区矫正+过滤国际台）"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"XML解析错误：{e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)
    # 14天时间范围
    now = datetime.now(TZ_UTC_PLUS_8)
    valid_start = now - timedelta(days=7)
    valid_end = now + timedelta(days=7)
    valid_channels = set()

    # 先解析所有频道
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if channel_id and channel_id not in [c[0] for c in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    # 解析节目并过滤14天+国际台
    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        # 过滤国际台
        channel_name = next((d[0] for d in channels.get(channel_id, [])), channel_id)
        if any(kw in channel_name for kw in FOREIGN_KEYWORDS):
            continue

        # 解析时间（自动矫正时区）
        try:
            start_str = re.sub(r'\s+', '', programme.get('start'))
            stop_str = re.sub(r'\s+', '', programme.get('stop'))
            channel_start = datetime.strptime(start_str, "%Y%m%d%H%M%S%z")
            channel_stop = datetime.strptime(stop_str, "%Y%m%d%H%M%S%z")
            # 转本地UTC+8
            channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
            channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)
        except Exception as e:
            print(f"时间解析失败：{channel_id} | {e}")
            continue

        # 保留14天内节目
        if valid_start <= channel_start <= valid_end and valid_start <= channel_stop <= valid_end:
            valid_channels.add(channel_id)
            # 重建节目节点（矫正时间）
            channel_elem = ET.Element(
                'programme',
                attrib={
                    "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                    "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
                    "channel": channel_id
                }
            )
            # 复制标题/描述
            for title in programme.findall('title'):
                if title.text is None:
                    continue
                langattr = title.get('lang') or 'zh'
                channel_title = transform2_zh_hans(title.text.strip())
                title_elem = ET.SubElement(channel_elem, 'title', lang=langattr)
                title_elem.text = channel_title
            for desc in programme.findall('desc'):
                if desc.text is None:
                    continue
                langattr = desc.get('lang') or 'zh'
                channel_desc = transform2_zh_hans(desc.text.strip())
                desc_elem = ET.SubElement(channel_elem, 'desc', lang=langattr)
                desc_elem.text = channel_desc
            programmes[channel_id].append(channel_elem)

    # 只保留有节目的频道
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    return channels, programmes

def write_to_xml(channels_id, channels_names, programmes, filename):
    """写入合并后的XML"""
    if not os.path.exists('output'):
        os.makedirs('output')
    root = ET.Element('tv', attrib={'date': datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")})
    # 写入频道
    for cid in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": cid})
        for name, lang in channels_names.get(cid, []):
            ET.SubElement(channel_elem, 'display-name', lang=lang).text = name
    # 写入节目
    for cid in channels_id:
        for prog in programmes.get(cid, []):
            root.append(prog)
    # 美化XML
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))

def force_compress_gz(input_filename, output_filename):
    """强制压缩并覆盖旧文件"""
    if os.path.exists(output_filename):
        try:
            os.remove(output_filename)
            print(f"🗑️  强制删除旧EPG：{output_filename}")
        except Exception as e:
            print(f"❌ 删除失败：{e}")
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"✅ 生成新EPG：{output_filename}")

def get_urls():
    """读取config.txt中的EPG源"""
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

async def main():
    urls = get_urls()
    if not urls:
        return
    # 异步拉取所有EPG
    tasks = [fetch_epg(url) for url in urls]
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="📥 拉取EPG数据")
    # 合并所有EPG
    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)

    for i, epg_content in enumerate(epg_contents):
        if epg_content is None:
            continue
        url = urls[i]
        print(f"\n🔧 处理EPG源 {i+1}/{len(urls)}：{url[:50]}...")
        # 预处理（时区矫正）
        epg_content = preprocess_epg(url, epg_content)
        # 解析（14天+过滤）
        channels, programmes = parse_epg(epg_content)
        # 合并频道和节目
        with tqdm(total=len(channels), desc="🔗 合并EPG", unit="频道") as pbar:
            for cid, display_names in channels.items():
                if len(programmes[cid]) == 0:
                    pbar.update(1)
                    continue
                # 匹配已有频道（去重）
                map_id = next((all_channels_map[dn[0]] for dn in display_names if dn[0] in all_channels_map), cid)
                if map_id not in all_channel_id:
                    all_channel_id.add(map_id)
                    all_channel_names[map_id] = display_names
                    all_programmes[map_id] = programmes[cid]
                    # 新增别名映射
                    for dn, lang in display_names:
                        all_channels_map[dn] = map_id
                else:
                    # 保留更多节目（取更长时间的）
                    if len(all_programmes[map_id]) < len(programmes[cid]):
                        all_programmes[map_id] = programmes[cid]
                    # 新增别名
                    for dn, lang in display_names:
                        if dn not in all_channels_map:
                            all_channel_names[map_id].append([dn, lang])
                            all_channels_map[dn] = map_id
                pbar.update(1
