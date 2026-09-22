import requests
import gzip
from io import BytesIO
from xml.etree import ElementTree as ET
import os
import html
from datetime import datetime, timedelta

# EPG源地址
EPG_URL = "https://raw.githubusercontent.com/sggc/SD-EPG/refs/heads/main/EPG/sggc.xml.gz"

# 输出目录
OUTPUT_DIR = "output_diyp"
OUTPUT_DIYP = os.path.join(OUTPUT_DIR, "diyp_epg.txt")

def unescape_all(s):
    # 循环多层转义解码，清理&符号
    while True:
        new_s = html.unescape(s)
        if new_s == s:
            break
        s = new_s
    s = s.replace("<", "").replace(">", "")
    return s

def normalize_channel_name(name):
    # 频道名标准化 CCTV-1 → CCTV1
    name = unescape_all(name.strip())
    name = name.replace("-", "").replace(" ","")
    return name

def get_http(url, timeout=60, retry=3):
    session = requests.Session()
    for i in range(retry):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"下载失败: {e}")
    raise Exception("多次重试下载EPG源失败")

def main():
    print("正在下载 SD-EPG xml.gz ...")
    try:
        r = get_http(EPG_URL, timeout=60)
    except Exception as e:
        print(f"下载失败: {e}")
        return

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print("内存解压并解析XML")
    try:
        with gzip.GzipFile(fileobj=BytesIO(r.content)) as gz:
            xml_content = gz.read()
        root = ET.fromstring(xml_content)
    except Exception as e:
        print(f"解压/解析失败: {e}")
        return

    print("转换DIYP txt格式，只保留【昨天、今天、明天】三天节目")
    epg_lines = []

    ns = {}
    if '}' in root.tag:
        ns_uri = root.tag.split('{')[1].split('}')[0]
        ns['tv'] = ns_uri

    channel_map = {}
    if ns:
        channel_list = root.findall('.//tv:channel', ns)
    else:
        channel_list = root.findall('.//channel')

    for ch in channel_list:
        cid = ch.get('id')
        if ns:
            dn_node = ch.find('tv:display-name', ns)
        else:
            dn_node = ch.find('display-name')
        if cid and dn_node is not None and dn_node.text:
            raw_name = unescape_all(dn_node.text.strip())
            clean_name = normalize_channel_name(raw_name)
            channel_map[cid] = clean_name

    # Action机器已经设置Asia/Shanghai，now就是北京时间
    now = datetime.now()
    start_time = now - timedelta(days=1)   # 昨天
    end_time = now + timedelta(days=1)     # 明天
    print(f"当前系统时间(北京时间): {now}")
    print(f"筛选范围：{start_time.strftime('%Y-%m-%d')} ~ {end_time.strftime('%Y-%m-%d')}")

    def in_time_range(time_str):
        try:
            # SD-EPG start属性类似：20260922080000 +0800，截取前14位
            t = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            return start_time <= t <= end_time
        except Exception as e:
            return False

    if ns:
        prog_list = root.findall('.//tv:programme', ns)
    else:
        prog_list = root.findall('.//programme')

    for prog in prog_list:
        ch_id = prog.get('channel')
        start = prog.get('start')
        stop = prog.get('stop')
        if ns:
            title_node = prog.find('tv:title', ns)
        else:
            title_node = prog.find('title')

        if not all([ch_id, start, stop, title_node is not None]) or len(start)<14 or len(stop)<14:
            continue
        # 时间过滤
        if not in_time_range(start):
            continue

        ch_name = channel_map.get(ch_id, ch_id)
        title = unescape_all(title_node.text.strip()) if title_node.text else "未知节目"
        s_time = start[:14]
        e_time = stop[:14]
        epg_lines.append(f"{ch_name},{s_time},{e_time},{title}")

    with open(OUTPUT_DIYP, 'w', encoding='utf-8') as f:
        for line in epg_lines:
            f.write(line + '\n')

    print(f"DIYP EPG生成完成，共 {len(epg_lines)} 条节目")
    # 打印前5行预览，方便排查
    print("===== 预览前5行 =====")
    for line in epg_lines[:5]:
        print(line)

if __name__ == '__main__':
    main()
