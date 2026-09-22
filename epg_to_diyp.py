import requests
import gzip
from io import BytesIO
from xml.etree import ElementTree as ET
import os

# EPG源地址
EPG_URL = "https://raw.githubusercontent.com/sggc/SD-EPG/refs/heads/main/EPG/sggc.xml.gz"

# 输出目录，独立文件夹，不和旧脚本output冲突
OUTPUT_DIR = "output_diyp"
OUTPUT_DIYP = os.path.join(OUTPUT_DIR, "diyp_epg.txt")

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

    print("内存解压并解析XML，不生成临时xml文件")
    try:
        with gzip.GzipFile(fileobj=BytesIO(r.content)) as gz:
            xml_content = gz.read()
        root = ET.fromstring(xml_content)
    except Exception as e:
        print(f"解压/解析失败: {e}")
        return

    print("转换为DIYP txt格式")
    epg_lines = []

    # 自动识别命名空间
    ns = {}
    if '}' in root.tag:
        ns_uri = root.tag.split('{')[1].split('}')[0]
        ns['tv'] = ns_uri

    # 频道映射
    channel_map = {}
    # 兼容有无命名空间两种情况
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
            channel_map[cid] = dn_node.text.strip()

    # 遍历节目
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

        ch_name = channel_map.get(ch_id, ch_id)
        title = title_node.text.strip() if title_node.text else "未知节目"
        s_time = start[:14]
        e_time = stop[:14]
        epg_lines.append(f"{ch_name},{s_time},{e_time},{title}")

    # 写入成品txt
    with open(OUTPUT_DIYP, 'w', encoding='utf-8') as f:
        for line in epg_lines:
            f.write(line + '\n')

    print(f"DIYP EPG生成完成，共 {len(epg_lines)} 条节目")
    print(f"输出路径：{OUTPUT_DIYP}")

if __name__ == '__main__':
    main()
