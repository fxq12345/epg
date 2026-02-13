import os
import gzip
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from lxml import etree

# 全局配置
OUTPUT_DIR = "output"
MAX_RETRY = 3
TIMEOUT = 30

def fetch_with_retry(url):
    retry_cnt = 0
    while retry_cnt < MAX_RETRY:
        retry_cnt += 1
        try:
            resp = requests.get(url, timeout=TIMEOUT, stream=True)
            resp.raise_for_status()
            content = resp.content
            if url.endswith(".gz"):
                content = gzip.decompress(content)
            tree = etree.fromstring(content)
            return True, tree, len(tree.findall(".//channel")), len(tree.findall(".//programme"))
        except:
            if retry_cnt >= MAX_RETRY:
                return False, None, 0, 0

def merge_all(local_file):
    all_channels = []
    all_programs = []

    # 1. 读取网络源（config.txt 里的5条）
    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip().startswith("http")]

    print(f"📥 网络源共 {len(urls)} 个")

    xml_trees = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_with_retry, url) for url in urls]
        for fut in futures:
            ok, tree, _, _ = fut.result()
            if ok:
                xml_trees.append(tree)

    # 统一频道ID为名称
    id_map = {}
    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            cid = ch.get("id", "")
            dn = ch.find("display-name")
            name = dn.text.strip() if (dn is not None and dn.text) else cid
            if cid and cid not in id_map:
                id_map[cid] = name

    exist_names = set()
    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            old_id = ch.get("id", "")
            name = id_map.get(old_id, old_id)
            if name and name not in exist_names:
                exist_names.add(name)
                ch.set("id", name)
                all_channels.append(ch)

    for tree in xml_trees:
        for p in tree.findall(".//programme"):
            old_c = p.get("channel", "")
            new_c = id_map.get(old_c, old_c)
            if new_c:
                p.set("channel", new_c)
            all_programs.append(p)

    # 2. 合并潍坊本地源（weifang_4channels_epg.xml）
    if os.path.exists(local_file):
        try:
            with open(local_file, "r", encoding="utf-8") as f:
                local_tree = etree.fromstring(f.read().encode("utf-8"))
            local_map = {}
            for ch in local_tree.findall(".//channel"):
                cid = ch.get("id", "")
                dn = ch.find("display-name")
                name = dn.text.strip() if (dn is not None and dn.text) else cid
                local_map[cid] = name
                if name and name not in exist_names:
                    exist_names.add(name)
                    ch.set("id", name)
                    all_channels.append(ch)
            for p in local_tree.findall(".//programme"):
                old_c = p.get("channel", "")
                new_c = local_map.get(old_c, old_c)
                if new_c:
                    p.set("channel", new_c)
                all_programs.append(p)
            print("✅ 潍坊本地4频道已合并")
        except Exception as e:
            print("⚠️ 潍坊源读取失败，已跳过")

    # 3. 节目去重
    unique_p = []
    seen = set()
    for p in all_programs:
        try:
            key = p.get("channel") + "|" + p.get("start")
            if key not in seen:
                seen.add(key)
                unique_p.append(p)
        except:
            continue

    # 4. 输出到 output/epg.gz
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "epg.gz")

    root = etree.Element("tv")
    for ch in all_channels:
        root.append(ch)
    for p in unique_p:
        root.append(p)

    xml_data = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_data)

    size = os.path.getsize(out_path) / 1024 / 1024
    print(f"✅ 生成完成！频道={len(all_channels)} 节目={len(unique_p)} | {size:.2f}MB")

if __name__ == "__main__":
    merge_all("weifang_4channels_epg.xml")
