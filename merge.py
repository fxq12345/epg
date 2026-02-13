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
            print(f"🔄 抓取: {url[:60]}... 第{retry_cnt}次")
            resp = requests.get(url, timeout=TIMEOUT, stream=True)
            resp.raise_for_status()
            content = resp.content
            if url.endswith(".gz") or resp.headers.get("content-encoding") == "gzip":
                content = gzip.decompress(content)
            tree = etree.fromstring(content)
            ch = len(tree.findall(".//channel"))
            pg = len(tree.findall(".//programme"))
            print(f"✅ 成功: 频道 {ch} 节目 {pg}")
            return True, tree, ch, pg
        except Exception as e:
            print(f"❌ 失败: {str(e)[:80]}")
    return False, None, 0, 0

def merge_all(local_file):
    all_channels = []
    all_programs = []

    # 1. 读取网络源（config.txt）
    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip().startswith("http")]

    print(f"📥 网络源共 {len(urls)} 个")

    xml_trees = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = {executor.submit(fetch_with_retry, u): u for u in urls}
        for t in tasks:
            ok, tree, ch, pg = t.result()
            if ok and tree is not None:
                xml_trees.append(tree)

    print(f"📥 成功加载 {len(xml_trees)} 个XML")

    # 统一频道ID为名称
    id_map = {}
    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            cid = ch.get("id", "").strip()
            dn = ch.find("display-name")
            name = dn.text.strip() if (dn is not None and dn.text) else cid
            if cid and name and cid not in id_map:
                id_map[cid] = name

    exist_names = set()
    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            old_id = ch.get("id", "").strip()
            name = id_map.get(old_id, old_id)
            if name and name not in exist_names:
                exist_names.add(name)
                ch.set("id", name)
                all_channels.append(ch)

    for tree in xml_trees:
        for p in tree.findall(".//programme"):
            old_c = p.get("channel", "").strip()
            new_c = id_map.get(old_c, old_c)
            if new_c:
                p.set("channel", new_c)
            tit = p.find("title")
            if tit is None or not tit.text or len(tit.text.strip()) < 1:
                continue
            all_programs.append(p)

    # 2. 合并潍坊本地源（weifang.gz）
    if os.path.exists(local_file):
        try:
            with gzip.open(local_file, "rb") as f:
                local_tree = etree.fromstring(f.read())
            local_map = {}
            for ch in local_tree.findall(".//channel"):
                cid = ch.get("id", "").strip()
                dn = ch.find("display-name")
                name = dn.text.strip() if (dn is not None and dn.text) else cid
                local_map[cid] = name
                if name and name not in exist_names:
                    exist_names.add(name)
                    ch.set("id", name)
                    all_channels.append(ch)
            for p in local_tree.findall(".//programme"):
                old_c = p.get("channel", "").strip()
                new_c = local_map.get(old_c, old_c)
                if new_c:
                    p.set("channel", new_c)
                tit = p.find("title")
                if tit is None or not tit.text or len(tit.text.strip()) < 1:
                    continue
                all_programs.append(p)
            print("✅ 潍坊本地4频道已合并")
        except Exception as e:
            print(f"⚠️ 潍坊源读取失败: {e}")
    else:
        print(f"⚠️ 未找到潍坊源: {local_file}")

    # 3. 节目去重
    print(f"原始节目数: {len(all_programs)}")
    unique = []
    seen = set()
    for p in all_programs:
        try:
            key = p.get("channel") + "|" + p.get("start")
            if key not in seen:
                seen.add(key)
                unique.append(p)
        except:
            continue
    unique.sort(key=lambda x: (x.get("channel", ""), x.get("start", "")))
    print(f"去重后节目: {len(unique)}")

    # 4. 输出到 output/epg.gz
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "epg.gz")

    root = etree.Element("tv")
    root.insert(0, etree.Comment(f"Built {datetime.now()}"))
    for ch in all_channels:
        root.append(ch)
    for p in unique:
        root.append(p)

    xml_data = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_data)

    size = os.path.getsize(out_path) / 1024 / 1024
    print("="*60)
    print(f"✅ 生成完成！频道={len(all_channels)} 节目={len(unique)} | {size:.2f}MB")
    print("="*60)

if __name__ == "__main__":
    merge_all("weifang.gz")
