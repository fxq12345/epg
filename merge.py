import os
import gzip
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from lxml import etree

# 全局配置
OUTPUT_DIR = "output"
MAX_RETRY = 2
TIMEOUT = 15

def fetch_with_retry(url):
    retry_cnt = 0
    while retry_cnt < MAX_RETRY:
        retry_cnt += 1
        try:
            print(f"🔄 抓取: {url[:50]}... 第{retry_cnt}次")
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            tree = etree.fromstring(resp.content)
            ch = len(tree.findall(".//channel"))
            pg = len(tree.findall(".//programme"))
            print(f"✅ 成功: 频道 {ch} 节目 {pg}")
            return True, tree, ch, pg, retry_cnt
        except Exception as e:
            print(f"❌ 失败: {str(e)[:50]}")
    return False, None, 0, 0, retry_cnt

def merge_all(weifang_gz_file):
    import sys
    def p(*args):
        print(*args)
        sys.stdout.flush()

    p("🔰 EPG 合并脚本开始运行")

    all_channels = []
    all_programs = []
    xml_trees = []

    # 读取 config.txt
    if not os.path.exists("config.txt"):
        p("❌ 找不到 config.txt")
        return
    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip().startswith("http")]
    if not urls:
        p("❌ config.txt 无有效URL")
        return

    p(f"📥 共 {len(urls)} 个源")

    # 抓取
    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = {executor.submit(fetch_with_retry, u): u for u in urls}
        for t in tasks:
            ok, tree, ch, pg, _ = t.result()
            if ok and tree is not None:
                xml_trees.append(tree)

    p(f"📥 成功加载 {len(xml_trees)} 个XML")

    # 统一频道ID = 频道名称
    chan_map = {}
    for tree in xml_trees:
        for c in tree.findall(".//channel"):
            cid = c.get("id", "").strip()
            dn = c.find("display-name")
            name = dn.text.strip() if (dn is not None and dn.text) else cid
            if cid and name and cid not in chan_map:
                chan_map[cid] = name

    # 收集频道（去重）
    exist_names = set()
    for tree in xml_trees:
        for c in tree.findall(".//channel"):
            old_id = c.get("id", "").strip()
            name = chan_map.get(old_id, old_id)
            if name and name not in exist_names:
                exist_names.add(name)
                c.set("id", name)
                all_channels.append(c)

    # 收集节目
    for tree in xml_trees:
        for p in tree.findall(".//programme"):
            old_c = p.get("channel", "").strip()
            new_c = chan_map.get(old_c, old_c)
            if new_c:
                p.set("channel", new_c)
            tit = p.find("title")
            if tit is None or not tit.text or len(tit.text.strip()) < 2:
                continue
            all_programs.append(p)

    # 本地潍坊源（可选）
    if os.path.exists(weifang_gz_file):
        try:
            with gzip.open(weifang_gz_file, "rb") as f:
                wf_tree = etree.fromstring(f.read())
            wf_chan = {}
            for c in wf_tree.findall(".//channel"):
                cid = c.get("id", "").strip()
                dn = c.find("display-name")
                name = dn.text.strip() if (dn is not None and dn.text) else cid
                wf_chan[cid] = name
                if name and name not in exist_names:
                    exist_names.add(name)
                    c.set("id", name)
                    all_channels.append(c)
            for p in wf_tree.findall(".//programme"):
                old_c = p.get("channel", "").strip()
                new_c = wf_chan.get(old_c, old_c)
                if new_c:
                    p.set("channel", new_c)
                tit = p.find("title")
                if tit is None or not tit.text or len(tit.text.strip()) < 2:
                    continue
                all_programs.append(p)
            p("✅ 潍坊本地源已合并")
        except:
            p("⚠️ 潍坊源读取失败，跳过")

    # 节目去重
    p(f"原始节目数: {len(all_programs)}")
    unique = []
    seen = set()
    for p in all_programs:
        try:
            c = p.get("channel", "")
            s = p.get("start", "")
            t = p.find("title").text.strip()
            key = f"{c}|{s}|{t}"
            if key not in seen:
                seen.add(key)
                unique.append(p)
        except:
            continue
    unique.sort(key=lambda x: (x.get("channel", ""), x.get("start", "")))
    p(f"去重后节目: {len(unique)}")

    # 输出
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, "epg.gz")

    root = etree.Element("tv")
    root.insert(0, etree.Comment(f"Built {datetime.now()}"))
    for c in all_channels:
        root.append(c)
    for p in unique:
        root.append(p)

    xml = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out, "wb") as f:
        f.write(xml)

    size = os.path.getsize(out) / 1024 / 1024
    p("="*50)
    p(f"✅ 完成！频道={len(all_channels)} 节目={len(unique)}")
    p(f"📦 文件: {out}  {size:.2f}MB")
    p("="*50)

if __name__ == "__main__":
    merge_all("weifang_epg.gz")
