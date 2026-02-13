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

            # 自动解压 gz 压缩源
            if url.endswith(".gz") or resp.headers.get("content-encoding") == "gzip":
                content = gzip.decompress(content)

            tree = etree.fromstring(content)
            ch = len(tree.findall(".//channel"))
            pg = len(tree.findall(".//programme"))
            print(f"✅ 成功: 频道 {ch} 节目 {pg}")
            return True, tree, ch, pg, retry_cnt
        except Exception as e:
            print(f"❌ 失败: {str(e)[:80]}")
    return False, None, 0, 0, retry_cnt

def merge_all(weifang_gz_file):
    import sys
    def print_flush(*args):
        print(*args)
        sys.stdout.flush()

    print_flush("🔰 EPG 合并脚本（完整8天版）")

    all_channels = []
    all_programs = []
    xml_trees = []

    # 读取 config.txt
    if not os.path.exists("config.txt"):
        print_flush("❌ 找不到 config.txt")
        return
    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip().startswith("http")]
    if not urls:
        print_flush("❌ config.txt 无有效URL")
        return

    print_flush(f"📥 共 {len(urls)} 个源")

    # 抓取
    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = {executor.submit(fetch_with_retry, u): u for u in urls}
        for t in tasks:
            ok, tree, ch, pg, _ = t.result()
            if ok and tree is not None:
                xml_trees.append(tree)

    print_flush(f"📥 成功加载 {len(xml_trees)} 个XML")

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
        for prog in tree.findall(".//programme"):
            old_c = prog.get("channel", "").strip()
            new_c = chan_map.get(old_c, old_c)
            if new_c:
                prog.set("channel", new_c)
            tit = prog.find("title")
            if tit is None or not tit.text or len(tit.text.strip()) < 1:
                continue
            all_programs.append(prog)

    # 潍坊本地源（4个频道）
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
            for prog in wf_tree.findall(".//programme"):
                old_c = prog.get("channel", "").strip()
                new_c = wf_chan.get(old_c, old_c)
                if new_c:
                    prog.set("channel", new_c)
                tit = prog.find("title")
                if tit is None or not tit.text or len(tit.text.strip()) < 1:
                    continue
                all_programs.append(prog)
            print_flush("✅ 潍坊本地4频道已合并")
        except Exception as e:
            print_flush(f"⚠️ 潍坊源读取失败，已跳过")
    else:
        print_flush(f"⚠️ 未找到潍坊本地源，已跳过")

    # 去重：只按 频道+开始时间，绝不丢天数
    print_flush(f"原始节目数: {len(all_programs)}")
    unique = []
    seen = set()
    for prog in all_programs:
        try:
            c = prog.get("channel", "")
            s = prog.get("start", "")
            key = f"{c}|{s}"
            if key not in seen:
                seen.add(key)
                unique.append(prog)
        except:
            continue
    unique.sort(key=lambda x: (x.get("channel", ""), x.get("start", "")))
    print_flush(f"去重后节目: {len(unique)}")

    # 输出最终 epg.gz
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, "epg.gz")

    root = etree.Element("tv")
    root.insert(0, etree.Comment(f"Built {datetime.now()} | 完整8天"))
    for c in all_channels:
        root.append(c)
    for prog in unique:
        root.append(prog)

    xml = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out, "wb") as f:
        f.write(xml)

    size = os.path.getsize(out) / 1024 / 1024
    print_flush("="*60)
    print_flush(f"✅ 生成完成！频道={len(all_channels)} 节目={len(unique)} | {size:.2f}MB")
    print_flush("="*60)

if __name__ == "__main__":
    merge_all("weifang_epg.gz")
