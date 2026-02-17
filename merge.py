import os
import gzip
import requests
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from lxml import etree
import requests.adapters
from requests.packages.urllib3.util.retry import Retry

# 全局配置
OUTPUT_DIR = "output"
MAX_RETRY = 3
TIMEOUT = 30

def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = requests.adapters.HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_with_retry(url):
    session = create_session()
    retry_cnt = 0
    while retry_cnt < MAX_RETRY:
        retry_cnt += 1
        try:
            print(f"🔄 抓取: {url[:60]}... 第{retry_cnt}次")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            resp = session.get(url, timeout=TIMEOUT, headers=headers, stream=True)
            resp.raise_for_status()
            content = resp.content
            
            try_gzip = False
            if url.endswith(".gz"):
                try_gzip = True
            elif resp.headers.get("content-encoding") == "gzip":
                try_gzip = True
            elif resp.headers.get("Content-Type", "").endswith("gzip"):
                try_gzip = True
            
            if try_gzip:
                try:
                    content = gzip.decompress(content)
                    print(f"  检测到gzip格式，已解压")
                except (gzip.BadGzipFile, OSError):
                    print(f"  警告：标记为gzip但实际不是，按普通XML处理")
            
            try:
                xml_str = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    xml_str = content.decode('gbk')
                except:
                    xml_str = content.decode('utf-8', errors='ignore')
            
            tree = etree.fromstring(xml_str.encode('utf-8'))
            ch = len(tree.findall(".//channel"))
            pg = len(tree.findall(".//programme"))
            print(f"✅ 成功: 频道 {ch} 节目 {pg}")
            return True, tree, ch, pg
            
        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {type(e).__name__}: {str(e)[:80]}")
        except etree.XMLSyntaxError as e:
            print(f"❌ XML解析错误: {str(e)[:80]}")
        except Exception as e:
            print(f"❌ 其他错误: {type(e).__name__}: {str(e)[:80]}")
        
        if retry_cnt < MAX_RETRY:
            time.sleep(2 ** retry_cnt)
    
    return False, None, 0, 0

def merge_all(local_file):
    all_channels = []
    all_programs = []

    # 读取并去重URL
    with open("config.txt", "r", encoding="utf-8") as f:
        urls = list({line.strip() for line in f if line.strip().startswith("http")})

    print(f"📥 网络源共 {len(urls)} 个")

    xml_trees = []
    with ThreadPoolExecutor(max_workers=5) as executor:
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

    # 合并潍坊本地源
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
            print(f"⚠️ 潍坊源读取失败，已跳过: {e}")
    else:
        print(f"⚠️ 未找到潍坊源文件 {local_file}，已跳过")

    # 频道排序：山东 > 潍坊 > CCTV > 卫视 > 其他
    def channel_sort_key(channel_elem):
        name = channel_elem.get("id", "").strip()
        if "山东" in name:
            return 0, name
        elif "潍坊" in name:
            return 1, name
        elif "CCTV" in name:
            return 2, name
        elif "卫视" in name:
            return 3, name
        else:
            return 99, name

    all_channels.sort(key=channel_sort_key)

    # 节目去重 + 时间范围：放宽过滤版
    print(f"原始节目数: {len(all_programs)}")
    unique = []
    seen = set()

    now = datetime.now()
    # ====================== 已放宽时间范围 ======================
    start_cutoff = datetime(now.year, now.month, now.day, 0, 0, 0) - timedelta(days=3)   # 从前2天 → 前3天
    end_cutoff   = datetime(now.year, now.month, now.day, 23, 59, 59) + timedelta(days=8) # 从未来7天 → 未来8天

    for p in all_programs:
        try:
            key = p.get("channel") + "|" + p.get("start")
            if key in seen:
                continue

            # 过滤空标题（已放宽：≥1字即可）
            title_elem = p.find("title")
            title = title_elem.text.strip() if (title_elem is not None and title_elem.text) else ""
            if not title or len(title) < 1:
                continue

            # 兼容 14 位时间（防止截断出错）
            start_str = p.get("start", "")[:14]
            if len(start_str) >= 12:
                p_start = datetime.strptime(start_str[:12], "%Y%m%d%H%M")
            else:
                continue

            # 时间过滤（已放宽）
            if not (start_cutoff <= p_start <= end_cutoff):
                continue

            seen.add(key)
            unique.append(p)

        except Exception as e:
            # 打印异常节目，方便你排查
            # print(f"⚠️ 节目异常: {p.get('channel')} {p.get('start')} → {e}")
            continue

    unique.sort(key=lambda x: (x.get("channel", ""), x.get("start", "")))
    print(f"去重后节目: {len(unique)}")

    # 输出
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "epg.gz")

    root = etree.Element("tv")
    root.insert(0, etree.Comment(f"Built {datetime.now()} | 放宽过滤版：保留前3天+未来8天"))
    for ch in all_channels:
        root.append(ch)
    for p in unique:
        root.append(p)

    xml_data = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml_data)

    size = os.path.getsize(out_path) / 1024 / 1024
    print("="*60)
    print(f"✅ 最终生成完成！（已放宽过滤）")
    print(f"📅 时间范围：{start_cutoff.strftime('%Y-%m-%d')} 至 {end_cutoff.strftime('%Y-%m-%d')}")
    print(f"📺 频道总数：{len(all_channels)}")
    print(f"📅 有效节目：{len(unique)}")
    print(f"📦 文件大小：{size:.2f}MB")
    print("="*60)

if __name__ == "__main__":
    merge_all("weifang.gz")
