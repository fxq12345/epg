import os
import gzip
import re
import time
import signal
import requests
from lxml import etree
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import hashlib

# 10分钟强制终止
signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm(600)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ====================== 潍坊4频道配置 ======================
WEIFANG_CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

# 网站固定后缀：周一w1 ~ 周日w7
WEEK_MAP = {
    "周一": "w1",
    "周二": "w2",
    "周三": "w3",
    "周四": "w4",
    "周五": "w5",
    "周六": "w6",
    "周日": "w7"
}

MAX_RETRY = 2

# === 必应Referer 防反爬 ===
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com/search?q=%E7%94%B5%E8%A7%86%E8%8A%82%E7%9B%AE%E8%A1%A8"
}

# --- 可选Selenium（不装也能跑）---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# ====================== 工具函数 ======================
def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_page_html(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.encoding = 'utf-8'
        html_content = resp.text
        if re.findall(r'\d{1,2}:\d{2}', html_content):
            return html_content
    except Exception:
        pass

    if SELENIUM_AVAILABLE:
        try:
            opt = Options()
            opt.add_argument("--headless")
            opt.add_argument("--no-sandbox")
            opt.add_argument("--disable-dev-shm-usage")
            opt.add_argument(f"user-agent={HEADERS['User-Agent']}")
            driver = webdriver.Chrome(options=opt)
            driver.get(url)
            time.sleep(2.5)
            html_content = driver.page_source
            driver.quit()
            return html_content
        except Exception:
            pass
    return ""

def clean_channel_name(name):
    """清理频道名称"""
    if not name:
        return ""
    # 移除多余空格和特殊字符
    name = re.sub(r'\s+', ' ', name.strip())
    # 标准化一些常见名称
    name = re.sub(r'CCTV-(\d+)', r'CCTV\1', name)
    name = re.sub(r'CCTV(\d+)高清', r'CCTV\1', name)
    name = re.sub(r'CCTV(\d+)HD', r'CCTV\1', name)
    return name

def create_channel_id(name):
    """从频道名称创建规范的频道ID"""
    if not name:
        return "unknown"
    
    # 移除所有非字母数字字符，用下划线连接
    clean_id = re.sub(r'[^\w]+', '_', name.strip())
    # 移除连续的下划线
    clean_id = re.sub(r'_+', '_', clean_id)
    # 移除首尾下划线
    clean_id = clean_id.strip('_')
    # 确保以字母开头
    if clean_id and not clean_id[0].isalpha():
        clean_id = 'ch_' + clean_id
    
    return clean_id if clean_id else f"channel_{hashlib.md5(name.encode()).hexdigest()[:8]}"

# ====================== 核心：抓【本周一 ~ 周日】7天 ======================
def get_channel_7days(channel_name, base_url):
    week_list = list(WEEK_MAP.items())
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    channel_progs = []

    for i, (week_name, w_suffix) in enumerate(week_list):
        current_date = monday + timedelta(days=i)

        if base_url.endswith('/'):
            url = f"{base_url}{w_suffix}"
        else:
            url = f"{base_url}/{w_suffix}"

        html_content = get_page_html(url)
        if not html_content:
            time.sleep(1)
            continue

        soup = BeautifulSoup(html_content, "html.parser")
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I))
        if not items:
            items = soup.find_all("li")

        day_progs = []
        for item in items:
            txt = item.get_text(strip=True)
            match = re.search(r'(\d{1,2}:\d{2})\s*(.+)', txt)
            if not match:
                continue
            t_str, title = match.groups()
            if len(title) < 2 or '广告' in title or '报时' in title:
                continue
            day_progs.append((t_str.strip(), title.strip()))

        day_progs = sorted(list(set(day_progs)), key=lambda x: x[0])
        for idx in range(len(day_progs)):
            t_start, title = day_progs[idx]
            if idx < len(day_progs)-1:
                t_end = day_progs[idx+1][0]
            else:
                h, m = map(int, t_start.split(':'))
                end_dt = datetime(2000, 1, 1, h, m) + timedelta(minutes=30)
                t_end = end_dt.strftime("%H:%M")

            start = time_to_xmltv(current_date, t_start)
            end = time_to_xmltv(current_date, t_end)
            if start and end:
                channel_progs.append((start, end, title))
        time.sleep(1.0)
    return channel_progs

# ====================== 潍坊7天抓取 ======================
def crawl_weifang():
    try:
        root = etree.Element("tv")
        for ch_name, _ in WEIFANG_CHANNELS:
            ch_id = create_channel_id(ch_name)
            ch = etree.SubElement(root, "channel", id=ch_id)
            dn = etree.SubElement(ch, "display-name", lang="zh")
            dn.text = ch_name

        for ch_name, base_url in WEIFANG_CHANNELS:
            programs = get_channel_7days(channel_name=ch_name, base_url=base_url)
            ch_id = create_channel_id(ch_name)
            for start, stop, title in programs:
                prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_id)
                t = etree.SubElement(prog, "title", lang="zh")
                t.text = title

        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        xml_content = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
        
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_content)
            
        print(f"✅ 潍坊EPG已保存: {wf_path}")
        return wf_path
        
    except Exception as e:
        print(f"❌ 潍坊源抓取失败: {e}")
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
        return wf_path

# ====================== XML修复和清洗函数 ======================
def extract_valid_xml(content):
    """从可能格式错误的内容中提取有效的XML"""
    if not content:
        return None
    
    # 1. 查找所有有效的channel元素
    channel_pattern = r'<channel\s+[^>]*id\s*=\s*["\'][^"\']+["\'][^>]*>.*?</channel>'
    channels = re.findall(channel_pattern, content, re.DOTALL | re.IGNORECASE)
    
    # 2. 查找所有有效的programme元素
    programme_pattern = r'<programme\s+[^>]*start\s*=\s*["\'][^"\']+["\'][^>]*stop\s*=\s*["\'][^"\']+["\'][^>]*>.*?</programme>'
    programmes = re.findall(programme_pattern, content, re.DOTALL | re.IGNORECASE)
    
    # 如果找到了内容，重新构建规范的XML
    if channels or programmes:
        xml_parts = ['<?xml version="1.0" encoding="utf-8"?>', '<tv>']
        xml_parts.extend(channels)
        xml_parts.extend(programmes)
        xml_parts.append('</tv>')
        return '\n'.join(xml_parts)
    
    return None

def fetch_with_retry(u, max_retry=MAX_RETRY):
    for attempt in range(1, max_retry + 1):
        try:
            r = requests.get(u, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code not in (200, 206):
                time.sleep(1)
                continue

            if u.endswith(".gz"):
                content = gzip.decompress(r.content).decode("utf-8", "ignore")
            else:
                content = r.text

            # 尝试直接解析
            try:
                parser = etree.XMLParser(recover=True)
                tree = etree.fromstring(content.encode("utf-8"), parser=parser)
                ch = len(tree.xpath("//channel"))
                pg = len(tree.xpath("//programme"))
                if ch > 0 and pg > 0:
                    return (True, tree, ch, pg, attempt)
            except:
                pass
            
            # 如果直接解析失败，尝试提取有效内容
            fixed_xml = extract_valid_xml(content)
            if fixed_xml:
                parser = etree.XMLParser(recover=True)
                tree = etree.fromstring(fixed_xml.encode("utf-8"), parser=parser)
                ch = len(tree.xpath("//channel"))
                pg = len(tree.xpath("//programme"))
                if ch > 0 and pg > 0:
                    return (True, tree, ch, pg, attempt)
                    
        except Exception as e:
            print(f"❌ 抓取失败 {u[:50]}...: {e}")
            time.sleep(1)
            continue
    return (False, None, 0, 0, max_retry)

def merge_all(weifang_gz_file):
    # 存储处理后的频道和节目
    channel_data = {}  # 频道ID -> (显示名称, 原始频道节点)
    program_data = defaultdict(list)  # 频道ID -> 节目列表
    
    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        print("❌ 未找到 config.txt 文件")
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    if not urls:
        print("❌ config.txt 中没有找到有效的URL")
        return

    print("=" * 60)
    print("EPG 源抓取统计（失败自动重试）")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {executor.submit(fetch_with_retry, u): u for u in urls}
        for fut in future_map:
            u = future_map[fut]
            ok, tree, ch, pg, retry_cnt = fut.result()
            if ok:
                success_cnt += 1
                total_ch += ch
                total_pg += pg
                log_retry = f"[重试{retry_cnt-1}次]" if retry_cnt > 1 else ""
                print(f"✅ {u[:55]}... {log_retry}成功 | 频道 {ch:>4} | 节目 {pg:>6}")
                
                # 处理频道
                for channel in tree.xpath("//channel"):
                    try:
                        channel_id = channel.get('id')
                        if not channel_id:
                            continue
                            
                        display_name = channel.findtext("display-name", "").strip()
                        if not display_name:
                            display_name = channel_id
                        
                        # 清理频道名称
                        clean_name = clean_channel_name(display_name)
                        clean_id = create_channel_id(clean_name)
                        
                        # 存储频道数据
                        channel_data[clean_id] = (clean_name, channel)
                        
                    except Exception as e:
                        print(f"⚠️ 处理频道时出错: {e}")
                        continue
                
                # 处理节目
                for programme in tree.xpath("//programme"):
                    try:
                        channel_id = programme.get('channel')
                        start = programme.get('start')
                        stop = programme.get('stop')
                        title = programme.findtext("title", "").strip()
                        
                        if not all([channel_id, start, stop, title]):
                            continue
                            
                        # 查找对应的频道ID
                        display_name = ""
                        for ch_id, (ch_name, _) in channel_data.items():
                            # 如果频道ID匹配或显示名称匹配
                            if channel_id == ch_id:
                                clean_id = ch_id
                                break
                        else:
                            # 如果没有找到匹配，尝试清理原始频道ID
                            clean_id = create_channel_id(channel_id)
                        
                        # 检查节目是否重复（相同的频道、开始时间和标题）
                        program_key = f"{clean_id}_{start}_{hashlib.md5(title.encode()).hexdigest()[:8]}"
                        program_data[clean_id].append((start, stop, title, program_key))
                        
                    except Exception as e:
                        print(f"⚠️ 处理节目时出错: {e}")
                        continue
            else:
                fail_cnt += 1

    if fail_cnt > 0:
        print(f"❌ 共 {fail_cnt} 个源经{MAX_RETRY}次重试后仍失败，已跳过")

    print("=" * 60)
    print(f"汇总：成功 {success_cnt} 个 | 失败 {fail_cnt} 个 | 总频道 {len(channel_data)} | 总节目 {sum(len(v) for v in program_data.values())}")
    print("=" * 60)

    # 添加潍坊本地源
    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            parser = etree.XMLParser(recover=True)
            wf_tree = etree.fromstring(wf_content.encode("utf-8"), parser)
            
            for channel in wf_tree.xpath("//channel"):
                channel_id = channel.get('id')
                display_name = channel.findtext("display-name", "").strip()
                
                if channel_id and display_name:
                    clean_name = clean_channel_name(display_name)
                    clean_id = create_channel_id(clean_name)
                    channel_data[clean_id] = (clean_name, channel)
            
            for programme in wf_tree.xpath("//programme"):
                channel_id = programme.get('channel')
                start = programme.get('start')
                stop = programme.get('stop')
                title = programme.findtext("title", "").strip()
                
                if not all([channel_id, start, stop, title]):
                    continue
                
                # 查找对应的频道ID
                display_name = ""
                for ch_id, (ch_name, _) in channel_data.items():
                    if channel_id == ch_id:
                        clean_id = ch_id
                        break
                else:
                    clean_id = create_channel_id(channel_id)
                
                program_key = f"{clean_id}_{start}_{hashlib.md5(title.encode()).hexdigest()[:8]}"
                program_data[clean_id].append((start, stop, title, program_key))
                
    except Exception as e:
        print(f"⚠️ 潍坊本地源读取失败: {e}")

    print(f"处理后的频道数量: {len(channel_data)}")
    print(f"处理后的节目数量: {sum(len(v) for v in program_data.values())}")
    
    # ====================== 去重节目 ======================
    print("开始去重节目...")
    for channel_id in list(program_data.keys()):
        programs = program_data[channel_id]
        # 使用集合去重
        unique_programs = {}
        for start, stop, title, key in programs:
            unique_programs[key] = (start, stop, title)
        # 按开始时间排序
        sorted_programs = sorted(unique_programs.values(), key=lambda x: x[0])
        program_data[channel_id] = sorted_programs
    
    total_unique_programs = sum(len(v) for v in program_data.values())
    print(f"去重后的节目数量: {total_unique_programs}")
    
    # ====================== 生成最终XML ======================
    print("生成最终XML...")
    root = etree.Element("tv")
    
    # 添加频道
    for channel_id, (display_name, _) in sorted(channel_data.items()):
        ch = etree.SubElement(root, "channel", id=channel_id)
        dn = etree.SubElement(ch, "display-name", lang="zh")
        dn.text = display_name
    
    # 添加节目
    for channel_id, programs in program_data.items():
        for start, stop, title in programs:
            prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=channel_id)
            t = etree.SubElement(prog, "title", lang="zh")
            t.text = title
    
    # 生成XML
    xml_declaration = '<?xml version="1.0" encoding="utf-8"?>\n'
    xml_str = xml_declaration.encode('utf-8') + etree.tostring(root, encoding="utf-8", pretty_print=True)
    
    # 保存压缩文件
    output_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(output_path, "wb") as f:
        f.write(xml_str)
    
    # 计算文件大小
    file_size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"✅ 最终输出：频道 {len(channel_data)} 个 | 节目 {total_unique_programs} 个")
    print(f"📦 文件大小：{file_size_mb:.2f} MB")
    print(f"📁 输出文件：{output_path}")
    
    # 保存未压缩的XML用于调试
    xml_debug_path = os.path.join(OUTPUT_DIR, "epg.xml")
    with open(xml_debug_path, "wb") as f:
        f.write(xml_str)
    print(f"📁 调试文件（未压缩）：{xml_debug_path}")
    
    # 显示压缩前后大小对比
    if os.path.exists(os.path.join(OUTPUT_DIR, "weifang.gz")):
        wf_size = os.path.getsize(os.path.join(OUTPUT_DIR, "weifang.gz")) / 1024
        epg_size = os.path.getsize(output_path) / 1024
        print(f"📊 大小对比：潍坊源 {wf_size:.1f} KB | 合并后 {epg_size:.1f} KB | 差异 {epg_size-wf_size:.1f} KB")
    
    print("=" * 60)

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        print("开始抓取潍坊本地EPG...")
        wf_gz = crawl_weifang()
        print("开始合并所有EPG源...")
        merge_all(wf_gz)
        print("✅ EPG合并完成！")
    except Exception as e:
        print(f"❌ 脚本执行失败: {e}")
        import traceback
        traceback.print_exc()
