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

# ====================== XML修复函数 ======================
def clean_xml_content(content):
    """彻底清洗XML内容，修复格式错误"""
    if not content:
        return ""
    
    # 1. 修复错误的闭合标签
    content = re.sub(r'<//title>', '</title>', content)
    content = re.sub(r'</></title>', '</title>', content)
    
    # 2. 修复属性值换行
    content = re.sub(r'(start|stop|channel)=\s*\n\s*"([^"]+)"', r'\1="\2"', content)
    
    # 3. 修复孤立的<title>标签
    lines = content.split('\n')
    cleaned_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        
        # 修复孤立的<title>标签
        if stripped == '<title>' and (i+1 >= len(lines) or not lines[i+1].strip().startswith('</title>')):
            # 查找下一个</programme>或<programme>
            j = i + 1
            while j < len(lines) and not lines[j].strip().startswith(('</programme>', '<programme')):
                j += 1
            # 在合适位置插入</title>
            if j < len(lines):
                lines.insert(j, '  </title>')
        
        cleaned_lines.append(line)
        i += 1
    
    content = '\n'.join(cleaned_lines)
    
    # 4. 修复不匹配的</programme>
    content = re.sub(r'</programme>\s*<programme', '</programme>\n<programme', content)
    
    # 5. 移除没有对应开标签的</programme>
    programme_open = 0
    lines = content.split('\n')
    cleaned_lines = []
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('<programme'):
            programme_open += 1
            cleaned_lines.append(line)
        elif stripped == '</programme>':
            if programme_open > 0:
                programme_open -= 1
                cleaned_lines.append(line)
            # 否则跳过这个多余的</programme>
        else:
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

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
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name", lang="zh")
            dn.text = ch_name

        for ch_name, base_url in WEIFANG_CHANNELS:
            programs = get_channel_7days(channel_name=ch_name, base_url=base_url)
            for start, stop, title in programs:
                prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
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

# ====================== 改进的合并逻辑 ======================
def fetch_with_retry(u, max_retry=MAX_RETRY):
    for attempt in range(1, max_retry + 1):
        try:
            r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code not in (200, 206):
                time.sleep(1)
                continue

            if u.endswith(".gz"):
                content = gzip.decompress(r.content).decode("utf-8", "ignore")
            else:
                content = r.text

            # 清理XML内容
            content = clean_xml_content(content)
            
            # 修复常见格式问题
            content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
            content = content.replace("& ", "&amp; ")
            
            # 尝试解析
            try:
                parser = etree.XMLParser(recover=True)
                tree = etree.fromstring(content.encode("utf-8"), parser=parser)
                
                # 验证基本结构
                channels = tree.xpath("//channel")
                programmes = tree.xpath("//programme")
                
                if len(channels) > 0 and len(programmes) > 0:
                    return (True, tree, len(channels), len(programmes), attempt)
            except Exception as e:
                print(f"⚠️ XML解析失败，尝试修复: {e}")
                # 尝试提取有效数据
                pass
                
        except Exception as e:
            print(f"❌ 抓取失败 {u[:50]}...: {e}")
            time.sleep(1)
            continue
    return (False, None, 0, 0, max_retry)

def merge_all(weifang_gz_file):
    all_channels = []
    all_programs = []
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

    with ThreadPoolExecutor(max_workers=6) as executor:
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
                for node in tree:
                    if node.tag == "channel":
                        all_channels.append(node)
                    elif node.tag == "programme":
                        all_programs.append(node)
            else:
                fail_cnt += 1

    if fail_cnt > 0:
        print(f"❌ 共 {fail_cnt} 个源经{MAX_RETRY}次重试后仍失败，已跳过")

    print("=" * 60)
    print(f"汇总：成功 {success_cnt} 个 | 失败 {fail_cnt} 个 | 总频道 {total_ch} | 总节目 {total_pg}")
    print("=" * 60)

    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            parser = etree.XMLParser(recover=True)
            wf_tree = etree.fromstring(wf_content.encode("utf-8"), parser=parser)
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}（本周一~周日完整7天）")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
        else:
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except Exception as e:
        print(f"⚠️ 潍坊本地源读取失败: {e}")

    print(f"处理前: 频道 {len(all_channels)} 个, 节目 {len(all_programs)} 个")

    # ====================== 修复频道去重 ======================
    seen_channel_names = set()
    unique_channels = []
    channel_id_mapping = {}  # 原始ID -> 保留ID
    name_to_id = {}  # 频道名称 -> 频道ID
    
    for ch in all_channels:
        try:
            # 获取频道ID
            channel_id = ch.get('id', '')
            if not channel_id:
                continue
                
            # 获取频道名称
            display_name = ch.findtext("display-name", "").strip()
            if not display_name:
                display_name = channel_id
            
            # 标准化名称（小写）
            normalized_name = display_name.lower()
            
            if normalized_name not in seen_channel_names:
                seen_channel_names.add(normalized_name)
                unique_channels.append(ch)
                name_to_id[normalized_name] = channel_id
                # 自身映射
                channel_id_mapping[channel_id] = channel_id
            else:
                # 重复频道，映射到已存在的频道ID
                existing_id = name_to_id.get(normalized_name)
                if existing_id:
                    channel_id_mapping[channel_id] = existing_id
        except Exception as e:
            print(f"⚠️ 处理频道时出错: {e}")
            continue
    
    print(f"频道去重后: {len(unique_channels)} 个唯一频道")
    
    # ====================== 修复节目处理 ======================
    valid_programs = []
    program_keys = set()  # 用于去重
    
    for prog in all_programs:
        try:
            old_channel_id = prog.get('channel', '')
            start = prog.get('start', '')
            stop = prog.get('stop', '')
            title_elem = prog.find("title")
            
            # 验证必要字段
            if not all([old_channel_id, start, stop]):
                continue
                
            if title_elem is None or not title_elem.text:
                continue
                
            title = title_elem.text.strip()
            if len(title) < 2:
                continue
            
            # 查找正确的频道ID
            new_channel_id = channel_id_mapping.get(old_channel_id, old_channel_id)
            
            # 检查是否有对应的频道存在
            channel_exists = any(ch.get('id') == new_channel_id for ch in unique_channels)
            if not channel_exists:
                # 尝试通过名称查找
                for ch in unique_channels:
                    ch_name = ch.findtext("display-name", "").strip().lower()
                    if old_channel_id.lower() in ch_name or ch_name in old_channel_id.lower():
                        new_channel_id = ch.get('id', '')
                        break
            
            if not new_channel_id:
                continue
            
            # 创建去重键
            program_key = f"{new_channel_id}|{start}|{title}"
            
            if program_key not in program_keys:
                program_keys.add(program_key)
                
                # 创建新的节目元素
                new_prog = etree.Element("programme", 
                                        channel=new_channel_id,
                                        start=start,
                                        stop=stop)
                title_elem = etree.SubElement(new_prog, "title", lang="zh")
                title_elem.text = title
                
                valid_programs.append(new_prog)
                
        except Exception as e:
            print(f"⚠️ 处理节目时出错: {e}")
            continue
    
    print(f"节目去重后: {len(valid_programs)} 个有效节目")
    print(f"🎯 去重率: {(len(all_programs) - len(valid_programs)) / len(all_programs) * 100:.1f}%")
    
    # 按频道和开始时间排序
    valid_programs.sort(key=lambda x: (x.get('channel', ''), x.get('start', '')))
    
    # 生成最终XML
    final_root = etree.Element("tv")
    
    # 添加频道
    for ch in unique_channels:
        final_root.append(ch)
    
    # 添加节目
    for prog in valid_programs:
        final_root.append(prog)
    
    # 生成XML字符串
    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    
    # 验证XML格式
    try:
        parser = etree.XMLParser(recover=True)
        test_tree = etree.fromstring(xml_str, parser=parser)
        
        # 检查是否有格式问题
        test_channels = test_tree.xpath("//channel")
        test_programs = test_tree.xpath("//programme")
        
        print(f"✅ XML验证通过: {len(test_channels)} 频道, {len(test_programs)} 节目")
        
    except Exception as e:
        print(f"❌ 生成的XML格式错误: {e}")
        # 创建最小可用的XML
        final_root = etree.Element("tv")
        xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    
    # 保存文件
    output_path = os.path.join(OUTPUT_DIR, "epg.gz")
    with gzip.open(output_path, "wb") as f:
        f.write(xml_str)
    
    # 计算文件大小
    file_size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"✅ 最终输出：频道 {len(unique_channels)} 个 | 节目 {len(valid_programs)} 个")
    print(f"📦 文件大小：{file_size_mb:.2f} MB")
    print(f"📁 输出文件：{output_path}")
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
