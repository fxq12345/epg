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
import html

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

# ====================== XML清洗和修复函数 ======================
def clean_xml_content(content):
    """清洗和修复XML内容，移除格式错误"""
    if not content:
        return ""
    
    # 1. 移除控制字符，但保留换行和制表符
    content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', content)
    
    # 2. 修复未转义的&符号（但保留已转义的实体）
    content = re.sub(r'&(?!(amp|lt|gt|quot|apos|#x?[0-9a-f]+);)', '&amp;', content)
    
    # 3. 修复标签嵌套错误：移除多余的</channel>闭合标签
    # 匹配正确的channel标签对，然后移除不在正确位置的</channel>
    lines = content.split('\n')
    cleaned_lines = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # 如果当前行是<channel开头，则找到对应的结束标签
        if line.startswith('<channel') and not line.startswith('</channel'):
            # 收集这个channel的所有行，直到找到正确的</channel>
            channel_lines = [lines[i]]
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('</channel'):
                channel_lines.append(lines[i])
                i += 1
            if i < len(lines) and lines[i].strip().startswith('</channel'):
                channel_lines.append(lines[i])  # 添加正确的闭合标签
                i += 1
            # 将这个channel的内容加入清理后列表
            cleaned_lines.extend(channel_lines)
            continue
        # 如果遇到孤立的</channel>，跳过它
        if line == '</channel>':
            i += 1
            continue
        cleaned_lines.append(lines[i])
        i += 1
    
    content = '\n'.join(cleaned_lines)
    
    # 4. 修复programme标签内的错误：移除多余的<title>和</channel>
    # 使用正则表达式匹配每个programme标签
    programme_pattern = r'(<programme[^>]*>)(.*?)(</programme>)'
    
    def fix_programme(match):
        opening = match.group(1)
        inner = match.group(2)
        closing = match.group(3)
        
        # 移除inner中多余的</channel>标签
        inner = re.sub(r'</channel>', '', inner)
        
        # 提取所有title标签，只保留最后一个（如果多个）
        titles = re.findall(r'<title[^>]*>(.*?)</title>', inner, re.DOTALL)
        if titles:
            # 只保留最后一个title
            last_title = titles[-1].strip()
            # 移除inner中所有的title标签
            inner = re.sub(r'<title[^>]*>.*?</title>', '', inner, flags=re.DOTALL)
            # 在inner末尾添加正确的title标签
            inner += f'<title lang="zh">{last_title}</title>'
        
        return opening + inner + closing
    
    content = re.sub(programme_pattern, fix_programme, content, flags=re.DOTALL)
    
    # 5. 修复属性值中的换行（如图片中start=和stop=后的换行）
    content = re.sub(r'(\w+)=\s*\n\s*"([^"]+)"', r'\1="\2"', content)
    
    # 6. 确保所有标签正确闭合（简化处理，移除没有对应开头的闭合标签）
    # 移除没有对应<channel>的</channel>
    channel_open = 0
    lines = content.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('<channel') and not stripped.startswith('</channel'):
            channel_open += 1
            cleaned_lines.append(line)
        elif stripped == '</channel>':
            if channel_open > 0:
                channel_open -= 1
                cleaned_lines.append(line)
            # 否则跳过这个多余的闭合标签
        else:
            cleaned_lines.append(line)
    content = '\n'.join(cleaned_lines)
    
    return content

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

# ====================== 核心：抓【本周一 ~ 本周日】7天 ======================
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

# ====================== 修复后的合并逻辑 ======================
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

            # 清洗XML内容
            content = clean_xml_content(content)
            
            # 尝试解析XML
            try:
                parser = etree.XMLParser(recover=True)
                tree = etree.fromstring(content.encode("utf-8"), parser=parser)
                
                # 验证XML结构
                channels = tree.xpath("//channel")
                programmes = tree.xpath("//programme")
                
                if len(channels) > 0 and len(programmes) > 0:
                    return (True, tree, len(channels), len(programmes), attempt)
            except Exception as e:
                print(f"⚠️ XML解析失败，尝试修复: {e}")
                # 如果解析失败，尝试提取有效内容
                channels = re.findall(r'<channel[^>]*>.*?</channel>', content, re.DOTALL)
                programmes = re.findall(r'<programme[^>]*>.*?</programme>', content, re.DOTALL)
                
                if channels and programmes:
                    # 构建新的XML
                    fixed_xml = '<?xml version="1.0" encoding="utf-8"?>\n<tv>\n'
                    fixed_xml += '\n'.join(channels)
                    fixed_xml += '\n'.join(programmes)
                    fixed_xml += '\n</tv>'
                    
                    parser = etree.XMLParser(recover=True)
                    tree = etree.fromstring(fixed_xml.encode("utf-8"), parser=parser)
                    return (True, tree, len(channels), len(programmes), attempt)
                    
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

    with ThreadPoolExecutor(max_workers=3) as executor:  # 减少线程数以避免内存问题
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
            wf_tree = etree.fromstring(wf_content.encode("utf-8"), parser)
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

    print(f"去重前: {len(all_channels)} 个频道, {len(all_programs)} 个节目")

    # ====================== 频道去重 ======================
    seen_channel_names = set()
    unique_channels = []
    channel_id_mapping = {}  # 存储原始频道ID到保留频道ID的映射
    
    for ch in all_channels:
        try:
            display_name_node = ch.find("display-name")
            if display_name_node is not None and display_name_node.text:
                channel_name = display_name_node.text.strip()
                channel_name_lower = channel_name.lower()
                
                # 清理频道ID
                channel_id = ch.get('id', '')
                
                if channel_name_lower not in seen_channel_names:
                    seen_channel_names.add(channel_name_lower)
                    unique_channels.append(ch)
                    
                    if channel_id:
                        channel_id_mapping[channel_name_lower] = channel_id
                else:
                    if channel_id and channel_name_lower in channel_id_mapping:
                        retained_id = channel_id_mapping[channel_name_lower]
                        channel_id_mapping[channel_id] = retained_id
            else:
                # 没有display-name的频道，直接保留
                unique_channels.append(ch)
        except Exception as e:
            print(f"⚠️ 处理频道时出错: {e}")
            continue
    
    print(f"去重后: {len(unique_channels)} 个频道")
    
    # ====================== 清理节目并更新频道ID ======================
    cleaned_programs = []
    for prog in all_programs:
        try:
            # 获取和清理属性
            old_channel_id = prog.get('channel', '')
            start = prog.get('start', '')
            stop = prog.get('stop', '')
            
            # 查找正确的频道ID
            new_channel_id = old_channel_id
            for old_id, retained_id in channel_id_mapping.items():
                if old_channel_id == old_id:
                    new_channel_id = retained_id
                    break
            
            # 检查节目是否有效
            if not start or not stop or not new_channel_id:
                continue
            
            # 清理开始和结束时间格式
            if not re.match(r'^\d{14} \+0800$', start):
                continue
            if not re.match(r'^\d{14} \+0800$', stop):
                continue
            
            # 创建新的节目元素
            new_prog = etree.Element("programme", start=start, stop=stop, channel=new_channel_id)
            
            # 添加标题
            title_elem = prog.find("title")
            if title_elem is not None and title_elem.text:
                title = etree.SubElement(new_prog, "title", lang="zh")
                title.text = title_elem.text.strip()
            else:
                continue
                
            cleaned_programs.append(new_prog)
            
        except Exception as e:
            print(f"⚠️ 清理节目时出错: {e}")
            continue
    
    all_programs = cleaned_programs
    print(f"清理后: {len(all_programs)} 个有效节目")
    
    # ====================== 生成最终XML ======================
    try:
        final_root = etree.Element("tv")
        
        # 添加频道
        for ch in unique_channels:
            # 确保频道格式正确
            try:
                final_root.append(ch)
            except Exception as e:
                print(f"⚠️ 添加频道时出错: {e}")
                continue
        
        # 添加节目
        for prog in all_programs:
            try:
                final_root.append(prog)
            except Exception as e:
                print(f"⚠️ 添加节目时出错: {e}")
                continue
        
        # 生成XML
        xml_declaration = '<?xml version="1.0" encoding="utf-8"?>\n'
        xml_str = xml_declaration.encode('utf-8') + etree.tostring(final_root, encoding="utf-8", pretty_print=True)
        
        # 最终验证
        parser = etree.XMLParser(recover=True)
        etree.fromstring(xml_str, parser)
        
        output_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(output_path, "wb") as f:
            f.write(xml_str)
        
        # 计算文件大小
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"✅ 最终输出：频道 {len(unique_channels)} 个 | 节目 {len(all_programs)} 个")
        print(f"📦 文件大小：{file_size_mb:.2f} MB")
        print(f"📁 输出文件：{output_path}")
        
    except Exception as e:
        print(f"❌ 生成最终XML失败: {e}")
        import traceback
        traceback.print_exc()
        # 生成最小可用的XML
        output_path = os.path.join(OUTPUT_DIR, "epg.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(output_path, "wb") as f:
            f.write(empty_xml)
        print("⚠️ 已生成空的EPG文件")

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
