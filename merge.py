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

# 10分钟强制终止
signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm(600)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ====================== 你的原始频道列表 ======================
WEIFANG_CHANNELS = [
    (
        "潍坊新闻频道", 
        "https://m.tvsou.com/epg/db502561",
        "https://picsum.photos/seed/weifang-news/200/120"
    ),
    (
        "潍坊经济生活频道", 
        "https://m.tvsou.com/epg/47a9d24a",
        "https://picsum.photos/seed/weifang-econ/200/120"
    ),
    (
        "潍坊科教频道", 
        "https://m.tvsou.com/epg/d131d3d1",
        "https://picsum.photos/seed/weifang-sci/200/120"
    ),
    (
        "潍坊公共频道", 
        "https://m.tvsou.com/epg/c06f0cc0",
        "https://picsum.photos/seed/weifang-public/200/120"
    )
]

WEEK_DAY = ["w1", "w2", "w3", "w4", "w5", "w6", "w7"]
MAX_RETRY = 2

# ====================== 修改后的抓取逻辑（精准时间+防拦截） ======================

# --- 新增：增强的请求头 ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    # 模拟从搜索引擎点击进入，解决防盗链
    "Referer": "https://www.baidu.com/s?wd=潍坊电视台节目表" 
}

def crawl_weifang_single(ch_name, base_url, day_str, current_day):
    # 基于你的原始逻辑，但增加了请求头
    for attempt in range(1, MAX_RETRY + 1):
        try:
            url = f"{base_url}/{day_str}"
            print(f"尝试抓取 {ch_name} ({day_str}): {url}")
            
            # 发送请求
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.encoding = "utf-8"
            
            # 检查响应状态
            if resp.status_code != 200:
                print(f"状态码错误: {resp.status_code}")
                time.sleep(1)
                continue
                
            html = resp.text
            
            # 简单的反爬虫检查
            if "访问过于频繁" in html or "请输入验证码" in html:
                print(f"警告: {url} 触发反爬虫，尝试重试")
                time.sleep(3)
                continue
                
            soup = BeautifulSoup(html, "html.parser")
            
            program_list = []
            # 查找包含时间的元素，兼容多种标签
            # tvsou 的结构通常是 li 或 div 包含时间
            items = soup.find_all(["li", "div", "p"])
            
            for item in items:
                txt = item.get_text(strip=True)
                # 正则匹配时间格式，如 "08:00 节目名"
                match = re.match(r"(\d{1,2}:\d{2})\s*(.+)", txt)
                if not match:
                    continue
                time_str, title = match.groups()
                
                # 过滤无效数据
                if len(title) < 2 or "广告" in title or "测试卡" in title:
                    continue
                    
                # 构建准确的时间对象
                try:
                    hh, mm = map(int, time_str.split(":"))
                    prog_time = datetime.combine(current_day, datetime.min.time().replace(hour=hh, minute=mm))
                    program_list.append((prog_time, title))
                except ValueError:
                    continue
            
            # 如果没抓到数据，跳过
            if not program_list:
                print(f"警告: {url} 未找到有效节目数据")
                continue
                
            # 生成精准的开始和结束时间
            precise_programs = []
            for i in range(len(program_list)):
                start_time, title = program_list[i]
                if i == len(program_list) - 1:
                    # 最后一个节目，假设时长60分钟
                    stop_time = start_time + timedelta(minutes=60)
                else:
                    stop_time = program_list[i+1][0]
                
                start_xml = start_time.strftime("%Y%m%d%H%M%S +0800")
                stop_xml = stop_time.strftime("%Y%m%d%H%M%S +0800")
                precise_programs.append((start_xml, stop_xml, title))
            
            time.sleep(0.5) # 减少并发压力
            return precise_programs
            
        except Exception as e:
            print(f"抓取异常: {e}")
            time.sleep(1)
            continue
    return []

# ====================== 修改后的时间计算逻辑 ======================

def crawl_weifang():
    try:
        root = etree.Element("tv")
        
        # 1. 先生成频道节点
        for ch_name, base_url, icon_url in WEIFANG_CHANNELS:
            ch = etree.SubElement(root, "channel", id=ch_name)
            dn = etree.SubElement(ch, "display-name")
            dn.text = ch_name
            icon = etree.SubElement(ch, "icon", src=icon_url)

        # --- 关键修改：计算本周一作为基准 ---
        # 获取当前时间
        now = datetime.now()
        # 计算本周一的日期 (weekday() 返回 0-6, Monday is 0)
        # 使用 isoweekday() 返回 1-7, Monday is 1
        weekday = now.isoweekday() # 1=周一, 7=周日
        # 计算偏移量，将今天调整到本周一
        offset = weekday - 1
        # 得到本周一的日期对象
        monday = now - timedelta(days=offset)
        # 将时间归零 (时分秒设为00:00:00)
        monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 2. 循环抓取周一到周日 (w1 到 w7)
        for day_idx in range(7):
            # 计算当前循环对应的日期 (周一 + 天数偏移)
            current_day = monday + timedelta(days=day_idx)
            day_str = WEEK_DAY[day_idx] # w1, w2, ... w7
            
            for ch_name, base_url, _ in WEIFANG_CHANNELS:
                programs = crawl_weifang_single(ch_name, base_url, day_str, current_day)
                for start, stop, title in programs:
                    prog = etree.SubElement(root, "programme", start=start, stop=stop, channel=ch_name)
                    t = etree.SubElement(prog, "title")
                    t.text = title

        # 仅生成 gz，不生成 xml
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        xml_content = etree.tostring(root, encoding="utf-8", pretty_print=True)
        with gzip.open(wf_path, "wb") as f:
            f.write(xml_content)
        return wf_path
        
    except Exception as e:
        print(f"主抓取流程错误: {e}")
        # 失败也写入空gz
        wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
        return wf_path

# ====================== 你原有的其他函数保持不变 ======================

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

            content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content).replace("& ", "&amp; ")
            tree = etree.fromstring(content.encode("utf-8"))
            ch = len(tree.xpath("//channel"))
            pg = len(tree.xpath("//programme"))
            if ch > 0 and pg > 0:
                return (True, tree, ch, pg, attempt)
        except:
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
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

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

    # ====================== 读取潍坊 gz 文件合并 ======================
    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}（时间精准匹配+酷9图标）")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
        else:
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except:
        print("⚠️ 潍坊本地源读取失败，已跳过")

    # 最终只生成 epg.gz，删除明文xml输出
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for p in all_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True)
    # 仅输出压缩包，无xml文件
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str)

# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        wf_gz = crawl_weifang()
        merge_all(wf_gz)
    except Exception as e:
        print(f"程序运行时发生错误: {e}")
