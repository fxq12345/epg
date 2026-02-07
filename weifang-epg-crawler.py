import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom
import time
import random

# --- 尝试导入Selenium ---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("ℹ️ 提示: 未安装selenium。若requests失效，建议安装以获得更高成功率。")

# ===================== 配置区 =====================
CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_MAP = {"周一": "w1", "周二": "w2", "周三": "w3", "周四": "w4", "周五": "w5", "周六": "w6", "周日": "w7"}

# === 关键配置：使用必应搜索作为Referer，解决防盗链问题 ===
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com/search?q=%E7%94%B5%E8%A7%86%E8%8A%82%E7%9B%AE%E8%A1%A8" # 模拟从必应搜索"电视节目表"进入
}

# ===================== 工具函数 =====================
def time_to_xmltv(base_date, time_str):
    try:
        hh, mm = time_str.strip().split(":")
        dt = datetime.combine(base_date, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
        return dt.strftime("%Y%m%d%H%M%S +0800")
    except:
        return ""

def get_page_html(url):
    """获取网页内容，优先requests，失败则尝试Selenium"""
    
    # --- 第一优先级：Requests (速度快，且已配置必应Referer) ---
    try:
        print(f"📡 尝试请求: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = 'utf-8'
        html = resp.text
        
        # 简单验证：检查是否包含节目单关键词
        if "节目单" in html or "节目预告" in html or len(re.findall(r'\d{1,2}:\d{2}', html)) > 5:
            print("✅ Requests 获取成功")
            return html
        else:
            print("⚠️ Requests 获取内容无效，准备切换Selenium...")
            
    except Exception as e:
        print(f"❌ Requests 错误: {e}")

    # --- 第二优先级：Selenium (模拟浏览器) ---
    if SELENIUM_AVAILABLE:
        print("📱 启动Selenium模拟浏览器...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        # 关键：Selenium也需要设置Referer，虽然较难设置，但浏览器环境本身更可信
        chrome_options.add_argument("--referer=https://www.bing.com")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            time.sleep(3 + random.random()) # 随机等待，模拟人工
            html = driver.page_source
            driver.quit()
            print("✅ Selenium 获取成功")
            return html
        except Exception as e:
            print(f"❌ Selenium 错误: {e}")
    
    return ""

def get_day_program(channel_name, channel_base_url, week_name, w_suffix):
    # 修正URL拼接: 确保基础链接和后缀之间有斜杠
    if channel_base_url.endswith('/'):
        url = f"{channel_base_url}{w_suffix}"
    else:
        url = f"{channel_base_url}/{w_suffix}"
    
    programs = []
    try:
        html = get_page_html(url)
        if not html:
            return programs
            
        soup = BeautifulSoup(html, "html.parser")
        
        # 策略1: 查找常见的节目项class
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I))
        
        if not items: # 如果没找到，尝试查找li列表
            items = soup.find_all("li")
            
        for item in items:
            item_text = item.get_text(strip=True)
            # 使用正则提取时间+标题
            match = re.search(r'(\d{1,2}:\d{2})\s*(.+)', item_text)
            if match:
                t, title = match.groups()
                if len(title) > 1 and '广告' not in title and '报时' not in title: # 过滤无效项
                    programs.append((t.strip(), title.strip()))
        
        # 去重并排序
        programs = sorted(list(set(programs)), key=lambda x: x[0])
        print(f"✅ {channel_name} - {week_name}: {len(programs)} 条")
        
    except Exception as e:
        print(f"❌ 抓取失败 {week_name}: {e}")
    
    return programs

# ===================== 生成XML =====================
def build_weifang_xml(all_channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "潍坊搜视网EPG")
    
    # 1. 生成频道节点
    for channel_name, _ in CHANNELS:
        ch = ET.SubElement(root, "channel", id=channel_name)
        ET.SubElement(ch, "display-name", lang="zh").text = channel_name

    # 2. 填充节目数据
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())

    for channel_name, week_data_list in all_channel_data.items():
        for i, (week_name, w_suffix, progs) in enumerate(week_data_list):
            current_date = monday + timedelta(days=i)
            for idx in range(len(progs)):
                start_time_str, title = progs[idx]
                
                # 计算结束时间
                if idx < len(progs) - 1:
                    end_time_str = progs[idx+1][0]
                else:
                    end_time_str = (datetime.strptime(start_time_str, "%H:%M") + timedelta(minutes=30)).strftime("%H:%M")
                
                start_xmltv = time_to_xmltv(current_date, start_time_str)
                end_xmltv = time_to_xmltv(current_date, end_time_str)
                
                if start_xmltv and end_xmltv:
                    prog = ET.SubElement(root, "programme")
                    prog.set("start", start_xmltv)
                    prog.set("stop", end_xmltv)
                    prog.set("channel", channel_name)
                    
                    ET.SubElement(prog, "title", lang="zh").text = title
                    ET.SubElement(prog, "desc", lang="zh").text = f"{start_time_str} - {title}"
    
    # 格式化输出
    rough_string = ET.tostring(root, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8")

# ===================== 主程序 =====================
def main():
    print("="*50)
    print("潍坊4频道EPG抓取器 (必应Referer版)")
    print("="*50)
    
    all_channel_data = {}
    
    for channel_name, base_url in CHANNELS:
        print(f"\n{'-'*40}")
        print(f"📡 频道: {channel_name}")
        week_data = []
        
        for week_name, w_suffix in WEEK_MAP.items():
            progs = get_day_program(channel_name, base_url, week_name, w_suffix)
            week_data.append((week_name, w_suffix, progs))
            # 随机延时，防止被封
            time.sleep(1 + random.random() * 2)
            
        all_channel_data[channel_name] = week_data
    
    # 生成文件
    try:
        xml_bytes = build_weifang_xml(all_channel_data)
        with open("weifang_epg.xml", "wb") as f:
            f.write(xml_bytes)
        print(f"\n🎉 抓取完成！文件已保存。")
    except Exception as e:
        print(f"❌ 保存失败: {e}")

if __name__ == "__main__":
    main()
