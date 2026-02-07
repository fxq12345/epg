import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom
import time
import random

# 可选 Selenium 支持
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# ===================== 配置区 =====================
CHANNELS = [
    ("潍坊新闻频道", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济生活频道", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教频道", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共频道", "https://m.tvsou.com/epg/c06f0cc0")
]

WEEK_MAP = {
    "周一": "w1",
    "周二": "w2",
    "周三": "w3",
    "周四": "w4",
    "周五": "w5",
    "周六": "w6",
    "周日": "w7"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Referer": "https://www.bing.com/search?q=%E7%94%B5%E8%A7%86%E8%8A%82%E7%9B%AE%E8%A1%A8"
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
    try:
        print(f"📡 请求: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.encoding = 'utf-8'
        html = resp.text
        if "节目单" in html or "节目预告" in html or len(re.findall(r'\d{1,2}:\d{2}', html)) > 5:
            print("✅ Requests 成功")
            return html
        else:
            print("⚠️ 内容异常，切换备用模式")
    except Exception as e:
        print(f"❌ Requests 失败: {e}")

    if SELENIUM_AVAILABLE:
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            time.sleep(3 + random.random())
            html = driver.page_source
            driver.quit()
            print("✅ Selenium 成功")
            return html
        except Exception as e:
            print(f"❌ Selenium 失败: {e}")
    return ""

def get_day_program(channel_name, channel_base_url, week_name, w_suffix):
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
        items = soup.find_all("div", class_=re.compile("program-item|time-item", re.I))
        if not items:
            items = soup.find_all("li")

        for item in items:
            text = item.get_text(strip=True)
            match = re.search(r'(\d{1,2}:\d{2})\s*(.+)', text)
            if match:
                t, title = match.groups()
                if len(title) > 1 and '广告' not in title and '报时' not in title:
                    programs.append((t.strip(), title.strip()))

        programs = sorted(list(set(programs)), key=lambda x: x[0])
        print(f"✅ {channel_name} {week_name}: {len(programs)} 条")
    except Exception as e:
        print(f"❌ {week_name} 异常: {e}")
    return programs

# ===================== XML 生成 =====================
def build_weifang_xml(all_channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "潍坊四频道EPG自动抓取")

    # 频道信息
    for channel_name, _ in CHANNELS:
        ch = ET.SubElement(root, "channel", id=channel_name)
        ET.SubElement(ch, "display-name", lang="zh").text = channel_name

    today = datetime.now()
    monday = today - timedelta(days=today.weekday())

    for channel_name, week_data_list in all_channel_data.items():
        for i, (week_name, w_suffix, progs) in enumerate(week_data_list):
            current_date = monday + timedelta(days=i)
            for idx in range(len(progs)):
                start_time_str, title = progs[idx]
                if idx < len(progs) - 1:
                    end_time_str = progs[idx+1][0]
                else:
                    end_time_str = (datetime.strptime(start_time_str, "%H:%M") + timedelta(minutes=30)).strftime("%H:%M")

                start_xml = time_to_xmltv(current_date, start_time_str)
                end_xml = time_to_xmltv(current_date, end_time_str)
                if start_xml and end_xml:
                    prog = ET.SubElement(root, "programme")
                    prog.set("start", start_xml)
                    prog.set("stop", end_xml)
                    prog.set("channel", channel_name)
                    ET.SubElement(prog, "title", lang="zh").text = title
                    ET.SubElement(prog, "desc", lang="zh").text = f"{channel_name} {start_time_str} {title}"

    rough_string = ET.tostring(root, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8")

# ===================== 主程序（强制写入，失败也生成空文件保证提交） =====================
def main():
    print("="*60)
    print("🚀 潍坊4频道 EPG 抓取（强制覆盖版）")
    print("="*60)

    all_channel_data = {}
    for channel_name, base_url in CHANNELS:
        print(f"\n--- {channel_name} ---")
        week_data = []
        for week_name, w_suffix in WEEK_MAP.items():
            progs = get_day_program(channel_name, base_url, week_name, w_suffix)
            week_data.append((week_name, w_suffix, progs))
            time.sleep(1 + random.random()*1.5)
        all_channel_data[channel_name] = week_data

    # 强制写入：成功写正常XML，失败写最小合法XML，确保文件一定存在
    try:
        xml_bytes = build_weifang_xml(all_channel_data)
    except:
        xml_bytes = b'<?xml version="1.0"?>\n<tv source-info-name="潍坊EPG-异常备用"></tv>\n'

    try:
        with open("weifang_4channels_epg.xml", "wb") as f:
            f.write(xml_bytes)
        print("\n✅ 文件已强制写入：weifang_4channels_epg.xml")
    except Exception as e:
        print(f"\n❌ 写入失败（致命）: {e}")

if __name__ == "__main__":
    main()
