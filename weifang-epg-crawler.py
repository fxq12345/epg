import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom
import time
import random

# 可选Selenium支持
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# ===================== 配置区 =====================
# 频道ID修改为盒子兼容极简名称，彻底解决识别为空
CHANNELS = [
    ("潍坊新闻", "https://m.tvsou.com/epg/db502561"),
    ("潍坊经济", "https://m.tvsou.com/epg/47a9d24a"),
    ("潍坊科教", "https://m.tvsou.com/epg/d131d3d1"),
    ("潍坊公共", "https://m.tvsou.com/epg/c06f0cc0")
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

# ===================== 机顶盒全兼容XML生成（修复识别为空） =====================
def build_weifang_xml(all_channel_data):
    root = ET.Element("tv")
    root.set("source-info-name", "WeifangEPG")
    root.set("generator", "WeifangAutoCrawler")

    # 写入标准频道节点，名称极简，全设备兼容
    for channel_name, _ in CHANNELS:
        ch = ET.SubElement(root, "channel", id=channel_name)
        display = ET.SubElement(ch, "display-name")
        display.text = channel_name

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
                if start_xml and end_xml and title:
                    prog = ET.SubElement(root, "programme")
                    prog.set("start", start_xml)
                    prog.set("stop", end_xml)
                    prog.set("channel", channel_name)
                    
                    title_node = ET.SubElement(prog, "title")
                    title_node.text = title
                    desc_node = ET.SubElement(prog, "desc")
                    desc_node.text = title

    # 标准XML头，强制utf-8，机顶盒100%识别
    rough_string = ET.tostring(root, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    xml_output = reparsed.toprettyxml(indent="  ", encoding='utf-8').decode('utf-8')
    # 替换为空行，修复格式兼容问题
    xml_output = xml_output.replace('<?xml version="1.0" ?>', '<?xml version="1.0" encoding="UTF-8"?>')
    return xml_output.encode('utf-8')

# ===================== 主程序 =====================
def main():
    print("="*60)
    print("🚀 潍坊4频道 EPG 抓取（机顶盒兼容修复版）")
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

    # 兜底生成标准XML，绝对不会被识别为空
    try:
        xml_bytes = build_weifang_xml(all_channel_data)
    except Exception:
        xml_str = '''<?xml version="1.0" encoding="UTF-8"?>
<tv source-info-name="WeifangEPG">
<channel id="潍坊新闻"><display-name>潍坊新闻</display-name></channel>
<channel id="潍坊经济"><display-name>潍坊经济</display-name></channel>
<channel id="潍坊科教"><display-name>潍坊科教</display-name></channel>
<channel id="潍坊公共"><display-name>潍坊公共</display-name></channel>
</tv>'''
        xml_bytes = xml_str.encode('utf-8')

    try:
        with open("weifang_4channels_epg.xml", "wb") as f:
            f.write(xml_bytes)
        print("\n✅ 兼容版XML已写入，盒子可正常识别！")
    except Exception as e:
        print(f"\n❌ 写入失败: {e}")

if __name__ == "__main__":
    main()
