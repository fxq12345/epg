import requests
import random
import time
import re
import os
from bs4 import Beautifuimport requests
import random
import time
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 潍坊频道配置
weifang_channels = [
    {"id": "SDWF-SDWF1", "name": "潍坊新闻综合频道", "url": "https://m.tvmao.com/program/SDWF-SDWF1-w3.html"},
    {"id": "SDWF-SDWF3", "name": "潍坊生活频道", "url": "https://m.tvmao.com/program/SDWF-SDWF3-w3.html"},
    {"id": "SDWF-SDWF2", "name": "潍坊公共频道", "url": "https://m.tvmao.com/program/SDWF-SDWF2-w3.html"},
    {"id": "SDWF-SDWF4", "name": "潍坊科教频道", "url": "https://m.tvmao.com/program/SDWF-SDWF4-w3.html"}
]

def extract_week_offset(url):
    match = re.search(r'w(\d+)\.html', url)
    return int(match.group(1)) if match else 1

def get_target_date(week_offset):
    today = datetime.now().date()
    return (today + timedelta(days=week_offset - 1)).strftime("%Y-%m-%d")

def crawl_channel_epg(channel):
    epg_data = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        week_offset = extract_week_offset(channel["url"])
        calc_date = get_target_date(week_offset)
        print(f"📅 {channel['name']} - 计算日期：{calc_date}（w{week_offset}）")
        
        response = requests.get(channel["url"], headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 修复：重新定位页面日期（适配tvmao新结构）
        date_elem = soup.find("div", class_="program_date") or soup.find("h1", class_="program_date")
        use_date = date_elem.text.strip()[:10] if date_elem else calc_date
        print(f"✅ 使用日期：{use_date}")
        
        # 修复：重新定位节目元素（适配tvmao新结构）
        for item in soup.find_all("li", class_="program_item"):  # 替换为li.program_item
            time_elem = item.find("span", class_="time")
            title_elem = item.find("span", class_="name")
            if time_elem and title_elem:
                time_str = time_elem.text.strip()
                title = title_elem.text.strip()
                start_time = datetime.strptime(f"{use_date} {time_str}", "%Y-%m-%d %H:%M")
                epg_data.append({
                    "channel_id": channel["id"],
                    "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                    "title": title
                })
        
        time.sleep(1 + random.random()*1.5)
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    except Exception as e:
        print(f"❌ {channel['name']}爬取失败：{str(e)}\n")
    return epg_data

def generate_xmltv_file(epg_data, channels):
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    tv = ET.Element("tv", {
        "source-info-url": "https://m.tvmao.com",
        "source-info-name": "TVmao-潍坊EPG",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
    for prog in epg_data:
        prog_elem = ET.SubElement(tv, "programme", {"start": prog["start"], "channel": prog["channel_id"]})
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    with open("output/weifang.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"🎉 潍坊EPG生成：output/weifang.xml（{len(epg_data)}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60 + "\n潍坊EPG爬虫启动\n" + "="*60)
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    generate_xmltv_file(all_epg, weifang_channels)
lSoup
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom

# 潍坊频道配置（绑定精准链接，名称含“潍坊”适配merge.py分类）
weifang_channels = [
    {
        "id": "SDWF-SDWF1",
        "name": "潍坊新闻综合频道",
        "url": "https://m.tvmao.com/program/SDWF-SDWF1-w3.html"
    },
    {
        "id": "SDWF-SDWF3",
        "name": "潍坊生活频道",
        "url": "https://m.tvmao.com/program/SDWF-SDWF3-w3.html"
    },
    {
        "id": "SDWF-SDWF2",
        "name": "潍坊公共频道",
        "url": "https://m.tvmao.com/program/SDWF-SDWF2-w3.html"
    },
    {
        "id": "SDWF-SDWF4",
        "name": "潍坊科教频道",
        "url": "https://m.tvmao.com/program/SDWF-SDWF4-w3.html"
    }
]

# 从链接提取周偏移量（w3→3）
def extract_week_offset(url):
    match = re.search(r'w(\d+)\.html', url)
    return int(match.group(1)) if match else 1

# 计算目标日期（w1=今日，w2=明日...）
def get_target_date(week_offset):
    today = datetime.now().date()
    target_date = today + timedelta(days=week_offset - 1)
    return target_date.strftime("%Y-%m-%d")

# 爬取单个频道节目单
def crawl_channel_epg(channel):
    epg_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        week_offset = extract_week_offset(channel["url"])
        calc_date = get_target_date(week_offset)
        print(f"📅 {channel['name']} - 计算日期：{calc_date}（w{week_offset}）")
        
        # 发送请求
        response = requests.get(channel["url"], headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 验证页面日期
        date_elem = soup.find("div", class_="program_date")
        if date_elem:
            page_date = date_elem.text.strip()[:10]
            print(f"✅ 页面日期：{page_date}（使用页面日期）")
            use_date = page_date
        else:
            print(f"⚠️ 未找到页面日期，使用计算日期：{calc_date}")
            use_date = calc_date
        
        # 提取节目数据
        for item in soup.find_all("div", class_="p_item"):
            time_elem = item.find("span", class_="p_time")
            title_elem = item.find("a", class_="p_name")
            if time_elem and title_elem:
                time_str = time_elem.text.strip()
                title = title_elem.text.strip()
                start_time = datetime.strptime(f"{use_date} {time_str}", "%Y-%m-%d %H:%M")
                epg_data.append({
                    "channel_id": channel["id"],
                    "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                    "title": title
                })
        
        time.sleep(1 + random.random()*1.5)  # 反爬延时
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    except Exception as e:
        print(f"❌ {channel['name']}爬取失败：{str(e)}\n")
    return epg_data

# 生成标准XMLTV格式EPG文件（输出到output目录，适配merge.py读取）
def generate_xmltv_file(epg_data, channels):
    tv = ET.Element("tv", {
        "source-info-url": "https://m.tvmao.com",
        "source-info-name": "TVmao-潍坊专属EPG",
        "generator-info-name": "潍坊EPG自动爬虫",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    
    # 添加频道信息（ID和名称适配merge.py的分类与去重逻辑）
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        ET.SubElement(chan_elem, "url").text = channel["url"]
    
    # 添加节目信息（格式符合XMLTV标准，确保merge.py可解析）
    for prog in epg_data:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    # 确保output目录存在，避免报错
    os.makedirs("output", exist_ok=True)
    # 格式化XML输出，便于merge.py解析
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    with open("output/weifang.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"🎉 潍坊EPG生成完成：output/weifang.xml（共{len(epg_data)}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60)
    print("潍坊EPG自动爬虫启动（每日自动更新）")
    print("="*60 + "\n")
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    generate_xmltv_file(all_epg, weifang_channels)
