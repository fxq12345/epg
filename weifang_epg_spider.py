import requests
import random
import time
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 潍坊频道配置（酷9专属：纯数字ID）
weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道", "url": "https://m.tvsou.com/epg/db502561//"},
    {"id": "1002", "name": "潍坊生活频道", "url": "https://m.tvsou.com/epg/db502563/"},
    {"id": "1003", "name": "潍坊公共频道", "url": "https://m.tvsou.com/epg/db502562/"},
    {"id": "1004", "name": "潍坊科教频道", "url": "https://m.tvsou.com/epg/db502564/"}
]

def get_current_date():
    return datetime.now().date().strftime("%Y-%m-%d")

def crawl_channel_epg(channel):
    epg_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://m.tvsou.com/"
    }
    current_date = get_current_date()
    try:
        print(f"📅 {channel['name']}（ID：{channel['id']}）- 爬取日期：{current_date}")
        response = requests.get(channel["url"], headers=headers, timeout=20, allow_redirects=True)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")
        
        program_container = soup.find("div", class_="epg-list")
        if not program_container:
            print(f"⚠️  未找到{channel['name']}的节目容器")
            return epg_data
        
        program_items = program_container.find_all("li", class_="epg-item")
        if not program_items:
            print(f"⚠️  {channel['name']}暂无公开节目数据")
            return epg_data
        
        for item in program_items:
            time_elem = item.find("span", class_="epg-time")
            title_elem = item.find("span", class_="epg-name")
            if time_elem and title_elem:
                time_str = time_elem.text.strip()
                title = title_elem.text.strip()
                if re.match(r"^\d{2}:\d{2}$", time_str) and title:
                    try:
                        start_time = datetime.strptime(f"{current_date} {time_str}", "%Y-%m-%d %H:%M")
                        epg_data.append({
                            "channel_id": channel["id"],
                            "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                            "title": title
                        })
                    except ValueError:
                        continue
        
        time.sleep(1.5 + random.random() * 2)
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    except requests.exceptions.RequestException as e:
        print(f"❌ {channel['name']}网络错误：{str(e)}\n")
    except Exception as e:
        print(f"❌ {channel['name']}爬取异常：{str(e)}\n")
    return epg_data

def generate_xmltv_file(epg_data, channels):
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    tv = ET.Element("tv", {
        "source-info-url": "https://m.tvsou.com",
        "source-info-name": "TVSou-潍坊EPG（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800"),
        "generator-info-name": "WeifangEPGCrawler-Ku9"
    })
    
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        ET.SubElement(chan_elem, "icon", {"src": f"https://icon.tvsou.com/{channel['id']}.png"})
    
    for prog in epg_data:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    output_path = "output/weifang.xml"
    
    # 酷9适配：保存为GBK编码
    with open(output_path, "w", encoding="gbk") as f:
        f.write(xml_str)
    
    print(f"🎉 潍坊EPG生成完成（酷9适配）：{output_path}（共{len(epg_data)}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60 + "\n潍坊搜视网EPG爬虫（酷9适配）启动\n" + "="*60)
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    if all_epg:
        generate_xmltv_file(all_epg, weifang_channels)
    else:
        print("⚠️  未爬取到任何节目数据，生成空文件避免报错")
        os.makedirs("output", exist_ok=True)
        with open("output/weifang.xml", "w", encoding="gbk") as f:
            f.write('<tv></tv>')
