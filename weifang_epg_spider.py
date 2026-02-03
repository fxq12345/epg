import requests
import random
import time
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道", "url": "https://m.tvsou.com/epg/db502561/"},
    {"id": "1002", "name": "潍坊生活频道", "url": "https://m.tvsou.com/epg/db502563/"},
    {"id": "1003", "name": "潍坊公共频道", "url": "https://m.tvsou.com/epg/db502562/"},
    {"id": "1004", "name": "潍坊科教频道", "url": "https://m.tvsou.com/epg/db502564/"}
]

def get_current_date():
    return datetime.now().date().strftime("%Y-%m-%d")

def crawl_channel_epg(channel):
    epg_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://m.tvsou.com/"
    }
    current_date = get_current_date()
    try:
        print(f"📅 {channel['name']}（ID：{channel['id']}）- 爬取日期：{current_date}")
        response = requests.get(channel["url"], headers=headers, timeout=20, allow_redirects=True)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 适配最新页面：节目容器为div.program-list-container
        program_container = soup.find("div", class_="program-list-container")
        if not program_container:
            program_container = soup.find("div", class_="program-content")
            if not program_container:
                print(f"⚠️  未找到节目容器：{channel['name']}")
                return epg_data
        
        # 节目项为div.program-item-new
        program_items = program_container.find_all("div", class_="program-item-new")
        if not program_items:
            program_items = program_container.find_all("li", class_="program-item")
            if not program_items:
                print(f"⚠️  无节目数据：{channel['name']}")
                return epg_data
        
        for item in program_items:
            time_elem = item.find("span", class_="program-time")
            title_elem = item.find("span", class_="program-name")
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
        
        time.sleep(2 + random.random() * 3)
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络错误：{channel['name']} | {str(e)}\n")
    except Exception as e:
        print(f"❌ 爬取异常：{channel['name']} | {str(e)}\n")
    return epg_data

def generate_xmltv_file(epg_data, channels):
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    tv = ET.Element("tv", {
        "source-info-url": "https://m.tvsou.com",
        "source-info-name": "TVSou-潍坊EPG（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800")
    })
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        channel_epg = [prog for prog in epg_data if prog["channel_id"] == channel["id"]]
        for prog in channel_epg:
            prog_elem = ET.SubElement(tv, "programme", {
                "start": prog["start"],
                "channel": channel["id"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    xml_str = os.linesep.join([line for line in xml_str.splitlines() if line.strip()])
    output_path = "output/weifang.xml"
    with open(output_path, "w", encoding="gbk") as f:
        f.write(xml_str)
    print(f"🎉 潍坊EPG生成完成：{output_path}（{len(epg_data)}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60 + "\n潍坊搜视网EPG爬虫（最新适配）启动\n" + "="*60)
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    if all_epg:
        generate_xmltv_file(all_epg, weifang_channels)
    else:
        print("⚠️  未爬取到数据，生成空文件")
        os.makedirs("output", exist_ok=True)
        with open("output/weifang.xml", "w", encoding="gbk") as f:
            f.write('<tv></tv>')
