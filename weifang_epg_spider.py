import requests
import random
import time
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 潍坊频道配置（酷9专属：纯数字ID + 最新搜视网链接）
weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道", "url": "https://m.tvsou.com/epg/db502561/"},
    {"id": "1002", "name": "潍坊生活频道", "url": "https://m.tvsou.com/epg/db502563/"},
    {"id": "1003", "name": "潍坊公共频道", "url": "https://m.tvsou.com/epg/db502562/"},
    {"id": "1004", "name": "潍坊科教频道", "url": "https://m.tvsou.com/epg/db502564/"}
]

def get_current_date():
    """获取当前日期，格式YYYY - MM - DD"""
    return datetime.now().date().strftime("%Y-%m-%d")

def crawl_channel_epg(channel):
    epg_data = []
    # 完善请求头，模拟手机端访问（适配m.tvsou.com）
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://m.tvsou.com/",
        "X-Requested-With": "XMLHttpRequest"
    }
    current_date = get_current_date()
    try:
        print(f"📅 {channel['name']}（ID：{channel['id']}）- 爬取日期：{current_date}")
        
        response = requests.get(channel["url"], headers=headers, timeout=20, allow_redirects=True)
        response.raise_for_status()
        # 解决编码问题（适配手机端页面）
        response.encoding = response.apparent_encoding or 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 适配m.tvsou.com最新页面结构：节目容器为ul.program-list
        program_container = soup.find("ul", class_="program-list")
        if not program_container:
            # 备用容器：div.program-content
            program_container = soup.find("div", class_="program-content")
            if not program_container:
                print(f"⚠️  未找到{channel['name']}的节目容器，可能页面结构变更")
                return epg_data
        
        # 提取节目项：li.program-item（手机端最新结构）
        program_items = program_container.find_all("li", class_="program-item")
        if not program_items:
            # 备用节目项：div.program-item
            program_items = program_container.find_all("div", class_="program-item")
            if not program_items:
                print(f"⚠️  {channel['name']}暂无公开节目数据")
                return epg_data
        
        for item in program_items:
            # 适配手机端：时间为span.time，标题为span.name
            time_elem = item.find("span", class_="time")
            title_elem = item.find("span", class_="name")
            
            if time_elem and title_elem:
                time_str = time_elem.text.strip()
                title = title_elem.text.strip()
                # 过滤无效数据（时间格式需为HH:MM，标题非空）
                if re.match(r"^\d{2}:\d{2}$", time_str) and title:
                    try:
                        start_time = datetime.strptime(f"{current_date} {time_str}", "%Y-%m-%d %H:%M")
                        epg_data.append({
                            "channel_id": channel["id"],
                            "start": start_time.strftime("%Y%m%d%H%M%S +0800"),
                            "title": title
                        })
                    except ValueError:
                        print(f"⚠️  无效时间格式：{time_str}，已跳过")
                        continue
        
        # 随机延时防反爬（手机端更严格）
        time.sleep(2 + random.random() * 3)
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    except requests.exceptions.RequestException as e:
        print(f"❌ {channel['name']}网络请求失败：{str(e)}\n")
    except Exception as e:
        print(f"❌ {channel['name']}爬取异常：{str(e)}\n")
    return epg_data

def generate_xmltv_file(epg_data, channels):
    """整合潍坊频道的EPG数据并生成XML文件（酷9适配）"""
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    tv = ET.Element("tv", {
        "source-info-url": "https://m.tvsou.com",
        "source-info-name": "TVSou-潍坊EPG（酷9适配）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800"),
        "generator-info-name": "WeifangEPGCrawler-Ku9"
    })

    # 整合每个频道的EPG数据
    for channel in channels:
        # 添加频道信息
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        
        # 提取该频道的节目信息并添加
        channel_epg = [prog for prog in epg_data if prog["channel_id"] == channel["id"]]
        for prog in channel_epg:
            prog_elem = ET.SubElement(tv, "programme", {
                "start": prog["start"],
                "channel": channel["id"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]

    # 保存文件（酷9适配：GBK编码）
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    xml_str = os.linesep.join([line for line in xml_str.splitlines() if line.strip()])
    output_path = "output/weifang.xml"
    
    with open(output_path, "w", encoding="gbk") as f:
        f.write(xml_str)
    
    # 统计节目数量
    programme_count = len(epg_data)
    print(f"🎉 潍坊EPG生成完成（酷9适配）：{output_path}（{programme_count}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60 + "\n潍坊搜视网EPG爬虫（酷9适配）启动\n" + "="*60)
    # 遍历所有频道爬取数据
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    # 生成XML文件（仅当有有效数据时）
    if all_epg:
        generate_xmltv_file(all_epg, weifang_channels)
    else:
        print("⚠️  未爬取到任何节目数据，生成空文件避免报错")
        os.makedirs("output", exist_ok=True)
        with open("output/weifang.xml", "w", encoding="gbk") as f:
            f.write('<tv></tv>')
