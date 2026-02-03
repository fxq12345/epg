import requests
import random
import time
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 搜视网潍坊频道配置（经验证的有效链接）
weifang_channels = [
    {"id": "SDWF-SDWF1", "name": "潍坊新闻综合频道", "url": "https://www.tvsou.com/epg/db502561/"},
    {"id": "SDWF-SDWF3", "name": "潍坊生活频道", "url": "https://www.tvsou.com/epg/db502563/"},
    {"id": "SDWF-SDWF2", "name": "潍坊公共频道", "url": "https://www.tvsou.com/epg/db502562/"},
    {"id": "SDWF-SDWF4", "name": "潍坊科教频道", "url": "https://www.tvsou.com/epg/db502564/"}
]

def get_current_date():
    """获取当前日期（格式：YYYY-MM-DD）"""
    return datetime.now().date().strftime("%Y-%m-%d")

def crawl_channel_epg(channel):
    epg_data = []
    # 强化请求头，降低反爬概率
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://www.tvsou.com/",
        "Connection": "keep-alive"
    }
    current_date = get_current_date()
    try:
        print(f"📅 {channel['name']} - 爬取日期：{current_date}")
        
        # 发起请求（支持重定向）
        response = requests.get(channel["url"], headers=headers, timeout=20, allow_redirects=True)
        response.raise_for_status()  # 抛出HTTP错误
        response.encoding = response.apparent_encoding or "utf-8"  # 自动适配编码
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 定位节目列表容器（搜视网核心结构）
        program_container = soup.find("div", class_="epg-list")
        if not program_container:
            print(f"⚠️  未找到{channel['name']}的节目容器，可能页面结构变更")
            return epg_data
        
        # 提取所有节目项
        program_items = program_container.find_all("li", class_="epg-item")
        if not program_items:
            print(f"⚠️  {channel['name']}暂无公开节目数据")
            return epg_data
        
        # 解析节目时间和标题
        for item in program_items:
            time_elem = item.find("span", class_="epg-time")
            title_elem = item.find("span", class_="epg-name")
            
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
        
        # 随机延时防反爬（1.5-3.5秒）
        time.sleep(1.5 + random.random() * 2)
        print(f"📊 爬取完成：{len(epg_data)}条节目\n")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ {channel['name']}网络错误：{str(e)}\n")
    except Exception as e:
        print(f"❌ {channel['name']}爬取异常：{str(e)}\n")
    
    return epg_data

def generate_xmltv_file(epg_data, channels):
    """生成符合XMLTV标准的潍坊EPG文件"""
    import xml.etree.ElementTree as ET
    from xml.dom import minidom
    
    # 根节点配置
    tv = ET.Element("tv", {
        "source-info-url": "https://www.tvsou.com",
        "source-info-name": "TVSou-潍坊EPG",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800"),
        "generator-info-name": "WeifangEPGCrawler"
    })
    
    # 添加频道信息
    for channel in channels:
        chan_elem = ET.SubElement(tv, "channel", {"id": channel["id"]})
        ET.SubElement(chan_elem, "display-name").text = channel["name"]
        ET.SubElement(chan_elem, "icon", {"src": f"https://icon.tvsou.com/{channel['id']}.png"})  # 图标占位
    
    # 添加节目信息
    for prog in epg_data:
        prog_elem = ET.SubElement(tv, "programme", {
            "start": prog["start"],
            "channel": prog["channel_id"]
        })
        ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
    
    # 保存文件
    os.makedirs("output", exist_ok=True)
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])  # 去除空行
    output_path = "output/weifang.xml"  # 统一文件名，便于merge.py读取
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    
    print(f"🎉 潍坊EPG生成完成：{output_path}（共{len(epg_data)}条节目）")

if __name__ == "__main__":
    all_epg = []
    print("="*60 + "\n潍坊搜视网EPG爬虫启动\n" + "="*60)
    
    # 批量爬取所有频道
    for channel in weifang_channels:
        all_epg.extend(crawl_channel_epg(channel))
    
    # 生成XML文件（仅当有有效数据时）
    if all_epg:
        generate_xmltv_file(all_epg, weifang_channels)
    else:
        print("⚠️  未爬取到有效节目数据，跳过文件生成")
        # 生成空文件避免merge.py报错
        os.makedirs("output", exist_ok=True)
        with open("output/weifang.xml", "w", encoding="utf-8") as f:
            f.write('<tv></tv>')
