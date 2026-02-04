import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom
import datetime
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道", "alias": "潍坊新闻"},
    {"id": "1002", "name": "潍坊经济生活频道", "alias": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共频道", "alias": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化频道", "alias": "潍坊科教文化"},
    {"id": "1008", "name": "寿光蔬菜频道", "alias": "寿光蔬菜"},
    {"id": "1009", "name": "昌乐综合频道", "alias": "昌乐综合"},
    {"id": "1011", "name": "奎文娱乐频道", "alias": "奎文娱乐"}
]

def generate_epg_xml(programmes):
    if not programmes:
        logging.warning("⚠️ 无节目数据，跳过生成XML")
        return False  # 无数据时返回False
    
    root = ET.Element("tv")
    root.set("generator-info-name", "潍坊EPG抓取脚本（基于闪电新闻）")
    
    for channel in weifang_channels:
        channel_elem = ET.SubElement(root, "channel")
        channel_elem.set("id", channel["id"])
        name_elem = ET.SubElement(channel_elem, "display-name", lang="zh-CN")
        name_elem.text = channel["name"]
        alias_elem = ET.SubElement(channel_elem, "display-name", lang="zh-CN")
        alias_elem.text = channel["alias"]
    
    for prog in programmes:
        programme_elem = ET.SubElement(root, "programme", channel=prog["channel_id"], start=prog["start"], stop=prog["stop"])
        ET.SubElement(programme_elem, "title", lang="zh-CN").text = prog["title"]
        if prog.get("desc"):
            ET.SubElement(programme_elem, "desc", lang="zh-CN").text = prog["desc"]
    
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    xml_str = '\n'.join([line for line in xml_str.split('\n') if line.strip()])
    with open("weifang_epg.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    logging.info("✅ 潍坊EPG节目单已生成：weifang_epg.xml")
    return True

def crawl_weifang_epg():
    programmes = []
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Referer": "https://sd.iqilu.com/",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://sd.iqilu.com",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    for day_offset in range(3):
        target_date = (datetime.date.today() + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")
        logging.info(f"📅 正在抓取 {target_date} 节目单...")
        
        for channel in weifang_channels:
            url = f"https://sd.iqilu.com/api/tv/program?channel={channel['alias']}&date={target_date}"
            
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                if "application/json" not in response.headers.get("Content-Type", ""):
                    logging.warning(f"⚠️ {channel['name']} 接口返回非JSON数据，跳过")
                    continue
                
                data = response.json()
                if not data.get("data"):
                    logging.warning(f"⚠️ {channel['name']} {target_date} 无节目数据")
                    continue
                
                for prog in data["data"]:
                    try:
                        start_dt = datetime.datetime.strptime(prog["start_time"], "%Y-%m-%d %H:%M:%S")
                        stop_dt = datetime.datetime.strptime(prog["end_time"], "%Y-%m-%d %H:%M:%S")
                        start_time = start_dt.strftime("%Y%m%d%H%M%S +0800")
                        stop_time = stop_dt.strftime("%Y%m%d%H%M%S +0800")
                    except ValueError as e:
                        logging.warning(f"⚠️ {channel['name']} 节目时间格式错误：{str(e)}")
                        continue
                    
                    programmes.append({
                        "channel_id": channel["id"],
                        "title": prog["program_name"],
                        "desc": prog.get("program_desc", ""),
                        "start": start_time,
                        "stop": stop_time
                    })
                
                time.sleep(1.5)
                
            except Exception as e:
                logging.error(f"⚠️ 抓取 {channel['name']} {target_date} 失败：{str(e)}")
                continue  # 失败时跳过
    
    return programmes

if __name__ == "__main__":
    logging.info("🚀 开始抓取潍坊本地频道EPG节目单（基于闪电新闻APP）")
    epg_data = crawl_weifang_epg()
    generate_epg_xml(epg_data)
    logging.info("📌 本地EPG抓取流程已完成（无论是否成功，继续后续步骤）")
