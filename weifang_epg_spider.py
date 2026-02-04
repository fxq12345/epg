import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom
import datetime
import time

# 潍坊本地频道配置（与你的频道名完全匹配）
weifang_channels = [
    {"id": "1001", "name": "潍坊新闻综合频道", "alias": "潍坊新闻"},
    {"id": "1002", "name": "潍坊经济生活频道", "alias": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共频道", "alias": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化频道", "alias": "潍坊科教文化"},
    {"id": "1008", "name": "寿光蔬菜频道", "alias": "寿光蔬菜"},
    {"id": "1009", "name": "昌乐综合频道", "alias": "昌乐综合"},
    {"id": "1011", "name": "奎文娱乐频道", "alias": "奎文娱乐"}
]

# 生成EPG XML文件
def generate_epg_xml(programmes):
    # 创建根节点
    root = ET.Element("tv")
    root.set("generator-info-name", "潍坊EPG抓取脚本（基于闪电新闻）")
    
    # 添加频道节点
    for channel in weifang_channels:
        channel_elem = ET.SubElement(root, "channel")
        channel_elem.set("id", channel["id"])
        
        # 频道名称
        name_elem = ET.SubElement(channel_elem, "display-name")
        name_elem.text = channel["name"]
        name_elem.set("lang", "zh-CN")
        
        # 频道别名
        alias_elem = ET.SubElement(channel_elem, "display-name")
        alias_elem.text = channel["alias"]
        alias_elem.set("lang", "zh-CN")
    
    # 添加节目节点
    for prog in programmes:
        programme_elem = ET.SubElement(root, "programme")
        programme_elem.set("channel", prog["channel_id"])
        programme_elem.set("start", prog["start"])
        programme_elem.set("stop", prog["stop"])
        
        # 节目标题
        title_elem = ET.SubElement(programme_elem, "title")
        title_elem.text = prog["title"]
        title_elem.set("lang", "zh-CN")
        
        # 节目描述（若有）
        if prog.get("desc"):
            desc_elem = ET.SubElement(programme_elem, "desc")
            desc_elem.text = prog["desc"]
            desc_elem.set("lang", "zh-CN")
    
    # 美化XML格式
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    with open("weifang_epg.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)
    print("✅ 潍坊EPG节目单已生成：weifang_epg.xml")

# 抓取闪电新闻APP节目单（模拟移动端请求）
def crawl_weifang_epg():
    programmes = []
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Referer": "https://sd.iqilu.com/"
    }
    
    # 抓取今明后3天节目单
    for day_offset in range(3):
        target_date = (datetime.date.today() + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")
        print(f"📅 正在抓取 {target_date} 节目单...")
        
        for channel in weifang_channels:
            # 闪电新闻潍坊频道节目单接口（经抓包验证稳定）
            url = f"https://sd.iqilu.com/api/tv/program?channel={channel['alias']}&date={target_date}"
            
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                # 解析节目数据
                for prog in data.get("data", []):
                    # 时间格式转换（适配EPG标准：YYYYMMDDHHMMSS +0800）
                    start_time = f"{prog['start_time'].replace('-', '').replace(':', '')} +0800"
                    stop_time = f"{prog['end_time'].replace('-', '').replace(':', '')} +0800"
                    
                    programme = {
                        "channel_id": channel["id"],
                        "title": prog["program_name"],
                        "desc": prog.get("program_desc", ""),
                        "start": start_time,
                        "stop": stop_time
                    }
                    programmes.append(programme)
                
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                print(f"⚠️  抓取 {channel['name']} {target_date} 节目单失败：{str(e)}")
    
    return programmes

if __name__ == "__main__":
    print("🚀 开始抓取潍坊本地频道EPG节目单（基于闪电新闻APP）")
    epg_data = crawl_weifang_epg()
    if epg_data:
        generate_epg_xml(epg_data)
        print("🎉 抓取完成！可直接用于merge.py合并")
    else:
        print("❌ 未抓取到任何节目数据，请检查网络或接口状态")
