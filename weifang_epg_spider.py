import os
import time
import logging
from datetime import datetime, timedelta
import requests
from lxml import etree

# 配置区
OUTPUT_DIR = "output"
LOG_FILE = "weifang_epg.log"
# 潍坊本地频道配置
WEIFANG_CHANNELS = [
    {"id": "1001", "name": "潍坊新闻综合频道", "alias": "潍坊新闻"},
    {"id": "1002", "name": "潍坊经济生活频道", "alias": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共频道", "alias": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化频道", "alias": "潍坊科教文化"},
    {"id": "1008", "name": "寿光蔬菜频道", "alias": "寿光蔬菜"},
    {"id": "1009", "name": "昌乐综合频道", "alias": "昌乐综合"},
    {"id": "1011", "name": "奎文娱乐频道", "alias": "奎文娱乐"}
]

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_weifang_epg():
    """抓取潍坊本地EPG并生成XML文件"""
    logging.info("🚀 开始抓取潍坊本地EPG")
    root = etree.Element("tv")

    # 添加频道信息
    for channel in WEIFANG_CHANNELS:
        channel_elem = etree.SubElement(root, "channel")
        channel_elem.set("id", channel["id"])
        etree.SubElement(channel_elem, "display-name", lang="zh-CN").text = channel["name"]
        etree.SubElement(channel_elem, "display-name", lang="zh-CN").text = channel["alias"]

    # 抓取3天节目（示例接口，需替换为实际潍坊EPG接口）
    headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"}
    for day_offset in range(3):
        target_date = (datetime.today() + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        for channel in WEIFANG_CHANNELS:
            try:
                # 替换为实际的潍坊EPG接口（此处为示例）
                url = f"https://sd.iqilu.com/api/tv/program?channel={channel['alias']}&date={target_date}"
                resp = requests.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                for prog in data.get("data", []):
                    # 转换时间格式（EPG标准格式：YYYYMMDDHHMMSS）
                    start = f"{prog['start_time'].replace('-', '').replace(':', '')} +0800"
                    stop = f"{prog['end_time'].replace('-', '').replace(':', '')} +0800"
                    # 创建节目节点
                    prog_elem = etree.SubElement(root, "programme", 
                                                channel=channel["id"], 
                                                start=start, 
                                                stop=stop)
                    etree.SubElement(prog_elem, "title", lang="zh-CN").text = prog["program_name"]
                    if prog.get("program_desc"):
                        etree.SubElement(prog_elem, "desc", lang="zh-CN").text = prog["program_desc"]

                logging.info(f"✅ 抓取{channel['name']} {target_date}节目成功")
            except Exception as e:
                logging.error(f"❌ 抓取{channel['name']}节目失败: {str(e)}")

    # 保存为XML文件
    output_path = os.path.join(OUTPUT_DIR, "weifang.xml")
    with open(output_path, "wb") as f:
        f.write(etree.tostring(root, encoding="utf-8", pretty_print=True))
    logging.info(f"💾 潍坊本地EPG已保存到: {output_path}")


if __name__ == "__main__":
    fetch_weifang_epg()
