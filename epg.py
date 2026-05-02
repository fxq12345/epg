import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io
import time

# ========== 配置区域 ==========
# 模拟当前时间，用于调试，实际运行可注释掉或保留
# TARGET_DATE = datetime(2026, 5, 2)

# 频道白名单（核心500频道），用于过滤掉那1000多个杂乱频道
# 这里列出核心前缀，脚本会自动匹配
ALLOWED_PREFIXES = [
    "CCTV1", "CCTV2", "CCTV3", "CCTV4", "CCTV5", "CCTV6", "CCTV7", "CCTV8", "CCTV9",
    "CCTV10", "CCTV11", "CCTV12", "CCTV13", "CCTV14", "CCTV15", "CCTV17", "CCTV5+",
    "CGTN", "中国教育", "北京卫视", "上海东方", "天津卫视", "重庆卫视", "河北卫视",
    "山西卫视", "内蒙古卫视", "辽宁卫视", "吉林卫视", "黑龙江卫视", "江苏卫视",
    "浙江卫视", "安徽卫视", "福建东南", "江西卫视", "山东卫视", "河南卫视",
    "湖北卫视", "湖南卫视", "广东卫视", "广西卫视", "海南卫视", "深圳卫视",
    "四川卫视", "贵州卫视", "云南卫视", "西藏卫视", "陕西卫视", "甘肃卫视",
    "青海卫视", "宁夏卫视", "新疆卫视", "兵团卫视", "三沙卫视", "厦门卫视",
    "金鹰卡通", "卡酷动画", "嘉佳卡通", "优漫卡通", "哈哈炫动", "新动漫",
    "风云音乐", "风云剧场", "第一剧场", "世界地理", "高尔夫网球", "兵器科技",
    "央视台球", "央视文化精品", "女性时尚", "中视购物", "发现之旅", "老故事",
    "新科动漫", "证券资讯", "中学生", "快乐垂钓", "茶频道", "环球奇观",
    "卫生健康", "书画频道", "留学世界", "青年学苑", "摄影频道", "天元围棋",
    "现代女性", "早期教育", "彩民在线", "老年福", "快乐购", "先锋记录",
    "游戏竞技", "靓妆频道", "数字电视", "欧洲足球", "央视精品", "国防军事",
    "中央广播电视总台"
]

LOG_FILE = "epg_update.log"
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def unified_name(name):
    """
    核心频道名标准化函数
    将各种别名强制映射为标准名称，确保播放器能识别
    """
    if not name:
        return None

    original_name = name
    name = name.replace(" ", "").replace("高清", "").replace("HD", "").replace("标清", "").replace("-", "").replace("_", "")

    # --- CCTV 系列强制映射 ---
    if "CCTV" in name:
        # 处理 CCTV1, CCTV01, CCTV-1
        if "CCTV1" in name: return "CCTV1"
        if "CCTV2" in name: return "CCTV2"
        if "CCTV3" in name: return "CCTV3"
        if "CCTV4" in name: return "CCTV4"
        if "CCTV5" in name and "PLUS" in name: return "CCTV5+" # 特殊处理5+
        if "CCTV5" in name: return "CCTV5"
        if "CCTV6" in name: return "CCTV6"
        if "CCTV7" in name: return "CCTV7"
        if "CCTV8" in name: return "CCTV8"
        if "CCTV9" in name: return "CCTV9"
        if "CCTV10" in name: return "CCTV10"
        if "CCTV11" in name: return "CCTV11"
        if "CCTV12" in name: return "CCTV12"
        if "CCTV13" in name: return "CCTV13"
        if "CCTV14" in name: return "CCTV14"
        if "CCTV15" in name: return "CCTV15"
        if "CCTV16" in name: return "CCTV16"
        if "CCTV17" in name: return "CCTV17"
        if "CCTV4K" in name: return "CCTV4K"
        if "CCTV8K" in name: return "CCTV8K"

    # --- 卫视及地方台强制映射 ---
    # 格式：if "关键词" in name: return "标准名"
    if "北京卫视" in name: return "北京卫视"
    if "东方卫视" in name or "上海卫视" in name: return "上海东方"
    if "天津卫视" in name: return "天津卫视"
    if "重庆卫视" in name: return "重庆卫视"
    if "河北卫视" in name: return "河北卫视"
    if "山西卫视" in name: return "山西卫视"
    if "内蒙古" in name: return "内蒙古卫视"
    if "辽宁卫视" in name: return "辽宁卫视"
    if "吉林卫视" in name: return "吉林卫视"
    if "黑龙江" in name: return "黑龙江卫视"
    if "江苏卫视" in name: return "江苏卫视"
    if "浙江卫视" in name: return "浙江卫视"
    if "安徽卫视" in name: return "安徽卫视"
    if "福建" in name and "东南" in name: return "福建东南"
    if "江西卫视" in name: return "江西卫视"
    if "山东卫视" in name: return "山东卫视"
    if "河南卫视" in name: return "河南卫视"
    if "湖北卫视" in name: return "湖北卫视"
    if "湖南卫视" in name: return "湖南卫视"
    if "广东卫视" in name: return "广东卫视"
    if "广西卫视" in name: return "广西卫视"
    if "海南卫视" in name: return "海南卫视"
    if "深圳卫视" in name: return "深圳卫视"
    if "四川卫视" in name: return "四川卫视"
    if "贵州卫视" in name: return "贵州卫视"
    if "云南卫视" in name: return "云南卫视"
    if "西藏卫视" in name: return "西藏卫视"
    if "陕西卫视" in name: return "陕西卫视"
    if "甘肃卫视" in name: return "甘肃卫视"
    if "青海卫视" in name: return "青海卫视"
    if "宁夏卫视" in name: return "宁夏卫视"
    if "新疆卫视" in name: return "新疆卫视"
    if "兵团卫视" in name: return "兵团卫视"
    if "三沙卫视" in name: return "三沙卫视"
    if "厦门卫视" in name: return "厦门卫视"

    # --- 数字/付费频道 ---
    if "中国教育" in name: return "中国教育1"
    if "金鹰卡通" in name: return "金鹰卡通"
    if "卡酷动画" in name: return "卡酷动画"
    if "嘉佳卡通" in name: return "嘉佳卡通"
    if "优漫卡通" in name: return "优漫卡通"
    if "哈哈炫动" in name: return "哈哈炫动"

    # 如果不在白名单前缀里，直接返回None进行过滤
    for prefix in ALLOWED_PREFIXES:
        if prefix in original_name:
            # 如果匹配到了前缀但没有上面的具体规则，返回原名或做进一步处理
            return original_name.replace("高清", "").replace("HD", "").strip()

    return None

def fetch_url(url, timeout=10):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.error(f"请求失败: {url}, 错误: {e}")
        return None

def parse_xml_source(url):
    logging.info(f"开始抓取源: {url}")
    response = fetch_url(url)
    if not response:
        return {}

    try:
        # 尝试解压Gzip（如果是gz文件）
        if url.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read()
        else:
            content = response.content

        # 解析XML
        # 注意：百川格式通常是标准的XMLTV格式
        root = etree.fromstring(content)

        channels = {}
        programmes = []

        # 1. 解析频道信息
        channel_elements = root.findall("channel")
        logging.info(f"XML原始频道数量: {len(channel_elements)}")

        valid_channel_ids = {}

        for channel in channel_elements:
            channel_id = channel.get("id")
            display_name_elem = channel.find("display-name")
            if display_name_elem is not None and display_name_elem.text:
                raw_name = display_name_elem.text
                std_name = unified_name(raw_name)

                if std_name:
                    # 只有标准化后的名字才保留
                    valid_channel_ids[channel_id] = std_name
                    if std_name not in channels:
                        channels[std_name] = []
                # else:
                # logging.debug(f"过滤掉非白名单频道: {raw_name}")

        logging.info(f"清洗后有效频道数量: {len(valid_channel_ids)}")

        # 2. 解析节目信息
        programme_elements = root.findall("programme")
        logging.info(f"开始解析节目条目... (总数: {len(programme_elements)})")

        for prog in programme_elements:
            channel_id = prog.get("channel")
            if channel_id in valid_channel_ids:
                title_elem = prog.find("title")
                start_time = prog.get("start")
                end_time = prog.get("stop")

                if title_elem is not None and start_time and end_time:
                    # 格式化时间: 20260502120000 +0800 -> 2026-05-02 12:00
                    try:
                        # 简单截取前14位 YYYYMMDDHHMMSS
                        start_dt = datetime.strptime(start_time[:14], "%Y%m%d%H%M%S")
                        end_dt = datetime.strptime(end_time[:14], "%Y%m%d%H%M%S")

                        # 这里不再严格过滤时间，保留源数据提供的所有时间段
                        # 如果需要强制过滤，可以取消下面的注释
                        # if start_dt < TARGET_DATE - timedelta(days=7): continue

                        programme_data = {
                            "start": start_dt.strftime("%H:%M"),
                            "end": end_dt.strftime("%H:%M"),
                            "title": title_elem.text
                        }

                        std_name = valid_channel_ids[channel_id]
                        channels[std_name].append(programme_data)

                    except ValueError:
                        continue

        logging.info(f"解析完成，包含节目的频道数: {len(channels)}")
        return channels

    except Exception as e:
        logging.error(f"解析XML失败: {e}")
        return {}

def main():
    logging.info("========== EPG 更新任务开始 ==========")

    # 读取配置
    sources = []
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    sources.append(line)
    else:
        logging.error(f"配置文件 {CONFIG_FILE} 不存在！")
        return

    all_channels_data = {}

    for i, source_url in enumerate(sources):
        logging.info(f"---------- 处理第 {i+1} 个源 ----------")
        data = parse_xml_source(source_url)

        # 合并数据（后面的源覆盖前面的，或者补充）
        for ch_name, programs in data.items():
            if ch_name not in all_channels_data:
                all_channels_data[ch_name] = programs
            else:
                # 如果已有数据，可以选择跳过或合并（这里简单处理：如果已有则不覆盖，保留第一个源的完整性）
                # 也可以做去重合并，但百川格式通常一个源就是完整的
                pass

    # 保存结果
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    try:
        with gzip.open(output_path, "wt", encoding="utf-8") as f:
            json.dump(all_channels_data, f, ensure_ascii=False, indent=None)
        logging.info(f"EPG 数据已保存至: {output_path}")
        logging.info(f"最终频道总数: {len(all_channels_data)}")
    except Exception as e:
        logging.error(f"保存文件失败: {e}")

    logging.info("========== 任务结束 ==========")

if __name__ == "__main__":
    main()
