import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
import time
from datetime import datetime, timedelta

# 从config.txt加载EPG源（潍坊源强制优先加载）
EPG_SOURCES = []
DEFAULT_SOURCES = [
    "https://epg.27481716.xyz/epg.xml",
    "https://e.erw.cc/all.xml",
    "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
    "http://epg.51zmt.top:8000/e.xml",
    "https://raw.githubusercontent.com/fanmingming/live/main/e.xml"
]

def load_epg_sources(config_path="config.txt"):
    local_weifang_source = "output/weifang.xml"
    if not os.path.exists(config_path):
        print(f"⚠️  配置文件{config_path}不存在，潍坊源优先+默认5个网络源")
        return [local_weifang_source] + DEFAULT_SOURCES  # 潍坊源放最前面
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        network_sources = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        if not network_sources:
            network_sources = DEFAULT_SOURCES
        # 潍坊源强制置顶，确保优先加载
        return [local_weifang_source] + network_sources
    except Exception as e:
        print(f"⚠️  读取配置文件失败：{str(e)}，潍坊源优先+默认5个网络源")
        return [local_weifang_source] + DEFAULT_SOURCES

EPG_SOURCES = load_epg_sources()
channels = {}
programmes = []

# 潍坊本地频道（固定ID，永不覆盖）
WEIFANG_CHANNELS = {
    "潍坊新闻综合频道": {"id": "wf_0001"},
    "潍坊经济生活": {"id": "wf_0002"},
    "潍坊公共": {"id": "wf_0003"},
    "潍坊科教文化": {"id": "wf_0004"}
}

# 通用节目补全数据
GENERAL_PROG_DATA = [
    # 潍坊本地频道（固定节目）
    {"channel_name": "潍坊新闻综合频道", "time": "07:00", "title": "潍坊新闻早班车", "duration": 60},
    {"channel_name": "潍坊新闻综合频道", "time": "08:00", "title": "生活帮", "duration": 60},
    {"channel_name": "潍坊新闻综合频道", "time": "12:00", "title": "正午新闻", "duration": 30},
    {"channel_name": "潍坊新闻综合频道", "time": "18:30", "title": "潍坊新闻联播", "duration": 30},
    {"channel_name": "潍坊新闻综合频道", "time": "20:00", "title": "黄金剧场", "duration": 120},
    {"channel_name": "潍坊经济生活", "time": "09:00", "title": "生活百科", "duration": 60},
    {"channel_name": "潍坊经济生活", "time": "12:30", "title": "美食潍坊", "duration": 30},
    {"channel_name": "潍坊经济生活", "time": "19:00", "title": "家居风尚", "duration": 60},
    {"channel_name": "潍坊公共", "time": "10:00", "title": "健康大讲堂", "duration": 60},
    {"channel_name": "潍坊公共", "time": "15:00", "title": "公共剧场", "duration": 120},
    {"channel_name": "潍坊科教文化", "time": "08:30", "title": "科普天地", "duration": 60},
    {"channel_name": "潍坊科教文化", "time": "16:00", "title": "教育在线", "duration": 60},
    # 通用频道
    {"channel_name": "CCTV-1", "time": "07:00", "title": "朝闻天下", "duration": 120},
    {"channel_name": "CCTV-1", "time": "12:00", "title": "新闻30分", "duration": 30},
    {"channel_name": "CCTV-1", "time": "19:00", "title": "新闻联播", "duration": 30},
    {"channel_name": "山东卫视", "time": "08:00", "title": "早间新闻", "duration": 60},
    {"channel_name": "山东卫视", "time": "12:30", "title": "正午新闻圈", "duration": 30},
    {"channel_name": "山东卫视", "time": "19:30", "title": "黄金剧场", "duration": 120},
    {"channel_name": "湖南卫视", "time": "07:30", "title": "早安湖南", "duration": 30},
    {"channel_name": "浙江卫视", "time": "19:30", "title": "中国蓝剧场", "duration": 120},
    {"channel_name": "江苏卫视", "time": "20:20", "title": "非诚勿扰", "duration": 90}
]

def fetch_epg_source(source_path):
    try:
        print(f"📥 处理: {source_path}")
        start_time = datetime.now()
        # 处理本地潍坊源（特殊保护）
        if source_path == "output/weifang.xml":
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    xml_content = f.read()
                if not xml_content.strip() or xml_content.strip() == "<tv></tv>":
                    print(f"⚠️  潍坊源文件为空，使用内置默认潍坊节目")
                    # 生成默认潍坊XML数据
                    tv = ET.Element("tv")
                    for chan_name, chan_info in WEIFANG_CHANNELS.items():
                        chan_elem = ET.SubElement(tv, "channel", {"id": chan_info["id"]})
                        ET.SubElement(chan_elem, "display-name").text = chan_name
                    xml_content = ET.tostring(tv, encoding="utf-8").decode("utf-8")
                root = ET.fromstring(xml_content)
                parse_time = (datetime.now() - start_time).total_seconds()
                print(f"✅ 读取潍坊源（优先保护）：{source_path} | 耗时: {parse_time:.2f}s")
                return root
            except Exception as e:
                print(f"⚠️  潍坊源处理失败，使用内置默认数据：{e}")
                # 直接返回内置潍坊频道数据
                tv = ET.Element("tv")
                for chan_name, chan_info in WEIFANG_CHANNELS.items():
                    chan_elem = ET.SubElement(tv, "channel", {"id": chan_info["id"]})
                    ET.SubElement(chan_elem, "display-name").text = chan_name
                return tv
        # 处理网络源
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        max_retries = 3
        response = None
        for retry in range(max_retries):
            try:
                response = requests.get(source_path, headers=headers, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    print(f"⚠️  网络源重试{retry+1}/{max_retries}：{str(e)}")
                    time.sleep(5)
                else:
                    print(f"❌ 网络源最终失败，跳过：{source_path}")
                    return None
        # 编码适配
        try:
            if source_path.endswith(".gz"):
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                    xml_content = f.read().decode("utf-8")
            else:
                xml_content = response.content.decode(response.apparent_encoding or "utf-8")
        except UnicodeDecodeError:
            xml_content = response.content.decode("gbk", errors="ignore")
        if not xml_content.strip() or not xml_content.startswith("<?xml"):
            print(f"⚠️  网络源数据无效，跳过：{source_path}")
            return None
        xml_content = xml_content.replace("\x00", "").strip()
        root = ET.fromstring(xml_content)
        # 统计当天节目
        today = datetime.now().date().strftime("%Y%m%d")
        today_prog_count = 0
        for prog in root.findall(".//programme"):
            start = prog.get("start", "")
            if start.startswith(today):
                today_prog_count += 1
        if today_prog_count < 3:
            print(f"⚠️  网络源当天节目过少（{today_prog_count}条），后续补全：{source_path}")
        parse_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ 读取网络源：{source_path} | 耗时: {parse_time:.2f}s")
        return root
    except Exception as e:
        print(f"❌ 源处理失败，跳过：{source_path} | 错误: {str(e)}")
        return None

def parse_epg(root, source_path):
    for channel in root.findall(".//channel"):
        # 提取频道名称
        display_names = channel.findall(".//display-name")
        channel_name = ""
        for dn in display_names:
            if dn.text and dn.text.strip():
                channel_name = dn.text.strip()
                break
        if not channel_name:
            channel_name = f"未知频道_{len(channels)+1}"
        
        # 潍坊频道特殊处理：固定ID，强制保留
        if channel_name in WEIFANG_CHANNELS:
            chan_info = WEIFANG_CHANNELS[channel_name]
            # 强制写入潍坊频道，覆盖任何冲突
            channels[channel_name] = {"name": channel_name, "id": chan_info["id"]}
            print(f"🔒 锁定潍坊频道：{channel_name}（固定ID：{chan_info['id']}）")
            continue
        
        # 其他频道去重处理
        channel_name_normalized = channel_name.strip().lower()
        existing_name = next((name for name in channels.keys() if name.strip().lower() == channel_name_normalized), None)
        if existing_name:
            print(f"🔄 频道已存在，跳过重复：{channel_name} → {existing_name}")
            continue
        
        # 网络源频道ID
        channel_id = channel.get("id")
        if not channel_id or not channel_id.isdigit():
            import random
            channel_id = f"net_{random.randint(10000, 99999)}"
        else:
            channel_id = f"net_{channel_id}"
        
        channels[channel_name] = {"name": channel_name, "id": channel_id}
        if "山东" in channel_name or "央视" in channel_name or "卫视" in channel_name:
            print(f"➕ 新增优先频道：{channel_name}（ID：{channel_id}）")
        else:
            print(f"➕ 新增普通频道：{channel_name}（ID：{channel_id}）")
    
    # 处理节目
    for programme in root.findall(".//programme"):
        prog_channel_id = programme.get("channel")
        if not prog_channel_id:
            continue
        # 匹配频道（优先潍坊固定ID）
        prog_channel_name = None
        # 先匹配潍坊频道
        for chan_name, chan_info in WEIFANG_CHANNELS.items():
            if chan_info["id"] == prog_channel_id:
                prog_channel_name = chan_name
                break
        # 再匹配其他频道
        if not prog_channel_name:
            for name, info in channels.items():
                if info["id"] == prog_channel_id or info["id"] == f"net_{prog_channel_id}":
                    prog_channel_name = name
                    break
        if not prog_channel_name:
            continue
        # 处理节目时间
        start_str = programme.get("start", "")
        stop_str = programme.get("stop", "")
        if start_str and not stop_str:
            try:
                start_time = datetime.strptime(start_str.split("+")[0], "%Y%m%d%H%M%S")
                stop_time = start_time + timedelta(minutes=60)
                stop_str = stop_time.strftime("%Y%m%d%H%M%S +0800")
            except:
                stop_str = start_str
        # 提取标题
        title_elem = programme.find(".//title[@lang='zh']") or programme.find(".//title")
        title = title_elem.text.strip() if title_elem and title_elem.text else "未知节目"
        programmes.append({
            "channel_name": prog_channel_name,
            "start": start_str,
            "stop": stop_str,
            "title": title
        })

def fill_missing_today_programs():
    today = datetime.now().date().strftime("%Y%m%d")
    today_prog_count_before = len([p for p in programmes if p["start"].startswith(today)])
    for channel_name in channels.keys():
        has_valid_today = any(
            p["start"].startswith(today) and p["title"] != "未知节目"
            for p in programmes if p["channel_name"] == channel_name
        )
        if not has_valid_today:
            matched_progs = [p for p in GENERAL_PROG_DATA if p["channel_name"] == channel_name]
            if not matched_progs:
                for prog in GENERAL_PROG_DATA:
                    if prog["channel_name"] in channel_name or channel_name in prog["channel_name"]:
                        matched_progs.append(prog)
                        break
            for prog in matched_progs:
                start = datetime.strptime(f"{today} {prog['time']}", "%Y%m%d %H:%M")
                programmes.append({
                    "channel_name": channel_name,
                    "start": start.strftime("%Y%m%d%H%M%S +0800"),
                    "stop": (start + timedelta(minutes=prog["duration"])).strftime("%Y%m%d%H%M%S +0800"),
                    "title": prog["title"]
                })
            if matched_progs:
                print(f"🔧 补全当天节目：{channel_name}（{len(matched_progs)}个）")
            else:
                default_progs = [
                    {"time": "08:00", "title": "早间节目", "duration": 60},
                    {"time": "12:00", "title": "午间节目", "duration": 30},
                    {"time": "19:00", "title": "晚间节目", "duration": 90}
                ]
                for prog in default_progs:
                    start = datetime.strptime(f"{today} {prog['time']}", "%Y%m%d %H:%M")
                    programmes.append({
                        "channel_name": channel_name,
                        "start": start.strftime("%Y%m%d%H%M%S +0800"),
                        "stop": (start + timedelta(minutes=prog["duration"])).strftime("%Y%m%d%H%M%S +0800"),
                        "title": prog["title"]
                    })
                print(f"🔧 补全当天节目（通用模板）：{channel_name}")
    today_prog_count_after = len([p for p in programmes if p["start"].startswith(today)])
    print(f"📈 当天节目补全完成：{today_prog_count_before}条 → {today_prog_count_after}条")

def generate_final_epg():
    # 频道排序：潍坊频道强制置顶
    sorted_channel_names = list(WEIFANG_CHANNELS.keys()) + [
        name for name in channels.keys() if name not in WEIFANG_CHANNELS
    ]
    
    # 生成XML
    tv = ET.Element("tv", {
        "source-info-name": "综合EPG源（酷9适配+潍坊锁定）",
        "generated-date": datetime.now().strftime("%Y%m%d%H%M%S +0800"),
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"
    })
    xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
    
    # 添加频道（潍坊频道增加多备用名）
    for channel_name in sorted_channel_names:
        channel_info = channels[channel
