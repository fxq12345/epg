import requests
import gzip
import io
import xml.etree.ElementTree as ET
import os
import time
from datetime import datetime, timedelta

# 从config.txt加载EPG源（潍坊源优先）
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
        print(f"⚠️  配置文件不存在，潍坊源优先+默认5个网络源")
        return [local_weifang_source] + DEFAULT_SOURCES
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        network_sources = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        if not network_sources:
            network_sources = DEFAULT_SOURCES
        return [local_weifang_source] + network_sources
    except Exception as e:
        print(f"⚠️  配置文件读取失败：{e}，潍坊源优先+默认5个网络源")
        return [local_weifang_source] + DEFAULT_SOURCES

EPG_SOURCES = load_epg_sources()
channels = {}
programmes = []

# 🔥 关键：酷9内置ID映射（覆盖潍坊+央视+卫视，纯数字ID）
COOL9_ID_MAP = {
    # 潍坊本地频道（1-4，已验证1001可显示，改为1更贴合默认规则）
    "潍坊新闻综合频道": "1",
    "潍坊经济生活": "2",
    "潍坊公共": "3",
    "潍坊科教文化": "4",
    # 央视频道（酷9默认ID段：10-29）
    "CCTV-1": "10",
    "CCTV-2": "11",
    "CCTV-3": "12",
    "CCTV-4": "13",
    "CCTV-5": "14",
    "CCTV-6": "15",
    "CCTV-7": "16",
    "CCTV-8": "17",
    "CCTV-9": "18",
    "CCTV-10": "19",
    "CCTV-11": "20",
    "CCTV-12": "21",
    "CCTV-13": "22",
    "CCTV-14": "23",
    "CCTV-15": "24",
    # 山东频道（酷9默认ID段：30-39）
    "山东卫视": "30",
    "山东综艺": "31",
    "山东影视": "32",
    "山东体育": "33",
    # 热门卫视频道（酷9默认ID段：50-79）
    "湖南卫视": "50",
    "浙江卫视": "51",
    "江苏卫视": "52",
    "东方卫视": "53",
    "北京卫视": "54",
    "安徽卫视": "55",
    "广东卫视": "56",
    "深圳卫视": "57",
    "四川卫视": "58",
    "河南卫视": "59"
}

# 通用节目补全数据
GENERAL_PROG_DATA = [
    # 潍坊本地频道
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
    # 央视+卫视
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
        # 潍坊源特殊保护
        if source_path == "output/weifang.xml":
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    xml_content = f.read()
                if not xml_content.strip() or xml_content.strip() == "<tv></tv>":
                    print(f"⚠️  潍坊源为空，生成默认数据")
                    tv = ET.Element("tv")
                    for chan_name, chan_id in COOL9_ID_MAP.items():
                        if "潍坊" in chan_name:
                            chan_elem = ET.SubElement(tv, "channel", {"id": chan_id})
                            ET.SubElement(chan_elem, "display-name").text = chan_name
                    xml_content = ET.tostring(tv, encoding="utf-8").decode("utf-8")
                root = ET.fromstring(xml_content)
                parse_time = (datetime.now() - start_time).total_seconds()
                print(f"✅ 读取潍坊源：{source_path} | 耗时: {parse_time:.2f}s")
                return root
            except Exception as e:
                print(f"⚠️  潍坊源处理失败：{e}，生成默认数据")
                tv = ET.Element("tv")
                for chan_name, chan_id in COOL9_ID_MAP.items():
                    if "潍坊" in chan_name:
                        chan_elem = ET.SubElement(tv, "channel", {"id": chan_id})
                        ET.SubElement(chan_elem, "display-name").text = chan_name
                return tv
        # 网络源处理
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
                    print(f"⚠️  网络源重试{retry+1}/{max_retries}：{e}")
                    time.sleep(5)
                else:
                    print(f"❌ 网络源失败，跳过：{source_path}")
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
            print(f"⚠️  网络源当天节目过少（{today_prog_count}条），后续补全")
        parse_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ 读取网络源：{source_path} | 耗时: {parse_time:.2f}s")
        return root
    except Exception as e:
        print(f"❌ 源处理失败，跳过：{source_path} | 错误: {e}")
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
        
        # 分配酷9兼容纯数字ID
        if channel_name in COOL9_ID_MAP:
            channel_id = COOL9_ID_MAP[channel_name]
        else:
            # 未知频道：生成100+纯数字ID（避开已知ID段）
            channel_id = str(100 + len(channels) + 1)
        
        # 按ID去重（避免冲突）
        existing_chan = next((name for name, info in channels.items() if info["id"] == channel_id), None)
        if existing_chan:
            print(f"🔄 ID冲突，跳过重复：{channel_name} → {existing_chan}（ID：{channel_id}）")
            continue
        
        channels[channel_name] = {"name": channel_name, "id": channel_id}
        if "潍坊" in channel_name:
            print(f"🔒 潍坊频道：{channel_name}（ID：{channel_id}）")
        elif "CCTV" in channel_name or "央视" in channel_name:
            print(f"➕ 央视频道：{channel_name}（ID：{channel_id}）")
        elif "山东" in channel_name:
            print(f"➕ 山东频道：{channel_name}（ID：{channel_id}）")
        elif "卫视" in channel_name:
            print(f"➕ 卫视频道：{channel_name}（ID：{channel_id}）")
        else:
            print(f"➕ 普通频道：{channel_name}（ID：{channel_id}）")
    
    # 处理节目
    for programme in root.findall(".//programme"):
        prog_channel_id = programme.get("channel")
        if not prog_channel_id:
            continue
        # 匹配频道（按ID）
        prog_channel_name = None
        for name, info in channels.items():
            if info["id"] == prog_channel_id:
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
                print(f"🔧 补全节目：{channel_name}（{len(matched_progs)}个）")
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
                print(f"🔧 补全节目（通用模板）：{channel_name}")
    today_prog_count_after = len([p for p in programmes if p["start"].startswith(today)])
    print(f"📈 节目补全：{today
