#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin
from xml.sax.saxutils import escape

# ================= 配置区 =================
# 输出文件路径，必须与 merge.py 中的 LOCAL_WEIFANG_EPG 一致
OUTPUT_FILE = "output/weifang.xml"

# 潍坊电视台频道映射表 (ID -> 中文名)
# 请根据实际频道 ID 修改，这里使用了示例 ID
CHANNEL_MAP: Dict[str, str] = {
    '47a9d24a': '潍坊新闻综合',  # 请确认ID
    '47a9d24b': '潍坊公共',      # 请确认ID
    '47a9d24c': '潍坊科教',      # 请确认ID
    '47a9d24d': '潍坊影视'       # 请确认ID
}

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://epg.51zmt.com/'
}

# ==========================================

def setup_logging():
    """配置日志输出"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def fetch_channel_epg(channel_id: str, days: int = 3) -> Optional[List[Dict]]:
    """
    抓取单个频道未来 N 天的节目数据
    搜视网 API 接口 (示例)
    """
    base_url = "https://epg.51zmt.com/tv/{}.json"
    all_programs = []

    # 遍历未来指定天数
    for i in range(days):
        target_date = (datetime.now() + timedelta(days=i)).strftime('%Y%m%d')
        url = base_url.format(channel_id)
        params = {
            'date': target_date
        }

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if response.status_code != 200:
                logging.warning(f"[{channel_id}] 请求失败，状态码: {response.status_code}")
                continue

            data = response.json()
            # 解析 JSON 数据结构 (根据搜视网实际返回结构调整)
            if data.get('code') == 0 and 'data' in data:
                programs = data['data'].get('program_list', [])
                for prog in programs:
                    try:
                        # 提取关键字段
                        start_time = prog.get('start_time', '')
                        title = prog.get('name', '未知节目')
                        desc = prog.get('desc', '') or '暂无简介'

                        # 格式化时间 (假设返回的是 2024-01-01 12:00:00 格式)
                        dt_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        xmltv_start = dt_obj.strftime('%Y%m%d%H%M%S +0800')

                        all_programs.append({
                            'start': xmltv_start,
                            'title': title,
                            'desc': desc,
                            'channel_id': channel_id
                        })
                    except Exception as e:
                        logging.debug(f"解析单条节目数据出错: {e}")
                        continue
            else:
                logging.warning(f"[{channel_id}] {target_date} 无数据或接口异常")

        except Exception as e:
            logging.error(f"[{channel_id}] 网络请求异常: {e}")
            # 发生网络错误时，直接返回当前已抓取的数据，不阻塞流程
            break

    return all_programs if all_programs else None

def generate_xml(programs: List[Dict]) -> str:
    """生成 XML 字符串"""
    lines = []
    # 1. 生成 Channel 节点 (去重)
    channel_ids = set()
    for prog in programs:
        chan_id = prog['channel_id']
        if chan_id not in channel_ids:
            channel_name = CHANNEL_MAP.get(chan_id, '未知频道')
            lines.append(f'  <channel id="{chan_id}"><display-name>{channel_name}</display-name></channel>')
            channel_ids.add(chan_id)

    # 2. 生成 Programme 节点
    for prog in programs:
        try:
            # --- 安全处理 XML 特殊字符 ---
            # 使用 saxutils.escape 处理 < > & ，手动处理换行和引号
            safe_title = escape(prog['title'].strip())
            safe_desc = escape(prog['desc'].strip())
            # 替换多余的空白符
            safe_title = " ".join(safe_title.split())
            safe_desc = " ".join(safe_desc.split())

            lines.append(f'''  <programme start="{prog['start']}" stop="" channel="{prog['channel_id']}">
    <title lang="zh">{safe_title}</title>
    <desc lang="zh">{safe_desc}</desc>
  </programme>''')
        except Exception:
            continue

    return "\n".join(lines)

def main():
    setup_logging()
    logging.info("🚀 开始抓取潍坊 EPG 数据")

    all_data = []
    success_channels = 0

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # 遍历所有配置的频道
    for chan_id, chan_name in CHANNEL_MAP.items():
        logging.info(f"  ➡️ 正在抓取: {chan_name} ({chan_id})")
        
        try:
            channel_data = fetch_channel_epg(chan_id)
            if channel_data:
                all_data.extend(channel_data)
                success_channels += 1
                logging.info(f"  ✅ 抓取成功: {chan_name} 共 {len(channel_data)} 条")
            else:
                logging.warning(f"  ❌ 抓取失败或无数据: {chan_name}")
                
        except Exception as e:
            # 捕获所有脚本内部错误，确保一个频道挂了不影响其他频道
            logging.error(f"  💥 处理频道 {chan_name} 时发生致命错误: {e}")

    # 生成并写入文件
    if all_data:
        try:
            xml_content = generate_xml(all_data)
            # 写入临时文件防止覆盖
            temp_file = OUTPUT_FILE + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(xml_content)
            # 原子性替换
            os.replace(temp_file, OUTPUT_FILE)
            
            logging.info(f"\n✅ 抓取完成！共处理 {success_channels}/{len(CHANNEL_MAP)} 个频道")
            logging.info(f"📄 已生成文件: {os.path.abspath(OUTPUT_FILE)}")
            return 0  # 成功退出
        except Exception as e:
            logging.error(f"❌ 生成 XML 文件失败: {e}")
    else:
        logging.warning("\n⚠️ 警告：未抓取到任何有效节目数据")
        logging.warning(f"ℹ️ 为了保证流程不中断，将生成一个空的占位文件")
        # 即使没有数据，也生成一个空文件，防止 merge.py 报错
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("")

    return 1  # 失败退出 (但在 GitHub Actions 中会忽略此错误码)

if __name__ == "__main__":
    # 尝试运行，即使报错也尽量不抛出 SystemExit 导致 Actions 失败
    try:
        main()
    except Exception as e:
        logging.error(f"脚本执行发生未捕获异常: {e}")
        # 不调用 sys.exit(1)，让 GitHub Actions 认为这一步是成功的 (配合 continue-on-error)
