import os
import gzip
import re
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 3
TIMEOUT = 30
CORE_RETRY_COUNT = 3

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 全频道手动映射表（兜底）
COOL9_ID_MAPPING = {
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4",
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10", "11": "CCTV11", "12": "CCTV12",
    "13": "CCTV13", "14": "CCTV14", "15": "CCTV15", "16": "CCTV16",
    "21": "北京卫视", "22": "河南卫视", "23": "河北卫视",
    "24": "湖北卫视", "25": "海南卫视", "26": "贵州卫视", "27": "厦门卫视",
    "28": "CCTV5+", "29": "峨眉电影", "30": "峨眉电影4K", "31": "北京IPTV4K超清",
    "32": "淘电影", "33": "淘娱乐", "34": "淘剧场", "35": "淘baby", "36": "淘精彩",
    "37": "萌宠TV", "38": "优漫卡通"
}

# 国外频道过滤
FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国",
    "泰国", "越南", "印尼", "马来西亚", "新加坡", "澳洲",
    "欧洲", "美洲", "非洲", "俄罗斯", "印度", "巴西"
]

DOMESTIC_SPECIAL = ["popc", "淘", "new", "NEW", "POPC", "超级电影", "IPTV", "new系列", "NewTV"]
# ==================================================


class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.name_to_final_id = dict()
        self.program_channel_map = dict()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def read_epg_sources(self) -> List[str]:
        """读取 config.txt"""
        if not os.path.exists(CONFIG_FILE):
            logging.warning("配置文件不存在，将使用空列表")
            return []

        sources = []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
