import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
LOG_FILE = "epg_log.txt"

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items(): text=text.replace(a,b)
    return text

# 精准正则：只匹配纯CCTV4，不匹配CCTV4K/4+/4超
REGEX_CCTV4 = re.compile(r'^[\s]*(CCTV|央视)['r'-_]*4(?!\s*[Kk+])[\s]*$', re.IGNORECASE)
REGEX_CCTV4K = re.compile(r'^[\s]*(CCTV|央视)['r'-_]*4K[\s]*$', re.IGNORECASE)

def unified_name(raw_name):
    n = f2s(raw_name).strip()
    lower_n = n.lower()

    # 精准匹配CCTV4（排除4K）
    if REGEX_CCTV4.match(n):
        return "CCTV4"
    # 精准匹配CCTV4K
    if REGEX_CCTV4K.match(n):
        return "CCTV4K"

    if re.match(r'^[\s]*(CCTV|央视)['r'-_]*5(?!\s*\+|Kk)[\s]*$', re.IGNORECASE, n):
        return "CCTV5"
    if "cctv5+" in lower_n or "5+体育" in lower_n:
        return "CCTV5+"
    if "浙江卫视" in n:
        return "浙江卫视"
    # 山东体育严格匹配：必须同时有“山东”和“体育”
    if "山东" in n and "体育" in n and "休闲" not in n:
        return "山东体育"
    if "山东卫视" in n:
        return "山东卫视"
    if "齐鲁" in n:
        return "山东齐鲁"
    return n

# 时间区间
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start = today - timedelta(days=DAYS_BEFORE)
end = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent":"Mozilla/5.0"}

def fetch(url,i):
    logging.info(f"【第{i}条】抓取: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logging.error(f"【第{i}条】失败 码:{r.status_code}")
            return None, None,
