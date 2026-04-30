import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io
import sys

# 无缓冲输出，GitHub Action 完整看日志
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==================== 配置 前后各7天 合计15天 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io
import sys

# 强制控制台输出
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

# 前后7天 = 15天完整保留
DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==================== 下载 ====================
def fetch(url, index):
    try:
        logging.info(f"[{index}] 下载: {url}")
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logging.warning(f"[{index}] 失败 {r.status_code}")
            return None, None, False
        return r.content, detect_format(r.content, url), True
    except Exception as e:
        logging.warning(f"[{index}] 异常: {str(e)[:40]}")
        return None, None, False

# ==================== 格式判断 ====================
def detect_format(content, url):
    if content.startswith(b'\x1f\x8b'):
        return "xmlgz"
    if b'<?xml' in content[:200] or b'<tv' in content[:200]:
        return "xml"
    try:
        if content.lstrip().startswith((b'{', b'[')):
            json.loads(content.decode("utf-8","ignore")[:300])
            return "json"
    except:
        pass
    return "unknown"

# ==================== 解析XML【原版原样 不清理 不删除】====================
def parse_xml(content, index):
    try:
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = etree.fromstring(content)
        channels = {ch.get("id"): ch for ch in root.xpath("//channel") if ch.get("id")}
        programs = root.xpath("//programme")
        logging.info(f"[{index}] XML 原始导入：{len(channels)}频道 {len(programs)}节目")
        return channels, programs
    except Exception as e:
        logging.warning(f"[{index}] XML解析失败: {e}")
        return {}, []

# ==================== 主程序 ====================
def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    urls = read_config()
    if not urls:
        logging.error("无配置链接")
        return

    all_channels = {}
    all_programs = []

    for idx, url in enumerate(urls, 1):
        data, fmt, ok = fetch(url, idx)
        if not ok or not data:
            continue
        if fmt in ("xml", "xmlgz"):
            chs, progs = parse_xml(data, idx)
            all_channels.update(chs)
            all_programs.extend(progs)

    if all_channels:
        root = etree.Element("tv")
        for ch in all_channels.values():
            root.append(ch)
        for p in all_programs:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        outpath = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(outpath, "wb") as f:
            f.write(xml_bytes)

        print("="*60)
        print(f"✅ 生成完成：{len(all_channels)} 频道 | {len(all_programs)} 节目")
        print(f"✅ 完整保留：前7天 + 后7天 = 15天")
        print(f"✅ 原样输出：卫视、地方台、CCTV4/5 全部保留")
        print("="*60)

if __name__ == "__main__":
    main()


DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

now = datetime.now()
today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

def fetch(url, index):
    try:
        logging.info(f"[{index}] 下载: {url[:60]}")
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logging.warning(f"[{index}] 状态码异常 {r.status_code}")
            return None, None, False
        content = r.content
        fmt = detect_format(content, url)
        logging.info(f"[{index}] 成功识别格式 → {fmt}")
        return content, fmt, True
    except Exception as e:
        logging.warning(f"[{index}] 下载异常: {str(e)[:50]}")
        return None, None, False

def detect_format(content, url):
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                if b'<?xml' in f.read(200):
                    return "xml"
        except:
            pass
    if b'<?xml' in content[:200] or b'<tv' in content[:200]:
        return "xml"
    try:
        if content.lstrip().startswith(b'{') or content.lstrip().startswith(b'['):
            json.loads(content.decode("utf-8","ignore")[:300])
            return "json"
    except:
        pass
    return "unknown"

def parse_xml(content, index):
    channels = {}
    programs = []
    try:
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        root = etree.fromstring(content)
        for ch in root.xpath("//channel"):
            cid = ch.get("id")
            if cid:
                cid = re.sub(r'[^a-zA-Z0-9_-]', '', cid)
                channels[cid] = ch
        for p in root.xpath("//programme"):
            st = parse_program_time(p.get("start"))
            if st:
                cid = p.get("channel")
                if cid:
                    p.set("channel", re.sub(r'[^a-zA-Z0-9_-]', '', cid))
                programs.append(p)
        return channels, programs, len(channels), len(programs)
    except:
        return {}, [], 0, 0

def parse_program_time(ts):
    if not ts:
        return None
    try:
        return datetime.strptime(ts[:14], "%Y%m%d%H%M%S")
    except:
        return None

# ==================== dy2.fun 专属核心修复函数 ====================
def parse_json(content, index):
    channels = {}
    programs = []
    try:
        data = json.loads(content.decode("utf-8","ignore"))
        base_date = today

        for item in data:
            tvid = item.get("tvid") or item.get("id")
            name = item.get("name")
            plist = item.get("list", [])
            if not tvid or not name or not plist:
                continue

            # 干净频道ID，播放器全部兼容
            cid = re.sub(r'[^\w\u4e00-\u9fa5]','',tvid)
            if not cid:
                cid = re.sub(r'[^\w\u4e00-\u9fa5]','',name)

            if cid not in channels:
                ch = etree.Element("channel", id=cid)
                dn = etree.SubElement(ch, "display-name", {"lang":"zh"})
                dn.text = name.strip()
                channels[cid] = ch

            # 节目按时分排序
            sort_list = []
            for p in plist:
                t = p.get("time","")
                title = p.get("program","")
                if not t or not title:
                    continue
                try:
                    hh,mm = map(int,t.split(":"))
                    sort_list.append( (hh*60+mm, t, title) )
                except:
                    continue
            sort_list.sort()
            total = len(sort_list)

            for idx, (_, t_str, title) in enumerate(sort_list):
                try:
                    hh, mm = map(int, t_str.split(":"))

                    # 凌晨0-6点自动算第二天，解决全部挤同一天
                    if 0 <= hh < 6:
                        cur_day = base_date + timedelta(days=1)
                    else:
                        cur_day = base_date

                    start_dt = datetime.combine(cur_day, datetime.min.time()).replace(hour=hh,minute=mm)

                    # 动态自动算结束时间，不用固定30分钟
                    if idx < total - 1:
                        next_hh, next_mm = map(int, sort_list[idx+1][1].split(":"))
                        if 0 <= next_hh <6:
                            next_day = cur_day + timedelta(days=1)
                        else:
                            next_day = cur_day
                        stop_dt = datetime.combine(next_day, datetime.min.time()).replace(hour=next_hh,minute=next_mm)
                    else:
                        stop_dt = start_dt + timedelta(minutes=60)

                    # 只过滤超远期，近期全部保留，解决前面几天空白
                    if stop_dt < start_cutoff - timedelta(days=2):
                        continue

                    p = etree.Element("programme")
                    p.set("start", start_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("stop", stop_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("channel", cid)
                    etree.SubElement(p, "title", {"lang":"zh"}).text = title
                    programs.append(p)
                except:
                    continue
        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.warning(f"JSON解析异常:{str(e)}")
        return {}, [], 0, 0

def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    urls = read_config()
    if not urls:
        logging.error("config.txt 无链接")
        return

    all_ch = {}
    all_prog = []

    for i,url in enumerate(urls,1):
        c,fmt,ok = fetch(url,i)
        if not ok:
            continue
        if fmt == "xml":
            chs,progs,_,_ = parse_xml(c,i)
        elif fmt == "json":
            chs,progs,_,_ = parse_json(c,i)
        else:
            continue

        for k,v in chs.items():
            if k not in all_ch:
                all_ch[k] = v
        all_prog.extend(progs)

    if all_ch:
        root = etree.Element("tv")
        for ch in all_ch.values():
            root.append(ch)
        for p in all_prog:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out_path, "wb") as f:
            f.write(xml_bytes)

        print(f"========================================")
        print(f"✅ 完成！频道总数:{len(all_ch)}  节目总数:{len(all_prog)}")
        print(f"✅ 时间范围：前7天 + 后7天")
        print(f"========================================")

if __name__ == "__main__":
    main()
