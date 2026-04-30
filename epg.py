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

# ==================== 强制无缓冲输出 ====================
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 只输出控制台日志（Action能捕获）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 自动时间（不固定）
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
        logging.info(f"[{index}] 下载: {url[:50]}...")
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            logging.warning(f"[{index}] 失败 {r.status_code}")
            return None, None, False
        content = r.content
        fmt = detect_format(content, url)
        logging.info(f"[{index}] 成功 → {fmt}")
        return content, fmt, True
    except Exception as e:
        logging.warning(f"[{index}] 异常: {str(e)[:40]}")
        return None, None, False

# ==================== 格式判断（修复） ====================
def detect_format(content, url):
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                if b'<?xml' in f.read(100):
                    return "xml"
        except:
            pass
    if b'<?xml' in content[:100] or b'<tv' in content[:100]:
        return "xml"
    try:
        content_str = content.decode('utf-8', 'ignore')[:200]
        if content_str.startswith('{') or content_str.startswith('['):
            json.loads(content_str)
            return "json"
    except:
        pass
    if b'#EXTM3U' in content[:100]:
        return "m3u"
    return "unknown"

# ==================== 解析 XML ====================
def parse_xml(content, index):
    channels = {}
    programs = []
    try:
        if content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        s = content.decode('utf-8', 'ignore')
        root = etree.fromstring(s.encode('utf-8'))

        for ch in root.xpath("//channel"):
            cid = ch.get("id")
            if cid:
                cid = re.sub(r'[^a-zA-Z0-9_-]', '', cid)
                channels[cid] = ch

        for p in root.xpath("//programme"):
            st = parse_program_time(p.get("start"))
            if not st: continue
            if start_cutoff <= st <= end_cutoff:
                cid = p.get("channel")
                if cid:
                    p.set("channel", re.sub(r'[^a-zA-Z0-9_-]', '', cid))
                programs.append(p)

        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.warning(f"[{index}] XML解析失败: {str(e)[:40]}")
        return {}, [], 0, 0

# ==================== 解析 JSON（dy2.fun 百川源专属修复版） ====================
def parse_json(content, index):
    channels = {}
    programs = []
    try:
        data = json.loads(content.decode('utf-8', 'ignore'))
        current_base_day = today

        for item in data:
            tvid = item.get("tvid") or item.get("id")
            name = item.get("name")
            plist = item.get("list", [])
            if not tvid or not name or not plist:
                continue

            cid = re.sub(r'[^\w\u4e00-\u9fa5]', '', tvid)
            if not cid:
                cid = re.sub(r'[^\w\u4e00-\u9fa5]', '', name)

            if cid not in channels:
                ch = etree.Element("channel", id=cid)
                dn = etree.SubElement(ch, "display-name", attrib={"lang": "zh"})
                dn.text = name.strip()
                channels[cid] = ch

            prog_list_sorted = []
            for prog in plist:
                t_str = prog.get("time", "")
                title = prog.get("program", "").strip()
                if not t_str or not title:
                    continue
                try:
                    hm = datetime.strptime(t_str.strip(), "%H:%M")
                    prog_list_sorted.append((hm.hour * 60 + hm.minute, t_str, title))
                except:
                    continue

            prog_list_sorted.sort()
            total_cnt = len(prog_list_sorted)

            for idx, (_, t_str, title) in enumerate(prog_list_sorted):
                try:
                    hh, mm = map(int, t_str.split(":"))
                    if 0 <= hh < 6:
                        use_day = current_base_day + timedelta(days=1)
                    else:
                        use_day = current_base_day

                    start_dt = datetime.combine(use_day, datetime.min.time()).replace(hour=hh, minute=mm)

                    if idx < total_cnt - 1:
                        next_hh, next_mm = map(int, prog_list_sorted[idx+1][1].split(":"))
                        next_start = datetime.combine(use_day, datetime.min.time()).replace(hour=next_hh, minute=next_mm)
                        if next_hh >= 0 and next_hh < 6:
                            next_start += timedelta(days=1)
                        stop_dt = next_start
                    else:
                        stop_dt = start_dt + timedelta(minutes=60)

                    if start_dt < start_cutoff or start_dt > end_cutoff:
                        continue

                    p = etree.Element("programme")
                    p.set("start", start_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("stop", stop_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("channel", cid)
                    title_elem = etree.SubElement(p, "title", attrib={"lang": "zh"})
                    title_elem.text = title
                    programs.append(p)

                except Exception as e:
                    logging.debug(f"JSON单节目解析失败:{t_str} {str(e)[:30]}")
                    continue

        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.warning(f"[{index}] JSON整体解析异常: {str(e)[:50]}")
        return {}, [], 0, 0

# ==================== 时间解析 ====================
def parse_program_time(ts):
    if not ts: return None
    try:
        p = ts.split()[0]
        if len(p) >= 14:
            return datetime.strptime(p[:14], "%Y%m%d%H%M%S")
    except:
        pass
    return None

# ==================== 读取配置 ====================
def read_config():
    if not os.path.exists(CONFIG_FILE):
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

# ==================== 主程序 ====================
def main():
    urls = read_config()
    if not urls:
        logging.error("无数据源")
        return

    all_ch = {}
    all_prog = []

    for i, url in enumerate(urls, 1):
        c, fmt, ok = fetch(url, i)
        if not ok or not c:
            continue
        if fmt == "xml":
            chs, progs, _, _ = parse_xml(c, i)
        elif fmt == "json":
            chs, progs, _, _ = parse_json(c, i)
        else:
            logging.warning(f"[{i}] 未知格式，跳过")
            continue

        for cid, ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        all_prog.extend(progs)

    if all_ch and all_prog:
        root = etree.Element("tv")
        root.set("generator-info-name", "Tak IPTV Tool")
        root.set("generator-info-url", "https://github.com/taksssss/iptv-tool")
        for ch in all_ch.values():
            root.append(ch)
        for p in all_prog:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        out = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out, "wb") as f:
            f.write(xml_bytes)
        
        # 同时print和log，确保Action能看到
        print(f"✅ 生成成功：{len(all_ch)}频道 {len(all_prog)}节目")
        logging.info(f"✅ 生成成功：{len(all_ch)}频道 {len(all_prog)}节目")

if __name__ == "__main__":
    main()
