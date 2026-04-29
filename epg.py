import os
import gzip
import json
import re
import requests
from lxml import etree
from datetime import datetime, timedelta
import logging
import io
from hanziconv import HanziConv

# =============================================
# 配置区域
# =============================================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epg_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 安全繁转简：只转频道名，不破坏节目
def to_simple_chars(text):
    if not text:
        return text
    return HanziConv.toSimplified(text)

# ========== 重点：已改成【自动真实时间】，不再固定4月29日 ==========
# now = datetime(2026, 4, 29, 12, 0, 0)
now = datetime.now()

today = datetime(now.year, now.month, now.day, 0, 0, 0)
start_cutoff = today - timedelta(days=DAYS_BEFORE)
end_cutoff = today + timedelta(days=DAYS_AFTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def fetch(url, index):
    try:
        logging.info(f"[{index}] 📡 正在获取: {url[:50]}...")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            logging.warning(f"[{index}] ❌ HTTP错误 {r.status_code}")
            return None, None, False
        
        content = r.content
        content_type = r.headers.get('Content-Type', '').lower()
        format_type = detect_format(content, url, content_type)
        
        if "baichuan" in url.lower() or "bc" in url.lower():
            logging.info(f"[{index}] 🟦 检测到百川源")
        
        logging.info(f"[{index}] ✅ 获取成功 ({format_type.upper()})")
        return content, format_type, True
    except Exception as e:
        logging.warning(f"[{index}] ❌ 获取异常: {str(e)[:50]}")
        return None, None, False

def detect_format(content, url, content_type):
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                f.read()
            return "xml"
        except:
            return "gzip"

    if b'<?xml' in content[:100] or b'<tv' in content[:100]:
        return "xml"
    if content.startswith(b'{') or content.startswith(b'['):
        return "json"
    if b'#EXTM3U' in content[:100]:
        return "m3u"
    if 'text' in content_type:
        return "txt"
    return "unknown"

def parse(content, format_type, index):
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0

    try:
        if isinstance(content, bytes):
            if content.startswith(b'\x1f\x8b'):
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                    content = f.read()

            try:
                content_str = content.decode('utf-8')
            except UnicodeDecodeError:
                content_str = content.decode('gbk', errors='ignore')

            root = etree.fromstring(content_str.encode('utf-8'))

            # 仅频道名繁转简，节目完全不动
            for ch in root.xpath("//channel"):
                cid = ch.get("id")
                if not cid:
                    continue
                clean_cid = re.sub(r'[^a-zA-Z0-9_-]', '', cid)
                ch.set("id", clean_cid)
                dn = ch.find("display-name")
                if dn is not None and dn.text:
                    dn.text = to_simple_chars(dn.text)
                channels[clean_cid] = ch
                channel_count += 1

            # 节目原样保留，不修改、不丢失
            for p in root.xpath("//programme"):
                st = parse_program_time(p.get("start", ""))
                et = parse_program_time(p.get("stop", ""))
                if not st:
                    continue
                if start_cutoff <= st <= end_cutoff:
                    cid = p.get("channel")
                    if cid:
                        p.set("channel", re.sub(r'[^a-zA-Z0-9_-]', '', cid))
                    programs.append(p)
                    program_count += 1

        elif format_type == "json":
            return parse_json_format(content, index)

        elif format_type in ("txt","m3u"):
            return parse_text_format(content, index)

        logging.info(f"[{index}] 📺 频道:{channel_count} 节目:{program_count}")
        return channels, programs, channel_count, program_count
    except Exception as e:
        logging.error(f"[{index}] ❌ 解析异常:{e}")
        return {}, [], 0, 0

def parse_text_format(content, index):
    channels = {}
    programs = []
    try:
        text = content.decode('utf-8', errors='ignore')
        for line in text.splitlines():
            line = line.strip()
            if line.startswith('#EXTM3U'):
                tid = re.search(r'tvg-id="([^"]+)"', line)
                name = re.search(r',(.+)$', line)
                if tid and name:
                    tvgid = tid.group(1)
                    cname = to_simple_chars(name.group(1))
                    ch = etree.Element("channel", id=tvgid)
                    etree.SubElement(ch, "display-name").text = cname
                    channels[tvgid] = ch
    except:
        pass
    return channels, programs, len(channels), 0

def parse_json_format(content, index):
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0
    try:
        data = json.loads(content.decode('utf-8', errors='ignore'))
        if not isinstance(data, list):
            return {}, [], 0, 0

        for item in data:
            tvid = item.get("tvid") or item.get("id")
            name = item.get("name")
            plist = item.get("list", [])
            if not tvid or not name:
                continue

            cid = re.sub(r'[^a-zA-Z0-9_-]', '', tvid)
            cname = to_simple_chars(name)
            if cid not in channels:
                celem = etree.Element("channel", id=cid)
                etree.SubElement(celem, "display-name").text = cname
                channels[cid] = celem
                channel_count += 1

            for prog in plist:
                tstr = prog.get("time","")
                title = prog.get("program","")
                try:
                    base_dt = datetime.combine(today, datetime.strptime(tstr, "%H:%M").time())
                    # 智能前后浮动，保证7天历史+7天未来不断档
                    while base_dt > end_cutoff:
                        base_dt -= timedelta(days=1)
                    while base_dt < start_cutoff:
                        base_dt += timedelta(days=1)

                    if start_cutoff <= base_dt <= end_cutoff:
                        stop_dt = base_dt + timedelta(minutes=30)
                        p = etree.Element("programme")
                        p.set("start", base_dt.strftime("%Y%m%d%H%M%S 0"))
                        p.set("stop", stop_dt.strftime("%Y%m%d%H%M%S 0"))
                        p.set("channel", cid)
                        etree.SubElement(p, "title").text = title
                        programs.append(p)
                        program_count += 1
                except:
                    continue
        return channels, programs, channel_count, program_count
    except Exception as e:
        logging.error(f"JSON解析异常:{e}")
        return {}, [], 0, 0

def parse_program_time(time_str):
    if not time_str:
        return None
    try:
        tp = time_str.split()[0]
        if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", tp):
            return datetime.strptime(tp, "%Y-%m-%d %H:%M")
        if len(tp)>=14:
            return datetime.strptime(tp[:14], "%Y%m%d%H%M%S")
    except:
        pass
    return None

def read_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE,"w",encoding="utf-8") as f:
            f.write("")
        return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    logging.info(f"📋 读取{len(urls)}个源")
    return urls

def main():
    urls = read_config()
    if not urls:
        return

    all_channels = {}
    all_programs = []

    for i,url in enumerate(urls,1):
        cont, ftype, ok = fetch(url,i)
        if ok:
            chs, progs, _, _ = parse(cont, ftype, i)
            # 旧频道不覆盖，保住所有节目不丢失
            for cid,ch in chs.items():
                if cid not in all_channels:
                    all_channels[cid] = ch
            all_programs.extend(progs)

    if all_channels and all_programs:
        root = etree.Element("tv")
        root.set("generator-info-name","自动时间+安全繁转简+百川修复版")
        for ch in all_channels.values():
            root.append(ch)
        for p in all_programs:
            root.append(p)

        xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out_path, "wb") as f:
            f.write(xml_bytes)

        logging.info(f"✅ 生成完成 | 频道:{len(all_channels)} 节目:{len(all_programs)}")
    else:
        logging.error("❌ 无有效数据")

if __name__ == "__main__":
    main()
