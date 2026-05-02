import logging
import os
import gzip
import json
import requests
from lxml import etree
from datetime import datetime, timedelta
import io

# 配置
LOG_FILE = "epg_update.log"
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
DAYS_BEFORE = 7
DAYS_AFTER = 7
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 繁转简映射
F2S = {"臺":"台","衛":"卫","視":"视","體":"体","綜":"综","藝":"艺","頻":"频","廣":"广","東":"东"}
def f2s(text):
    if not text: return text
    for a,b in F2S.items():
        text = text.replace(a, b)
    return text.strip()

# 频道标准化（补全所有你截图里的频道）
def unified_name(raw_name):
    if not raw_name: return raw_name
    n = f2s(raw_name).strip()
    lower_n = n.lower()

    # CCTV系列
    if "cctv1" in lower_n or "央视1" in lower_n: return "CCTV1"
    if "cctv2" in lower_n or "央视2" in lower_n: return "CCTV2"
    if "cctv3" in lower_n or "央视3" in lower_n: return "CCTV3"
    if "cctv4" in lower_n or "央视4" in lower_n: return "CCTV4"
    if "cctv5" in lower_n or "央视5" in lower_n: return "CCTV5"
    if "cctv5+" in lower_n or "cctv5plus" in lower_n: return "CCTV5+"
    if "cctv6" in lower_n or "央视6" in lower_n: return "CCTV6"
    if "cctv7" in lower_n or "央视7" in lower_n: return "CCTV7"
    if "cctv8" in lower_n or "央视8" in lower_n: return "CCTV8"
    if "cctv4k" in lower_n: return "CCTV4K"

    # 山东系列频道
    if "山东" in n and "新闻" in n: return "山东新闻"
    if "山东" in n and "文旅" in n: return "山东文旅"
    if "山东" in n and "生活" in n: return "山东生活"
    if "山东" in n and "综艺" in n: return "山东综艺"
    if "山东" in n and "体育" in n and "休闲" not in n: return "山东体育"
    if "山东" in n and "农科" in n: return "山东农科"
    if "山东" in n and "少儿" in n: return "山东少儿"
    if "山东" in n and "教育卫视" in n: return "山东教育卫视"
    if "齐鲁" in n: return "山东齐鲁"
    if "山东卫视" in n: return "山东卫视"

    # 全国卫视
    if "浙江卫视" in n: return "浙江卫视"
    if "宁夏卫视" in n: return "宁夏卫视"
    if "新疆卫视" in n: return "新疆卫视"
    if "甘肃卫视" in n: return "甘肃卫视"
    if "青海卫视" in n: return "青海卫视"
    if "西藏卫视" in n: return "西藏卫视"
    if "三沙卫视" in n: return "三沙卫视"
    if "兵团卫视" in n: return "兵团卫视"
    if "农林卫视" in n: return "农林卫视"
    if "广西卫视" in n: return "广西卫视"
    if "吉林卫视" in n: return "吉林卫视"
    if "云南卫视" in n: return "云南卫视"
    if "陕西卫视" in n: return "陕西卫视"
    if "延边卫视" in n: return "延边卫视"
    if "内蒙古卫视" in n: return "内蒙古卫视"

    # 特色频道（SiTV/NewTV）
    if "都市剧场" in n: return "都市剧场"
    if "欢笑剧场" in n: return "欢笑剧场"
    if "金色学堂" in n: return "金色学堂"
    if "劲爆体育" in n: return "劲爆体育"
    if "乐游" in n: return "乐游"
    if "魅力足球" in n: return "魅力足球"
    if "七彩戏剧" in n: return "七彩戏剧"
    if "生活时尚" in n: return "生活时尚"
    if "游戏风云" in n: return "游戏风云"
    if "中国交通" in n: return "中国交通"

    return n

# 北京时间基准
now = datetime.now()
today = datetime(now.year, now.month, now.day)
start_day = today - timedelta(days=DAYS_BEFORE)
end_day = today + timedelta(days=DAYS_AFTER)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"}

def fetch(url, i):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logging.warning(f"[{i}] 请求失败，状态码：{r.status_code}")
            return None, None, False
        c = r.content
        fmt = "xml" if (c.startswith(b'\x1f\x8b') or b'<tv' in c[:200]) else "json"
        return c, fmt, True
    except Exception as e:
        logging.error(f"[{i}] 请求异常：{str(e)}")
        return None, None, False

# XML解析（酷9兼容版：强制标准时间戳，过滤无效日期）
def parse_xml(content, i):
    if content.startswith(b'\x1f\x8b'):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                content = f.read()
        except:
            pass
    try:
        root = etree.fromstring(content)
    except Exception as e:
        logging.error(f"[{i}] XML解析失败：{str(e)}")
        return {}, []
    chs = {}
    progs = []
    # 处理频道
    for ch in root.xpath("//channel"):
        raw = ch.findtext("display-name", "").strip()
        if not raw: continue
        un = unified_name(raw)
        ch.set("id", un)
        if ch.find("display-name") is not None:
            ch.find("display-name").text = un
        chs[un] = ch
    # 处理节目单（直接读取源里的日期，只保留前后7天的有效数据）
    for p in root.xpath("//programme"):
        rawid = p.get("channel", "").strip()
        if not rawid: continue
        un = unified_name(rawid)
        t = p.find("title")
        title = f2s(t.text) if t is not None else ""
        st_str = p.get("start", "")
        if len(st_str) < 14: continue
        try:
            # 直接解析源里的完整时间
            st_dt = datetime.strptime(st_str[:14], "%Y%m%d%H%M%S")
            st_dt = st_dt.replace(tzinfo=None)
            # 只保留前后7天的节目，过滤无效日期
            if not (start_day <= st_dt <= end_day):
                continue
            stop_str = p.get("stop", "")
            if len(stop_str) >= 14:
                et_dt = datetime.strptime(stop_str[:14], "%Y%m%d%H%M%S")
                et_dt = et_dt.replace(tzinfo=None)
            else:
                et_dt = st_dt + timedelta(minutes=30)
            # 强制生成酷9能识别的时间格式
            p_new = etree.Element("programme")
            p_new.set("start", st_dt.strftime("%Y%m%d%H%M%S +0800"))
            p_new.set("stop", et_dt.strftime("%Y%m%d%H%M%S +0800"))
            p_new.set("channel", un)
            etree.SubElement(p_new, "title").text = title
            progs.append(p_new)
        except Exception as e:
            logging.debug(f"节目解析失败：{st_str} - {str(e)}")
            continue
    return chs, progs

# JSON解析（酷9兼容版：修复无日期源，只生成有效日期节目）
def parse_json(content, i):
    try:
        data = json.loads(content)
    except Exception as e:
        logging.error(f"[{i}] JSON解析失败：{str(e)}")
        return {}, []
    chs = {}
    progs = []
    # 适配两种常见JSON格式
    if isinstance(data, list):
        for item in data:
            # 格式1：带date字段的标准格式
            if "channel_name" in item and "date" in item and "epg_data" in item:
                name = item.get("channel_name", "")
                date_str = item.get("date", "")
                plist = item.get("epg_data", [])
                if not name or not date_str or not plist: continue
                un = unified_name(name)
                if un not in chs:
                    ch = etree.Element("channel", id=un)
                    etree.SubElement(ch, "display-name").text = un
                    chs[un] = ch
                try:
                    base_day = datetime.strptime(date_str, "%Y-%m-%d")
                    if not (start_day <= base_day <= end_day):
                        continue
                except:
                    continue
                for prog in plist:
                    start_str = prog.get("start", "")
                    end_str = prog.get("end", "")
                    title = f2s(prog.get("title", ""))
                    if not start_str or not end_str: continue
                    try:
                        st_time = datetime.strptime(start_str, "%H:%M").time()
                        et_time = datetime.strptime(end_str, "%H:%M").time()
                        st_dt = datetime.combine(base_day, st_time)
                        et_dt = datetime.combine(base_day, et_time)
                        p = etree.Element("programme")
                        p.set("start", st_dt.strftime("%Y%m%d%H%M%S +0800"))
                        p.set("stop", et_dt.strftime("%Y%m%d%H%M%S +0800"))
                        p.set("channel", un)
                        etree.SubElement(p, "title").text = title
                        progs.append(p)
                    except Exception as e:
                        logging.debug(f"节目解析失败：{start_str} - {str(e)}")
                        continue
            # 格式2：无date字段，只有时间的源，按前后7天扩展
            elif "name" in item and "list" in item:
                name = item.get("name", "")
                plist = item.get("list", [])
                if not name or not plist: continue
                un = unified_name(name)
                if un not in chs:
                    ch = etree.Element("channel", id=un)
                    etree.SubElement(ch, "display-name").text = un
                    chs[un] = ch
                # 只扩展前后7天的节目
                for base_day in [today + timedelta(days=d) for d in range(-DAYS_BEFORE, DAYS_AFTER+1)]:
                    for prog in plist:
                        start_str = prog.get("time", "") or prog.get("start", "")
                        end_str = prog.get("end", "") or ""
                        title = f2s(prog.get("program", "") or prog.get("title", ""))
                        if not start_str: continue
                        try:
                            st_time = datetime.strptime(start_str, "%H:%M").time()
                            st_dt = datetime.combine(base_day, st_time)
                            if end_str:
                                et_time = datetime.strptime(end_str, "%H:%M").time()
                                et_dt = datetime.combine(base_day, et_time)
                            else:
                                et_dt = st_dt + timedelta(minutes=30)
                            p = etree.Element("programme")
                            p.set("start", st_dt.strftime("%Y%m%d%H%M%S +0800"))
                            p.set("stop", et_dt.strftime("%Y%m%d%H%M%S +0800"))
                            p.set("channel", un)
                            etree.SubElement(p, "title").text = title
                            progs.append(p)
                        except Exception as e:
                            logging.debug(f"节目解析失败：{start_str} - {str(e)}")
                            continue
    return chs, progs

# 强力去重（按频道+开始时间+结束时间去重，解决重叠）
def dedupe(progs):
    seen = set()
    u = []
    for p in progs:
        key = (p.get("channel"), p.get("start"), p.get("stop"))
        if key not in seen:
            seen.add(key)
            u.append(p)
    return u

def read_config():
    if not os.path.exists(CONFIG_FILE):
        logging.warning("未找到config.txt")
        return []
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    if os.path.exists(out_path):
        os.remove(out_path)
    urls = read_config()
    if not urls:
        logging.error("config.txt为空，退出")
        return
    all_ch = {}
    all_prog = []
    for i, url in enumerate(urls, 1):
        c, fmt, ok = fetch(url, i)
        if not ok:
            continue
        if fmt == "xml":
            chs, progs = parse_xml(c, i)
        else:
            chs, progs = parse_json(c, i)
        for cid, ch in chs.items():
            if cid not in all_ch:
                all_ch[cid] = ch
        all_prog.extend(progs)
    # 兜底频道（所有你截图里的频道）
    required = [
        "CCTV1", "CCTV2", "CCTV3", "CCTV4", "CCTV5", "CCTV5+", "CCTV6", "CCTV7", "CCTV8",
        "山东新闻", "山东文旅", "山东生活", "山东综艺", "山东体育", "山东农科", "山东少儿", "山东教育卫视", "山东齐鲁", "山东卫视",
        "浙江卫视", "宁夏卫视", "新疆卫视", "甘肃卫视", "青海卫视", "西藏卫视", "三沙卫视", "兵团卫视", "农林卫视",
        "广西卫视", "吉林卫视", "云南卫视", "陕西卫视", "延边卫视", "内蒙古卫视",
        "都市剧场", "欢笑剧场", "金色学堂", "劲爆体育", "乐游", "魅力足球", "七彩戏剧", "生活时尚", "游戏风云", "中国交通"
    ]
    for name in required:
        if name not in all_ch:
            ch = etree.Element("channel", id=name)
            etree.SubElement(ch, "display-name").text = name
            all_ch[name] = ch
    # 去重
    all_prog = dedupe(all_prog)
    # 生成XML（强制UTF-8声明，无多余命名空间，酷9完美兼容）
    root = etree.Element("tv")
    for ch in all_ch.values():
        root.append(ch)
    for p in all_prog:
        root.append(p)
    xml = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with gzip.open(out_path, "wb") as f:
        f.write(xml)
    logging.info(f"✅ 完成：频道数{len(all_ch)} 节目数{len(all_prog)}")

if __name__ == "__main__":
    main()
