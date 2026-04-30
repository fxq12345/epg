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

# 【只加强：CCTV4/5/5+】精准强制映射
FORCE = {
    "cctv4": "cctv4", "央视4": "cctv4", "央視4": "cctv4", "国际台": "cctv4",
    "cctv5": "cctv5", "央视5": "cctv5", "央視5": "cctv5", "体育": "cctv5",
    "cctv5+": "cctv5plus", "央视5+": "cctv5plus", "央視5+": "cctv5plus", "5+": "cctv5plus"
}

F2S = {"臺":"台","衛":"卫","視":"视","體":"体"}
def f2s(text):
    for a,b in F2S.items(): text=text.replace(a,b)
    return text

now=datetime.now()
today=datetime(now.year,now.month,now.day)
start=today-timedelta(days=DAYS_BEFORE)
end=today+timedelta(days=DAYS_AFTER)

HEADERS={"User-Agent":"Mozilla/5.0"}

def fetch(url,i):
    try:
        r=requests.get(url,headers=HEADERS,timeout=15)
        if r.status_code!=200: return None,None,False
        c=r.content
        fmt="xml" if c[:2]==b'\x1f\x8b' or b'<tv' in c[:100] else "json"
        return c,fmt,True
    except:
        return None,None,False

def parse_time(s):
    try:
        return datetime.strptime(s[:14],"%Y%m%d%H%M%S")
    except:
        return None

def parse_xml(content,i):
    if content[:2]==b'\x1f\x8b':
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f: content=f.read()
    root=etree.fromstring(content)
    chs={}
    progs=[]

    for ch in root.xpath("//channel"):
        raw=ch.get("id")
        name=ch.findtext("display-name","")
        raw=f2s(raw)
        name=f2s(name)

        # 精准匹配CCTV4/5/5+
        newid=None
        for k,v in FORCE.items():
            if k in raw or k in name:
                newid=v
                break
        if newid is None:
            newid=re.sub(r"[^a-zA-Z0-9_-]","",raw).lower()

        ch.set("id",newid)
        if ch.find("display-name") is not None:
            ch.find("display-name").text=name
        chs[newid]=ch

    for p in root.xpath("//programme"):
        st=parse_time(p.get("start",""))
        if not st or not (start<=st<=end): continue
        raw=f2s(p.get("channel",""))
        newid=None
        for k,v in FORCE.items():
            if k in raw:
                newid=v
                break
        if newid is None:
            newid=re.sub(r"[^a-zA-Z0-9_-]","",raw).lower()
        p.set("channel",newid)
        t=p.find("title")
        if t is not None: t.text=f2s(t.text)
        progs.append(p)

    return chs,progs

def parse_json(content,i):
    data=json.loads(content)
    chs={}
    progs=[]
    for item in data:
        tvid=item.get("tvid") or item.get("id")
        name=item.get("name","")
        plist=item.get("list",[])
        if not tvid: continue
        tvid=f2s(tvid)
        name=f2s(name)

        newid=None
        for k,v in FORCE.items():
            if k in tvid or k in name:
                newid=v
                break
        if newid is None:
            newid=re.sub(r"[^a-zA-Z0-9_-]","",tvid).lower()

        if newid not in chs:
            ch=etree.Element("channel",id=newid)
            etree.SubElement(ch,"display-name").text=name
            chs[newid]=ch

        for prog in plist:
            t=prog.get("time","")
            title=f2s(prog.get("program",""))
            try:
                bt=datetime.combine(today,datetime.strptime(t,"%H:%M").time())
                while bt<start: bt+=timedelta(days=1)
                while bt>end: bt-=timedelta(days=1)
                et=bt+timedelta(minutes=30)
                p=etree.Element("programme")
                p.set("start",bt.strftime("%Y%m%d%H%M%S 0"))
                p.set("stop",et.strftime("%Y%m%d%H%M%S 0"))
                p.set("channel",newid)
                etree.SubElement(p,"title").text=title
                progs.append(p)
            except:
                pass
    return chs,progs

def read_config():
    if not os.path.exists(CONFIG_FILE): return []
    with open(CONFIG_FILE,"r",encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def main():
    urls=read_config()
    all_ch={}
    all_prog=[]
    for i,url in enumerate(urls,1):
        c,fmt,ok=fetch(url,i)
        if not ok: continue
        if fmt=="xml":
            chs,progs=parse_xml(c,i)
        else:
            chs,progs=parse_json(c,i)
        for cid,ch in chs.items():
            if cid not in all_ch:
                all_ch[cid]=ch
        all_prog.extend(progs)

    if all_ch and all_prog:
        root=etree.Element("tv")
        for ch in all_ch.values(): root.append(ch)
        for p in all_prog: root.append(p)
        xml=etree.tostring(root,encoding="utf-8",xml_declaration=True)
        with gzip.open(os.path.join(OUTPUT_DIR,OUTPUT_FILE),"wb") as f:
            f.write(xml)
        logging.info(f"✅ 完成 | 频道:{len(all_ch)} 节目:{len(all_prog)}")
    else:
        logging.warning("⚠️ 无数据")

if __name__=="__main__":
    main()
