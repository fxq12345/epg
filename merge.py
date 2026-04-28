import os
import gzip
import re
import logging
import io
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
MAX_WORKERS = 5  # 增加线程数
TIMEOUT = 20
CORE_RETRY_COUNT = 2

DAYS_BEFORE = 7
DAYS_AFTER = 7

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ✅ 必保频道列表（这些频道必须有节目）
MUST_HAVE_CHANNELS = {
    # 央视
    "CCTV1", "CCTV2", "CCTV3", "CCTV4", "CCTV5", "CCTV5+", "CCTV6", "CCTV7", "CCTV8",
    "CCTV9", "CCTV10", "CCTV11", "CCTV12", "CCTV13", "CCTV14", "CCTV15", "CCTV16", "CCTV17",
    # 卫视
    "湖南卫视", "浙江卫视", "江苏卫视", "东方卫视", "北京卫视", "山东卫视", "天津卫视",
    "安徽卫视", "深圳卫视", "广东卫视", "东南卫视", "江西卫视", "湖北卫视", "四川卫视",
    "重庆卫视", "黑龙江卫视", "辽宁卫视", "河南卫视", "河北卫视", "山西卫视",
    # 山东本地
    "山东齐鲁", "山东体育休闲", "山东农科", "山东文旅", "山东生活", "山东综艺",
    "山东教育", "山东公共", "山东少儿"
}

# 频道别名映射（更全面的映射）
CHANNEL_ALIAS = {
    # 央视
    "CCTV-1": "CCTV1", "CCTV-1综合": "CCTV1", "CCTV1综合": "CCTV1",
    "CCTV-2": "CCTV2", "CCTV-2财经": "CCTV2", "CCTV2财经": "CCTV2",
    "CCTV-3": "CCTV3", "CCTV-3综艺": "CCTV3", "CCTV3综艺": "CCTV3",
    "CCTV-4": "CCTV4", "CCTV-4中文国际": "CCTV4", "CCTV4中文国际": "CCTV4",
    "CCTV-5": "CCTV5", "CCTV-5体育": "CCTV5", "CCTV5体育": "CCTV5",
    "CCTV-5+": "CCTV5+", "CCTV5+体育赛事": "CCTV5+",
    "CCTV-6": "CCTV6", "CCTV-6电影": "CCTV6", "CCTV6电影": "CCTV6",
    "CCTV-7": "CCTV7", "CCTV-7国防军事": "CCTV7", "CCTV7国防军事": "CCTV7",
    "CCTV-8": "CCTV8", "CCTV-8电视剧": "CCTV8", "CCTV8电视剧": "CCTV8",
    "CCTV-9": "CCTV9", "CCTV-9纪录": "CCTV9", "CCTV9纪录": "CCTV9",
    "CCTV-10": "CCTV10", "CCTV-10科教": "CCTV10", "CCTV10科教": "CCTV10",
    "CCTV-11": "CCTV11", "CCTV-11戏曲": "CCTV11", "CCTV11戏曲": "CCTV11",
    "CCTV-12": "CCTV12", "CCTV-12社会与法": "CCTV12", "CCTV12社会与法": "CCTV12",
    "CCTV-13": "CCTV13", "CCTV-13新闻": "CCTV13", "CCTV13新闻": "CCTV13",
    "CCTV-14": "CCTV14", "CCTV-14少儿": "CCTV14", "CCTV14少儿": "CCTV14",
    "CCTV-15": "CCTV15", "CCTV-15音乐": "CCTV15", "CCTV15音乐": "CCTV15",
    "CCTV-16": "CCTV16", "CCTV-16奥林匹克": "CCTV16", "CCTV16奥林匹克": "CCTV16",
    "CCTV-17": "CCTV17", "CCTV-17农业农村": "CCTV17", "CCTV17农业农村": "CCTV17",
    
    # 卫视
    "湖南卫视HD": "湖南卫视", "湖南卫视高清": "湖南卫视",
    "浙江卫视HD": "浙江卫视", "浙江卫视高清": "浙江卫视",
    "江苏卫视HD": "江苏卫视", "江苏卫视高清": "江苏卫视",
    "东方卫视HD": "东方卫视", "东方卫视高清": "东方卫视",
    "北京卫视HD": "北京卫视", "北京卫视高清": "北京卫视",
    "山东卫视HD": "山东卫视", "山东卫视高清": "山东卫视",
    "天津卫视HD": "天津卫视", "天津卫视高清": "天津卫视",
    "安徽卫视HD": "安徽卫视", "安徽卫视高清": "安徽卫视",
    "深圳卫视HD": "深圳卫视", "深圳卫视高清": "深圳卫视",
    "广东卫视HD": "广东卫视", "广东卫视高清": "广东卫视",
    
    # 山东本地
    "山东齐鲁": "山东齐鲁", "齐鲁频道": "山东齐鲁", "山东齐鲁HD": "山东齐鲁", "山东齐鲁高清": "山东齐鲁",
    "山东体育": "山东体育休闲", "山东体育HD": "山东体育休闲", "山东体育高清": "山东体育休闲", "山东体育休闲频道": "山东体育休闲",
    "山东农科": "山东农科", "山东农科HD": "山东农科", "山东农科高清": "山东农科", "农科频道": "山东农科",
    "山东文旅": "山东文旅", "文旅频道": "山东文旅", "山东影视": "山东文旅", "山东文旅频道": "山东文旅",
    "山东生活": "山东生活", "生活频道": "山东生活", "山东生活频道": "山东生活",
    "山东综艺": "山东综艺", "综艺频道": "山东综艺", "山东综艺频道": "山东综艺",
    "山东教育": "山东教育", "山东教育卫视": "山东教育", "山东教育频道": "山东教育",
    "山东公共": "山东公共", "山东公共频道": "山东公共",
    "山东少儿": "山东少儿", "山东少儿频道": "山东少儿"
}

FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国",
    "泰国", "越南", "印尼", "马来西亚", "新加坡",
    "澳洲", "欧洲", "美洲", "非洲", "俄罗斯", "印度", "巴西"
]

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.orig_id_to_final_id: Dict[str, str] = {}
        self.channel_programs: Dict[str, List] = {}  # 按频道存储节目
        self.channel_sources: Dict[str, Set[str]] = {}  # 频道来源统计
        
        now = datetime.now()
        self.today_start = datetime(now.year, now.month, now.day, 0, 0, 0)
        self.start_cutoff = self.today_start - timedelta(days=DAYS_BEFORE)
        self.end_cutoff = self.today_start + timedelta(days=DAYS_AFTER)
        
        logging.info("=" * 60)
        logging.info("📡 EPG智能合并系统启动")
        logging.info(f"📅 时间范围: {self.start_cutoff.date()} 至 {self.end_cutoff.date()}")
        logging.info(f"🎯 必保频道: {len(MUST_HAVE_CHANNELS)} 个")
        logging.info("=" * 60)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=MAX_WORKERS)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml,text/xml,*/*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def clean_xml_content(self, content: str) -> str:
        content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content = content.replace('& ', '&amp; ')
        return content

    def get_content(self, source: str) -> Tuple[bool, str, str]:
        """获取EPG源内容，返回(是否成功, 状态信息, 内容)"""
        try:
            logging.debug(f"获取源: {source[:50]}...")
            resp = self.session.get(source, timeout=TIMEOUT)
            
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}", ""
            
            data = resp.content
            if data.startswith(b'\x1f\x8b'):
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                    content = f.read().decode('utf-8', errors='ignore')
            else:
                content = data.decode('utf-8', errors='ignore')
            
            cleaned = self.clean_xml_content(content)
            return True, "成功", cleaned
            
        except Exception as e:
            error_msg = str(e)[:60]
            logging.debug(f"源获取失败 {source[:30]}: {error_msg}")
            return False, error_msg, ""

    def read_epg_sources(self) -> List[str]:
        """读取EPG源，并添加高质量备用源"""
        sources = []
        
        # 1. 读取用户配置的源
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith(("http://", "https://")):
                            sources.append(line)
                logging.info(f"从配置读取源: {len(sources)} 个")
            except Exception as e:
                logging.error(f"读取配置失败: {e}")
        
        # 2. 添加高质量备用源（如果用户源不够）
        if len(sources) < 3:
            backup_sources = [
                "https://epg.erw.cc/cctv.xml",      # 央视专用
                "https://epg.erw.cc/weishi.xml",    # 卫视专用
                "https://live.fanmingming.com/e.xml",  # 综合源
                "https://epg.51zmt.top/",           # 综合源
                "http://diyp.112114.xyz/gg/tv/1.txt",  # 备用源
            ]
            sources.extend(backup_sources)
            logging.info(f"添加备用源，总数: {len(sources)} 个")
        
        return sources

    def normalize_channel_name(self, display_name: str) -> str:
        """标准化频道名称"""
        if not display_name:
            return ""
        
        # 清理名称
        name = display_name.strip()
        name = re.sub(r'[\[\]【】()（）]', '', name)
        
        # 移除HD/高清等后缀（但保留在映射中）
        name_lower = name.lower()
        if name_lower.endswith(('hd', '高清', '高码率', '超清')):
            name = name[:-2] if len(name) > 2 else name
        
        # 检查是否外台
        for kw in FOREIGN_KEYWORDS:
            if kw in name:
                return ""
        
        # 别名映射
        for alias, final in CHANNEL_ALIAS.items():
            if alias in name or name in alias:
                return final
        
        # 检查是否必保频道
        for must_have in MUST_HAVE_CHANNELS:
            if must_have in name or name in must_have:
                return must_have
        
        return name

    def process_channels(self, xml_tree, source_url: str):
        """处理频道信息"""
        channels = xml_tree.xpath("//channel")
        for ch in channels:
            orig_id = ch.get("id", "").strip()
            names = ch.xpath(".//display-name/text()")
            if not names:
                continue
            
            display_name = names[0].strip()
            final_name = self.normalize_channel_name(display_name)
            
            if not final_name:
                continue
            
            # 记录频道来源
            if final_name not in self.channel_sources:
                self.channel_sources[final_name] = set()
            self.channel_sources[final_name].add(source_url)
            
            # 如果频道已存在，跳过
            if final_name in self.channel_ids:
                continue
            
            self.orig_id_to_final_id[orig_id] = final_name
            ch.set("id", final_name)
            self.all_channels.append(ch)
            self.channel_ids.add(final_name)
            
            # 初始化节目列表
            if final_name not in self.channel_programs:
                self.channel_programs[final_name] = []

    def parse_program_time(self, ts):
        """解析节目时间"""
        try:
            if not ts:
                return None
            part = ts.split()[0] if " " in ts else ts
            if len(part) >= 14:
                return datetime.strptime(part[:14], "%Y%m%d%H%M%S")
        except Exception as e:
            logging.debug(f"时间解析失败: {ts} - {e}")
        return None

    def is_placeholder(self, title: str) -> bool:
        """判断是否为占位符节目"""
        if not title:
            return True
        
        placeholder_keywords = ["精彩节目", "敬请期待", "节目", "Live", "直播", "即将开始", "暂无信息"]
        title_lower = title.lower()
        
        for kw in placeholder_keywords:
            if kw in title or kw.lower() in title_lower:
                return True
        
        # 检查是否为简单数字或字母
        if re.match(r'^[\d\s\-:]+$', title):
            return True
        
        return False

    def process_programs(self, xml_tree, source_url: str):
        """处理节目信息"""
        programs = xml_tree.xpath("//programme")
        added_count = 0
        
        for p in programs:
            orig_cid = p.get("channel", "").strip()
            final_id = self.orig_id_to_final_id.get(orig_cid)
            
            if not final_id:
                continue
            
            # 设置最终频道ID
            p.set("channel", final_id)
            
            # 调整iHOT频道时间
            if "iHOT" in final_id or "ihot" in final_id.lower():
                self.adjust_program_time(p, hours=8)
            
            # 检查时间
            start_time = self.parse_program_time(p.get("start", ""))
            if not start_time:
                continue
            
            # 检查时间范围
            if not (self.start_cutoff <= start_time <= self.end_cutoff):
                continue
            
            # 检查是否为占位符
            title_elem = p.find("title")
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()
                if self.is_placeholder(title):
                    continue
            
            # 检查是否重复（相同开始时间）
            duplicate = False
            for existing in self.channel_programs.get(final_id, []):
                existing_start = self.parse_program_time(existing.get("start", ""))
                if existing_start and start_time and abs((existing_start - start_time).total_seconds()) < 300:
                    duplicate = True
                    break
            
            if not duplicate:
                if final_id not in self.channel_programs:
                    self.channel_programs[final_id] = []
                self.channel_programs[final_id].append(p)
                added_count += 1
        
        if added_count > 0:
            logging.debug(f"从 {source_url[:30]} 添加 {added_count} 个节目")

    def adjust_program_time(self, program, hours=0):
        """调整节目时间"""
        for attr in ["start", "stop"]:
            ts = program.get(attr, "")
            if not ts or " " not in ts:
                continue
            part, tz = ts.split(" ", 1)
            if len(part) < 14:
                continue
            try:
                dt = datetime.strptime(part[:14], "%Y%m%d%H%M%S")
                dt += timedelta(hours=hours)
                program.set(attr, dt.strftime("%Y%m%d%H%M%S") + " " + tz)
            except:
                pass

    def fill_missing_channels(self):
        """为缺失节目的必保频道添加占位符"""
        logging.info("检查必保频道节目完整性...")
        
        for channel_name in MUST_HAVE_CHANNELS:
            programs = self.channel_programs.get(channel_name, [])
            
            if len(programs) == 0:
                logging.warning(f"❌ 频道 {channel_name} 无节目，添加占位符")
                self.add_placeholder_programs(channel_name)
            elif len(programs) < 10:  # 节目太少
                logging.warning(f"⚠️  频道 {channel_name} 节目太少 ({len(programs)}个)，补充占位符")
                self.add_placeholder_programs(channel_name, count=5)

    def add_placeholder_programs(self, channel_name: str, count: int = 10):
        """为频道添加占位符节目"""
        if channel_name not in self.channel_ids:
            # 如果频道不存在，先创建
            channel_elem = etree.Element("channel")
            channel_elem.set("id", channel_name)
            display_name = etree.SubElement(channel_elem, "display-name")
            display_name.text = channel_name
            self.all_channels.append(channel_elem)
            self.channel_ids.add(channel_name)
        
        if channel_name not in self.channel_programs:
            self.channel_programs[channel_name] = []
        
        # 添加今天和未来几天的占位符
        for day_offset in range(DAYS_AFTER + 1):
            date = self.today_start + timedelta(days=day_offset)
            
            # 每天添加几个时间段的占位符
            for hour in [8, 12, 14, 16, 18, 20, 22]:
                start_time = datetime(date.year, date.month, date.day, hour, 0, 0)
                end_time = start_time + timedelta(hours=2)
                
                if self.start_cutoff <= start_time <= self.end_cutoff:
                    # 创建节目元素
                    program = etree.Element("programme")
                    program.set("start", start_time.strftime("%Y%m%d%H%M%S") + " +0800")
                    program.set("stop", end_time.strftime("%Y%m%d%H%M%S") + " +0800")
                    program.set("channel", channel_name)
                    
                    title_elem = etree.SubElement(program, "title")
                    title_elem.text = f"{channel_name}节目"
                    
                    desc_elem = etree.SubElement(program, "desc")
                    desc_elem.text = f"{channel_name}精彩节目，敬请收看"
                    
                    self.channel_programs[channel_name].append(program)
                    
                    if len(self.channel_programs[channel_name]) >= count:
                        return

    def build_xml_tree(self):
        """构建最终的XML树"""
        root = etree.Element("tv")
        root.set("source-info-name", "EPG智能合并系统")
        root.set("generator-info-name", "Python EPG Merger")
        root.set("generator-info-url", "")
        
        # 添加频道
        for c in self.all_channels:
            root.append(c)
        
        # 添加节目（按频道和时间排序）
        all_programs = []
        for channel_name, programs in self.channel_programs.items():
            # 按开始时间排序
            programs.sort(key=lambda p: self.parse_program_time(p.get("start", "")) or datetime.min)
            all_programs.extend(programs)
        
        # 按时间排序所有节目
        all_programs.sort(key=lambda p: self.parse_program_time(p.get("start", "")) or datetime.min)
        
        for p in all_programs:
            root.append(p)
        
        self.all_programs = all_programs
        return root

    def save_epg(self):
        """保存EPG文件"""
        if not self.all_channels:
            logging.warning("无有效频道")
            return
        
        tree = self.build_xml_tree()
        xml_bytes = etree.tostring(
            tree,
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True
        )
        
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_bytes)
        
        # 打印统计信息
        self.print_statistics()
        
        logging.info(f"✅ EPG生成完成: {gz_path}")
        logging.info(f"📺 频道总数: {len(self.all_channels)}")
        logging.info(f"🎬 节目总数: {len(self.all_programs)}")

    def print_statistics(self):
        """打印详细统计信息"""
        logging.info("=" * 60)
        logging.info("📊 EPG统计报告")
        logging.info("=" * 60)
        
        # 必保频道统计
        must_have_with_programs = 0
        must_have_no_programs = []
        
        for channel in MUST_HAVE_CHANNELS:
            programs = self.channel_programs.get(channel, [])
            if len(programs) > 0:
                must_have_with_programs += 1
            else:
                must_have_no_programs.append(channel)
        
        logging.info(f"必保频道覆盖: {must_have_with_programs}/{len(MUST_HAVE_CHANNELS)}")
        
        if must_have_no_programs:
            logging.warning(f"无节目的必保频道: {', '.join(must_have_no_programs)}")
        
        # 频道来源统计
        logging.info("\n📡 频道来源统计:")
        for channel, sources in sorted(self.channel_sources.items(), key=lambda x: len(x[1]), reverse=True)[:20]:
            if channel in MUST_HAVE_CHANNELS:
                source_count = len(sources)
                status = "✅" if source_count >= 2 else "⚠️ " if source_count == 1 else "❌"
                logging.info(f"  {status} {channel:20} -> {source_count:2} 个源")
        
        # 节目数量统计
        logging.info("\n🎬 节目数量统计 (前20个频道):")
        sorted_channels = sorted(
            [(name, len(progs)) for name, progs in self.channel_programs.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        for i, (channel, count) in enumerate(sorted_channels[:20], 1):
            status = "✅" if count > 20 else "⚠️ " if count > 5 else "❌"
            logging.info(f"  {i:2}. {status} {channel:20} -> {count:4} 个节目")
        
        if len(sorted_channels) > 20:
            logging.info(f"  ... 还有 {len(sorted_channels) - 20} 个频道")

    def run(self):
        """主运行函数"""
        sources = self.read_epg_sources()
        if not sources:
            logging.error("❌ 没有可用的EPG源")
            return
        
        logging.info(f"开始处理 {len(sources)} 个EPG源...")
        
        # 多线程获取和处理
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for source in sources:
                futures.append(executor.submit(self.get_content, source))
            
            successful_sources = 0
            for i, future in enumerate(as_completed(futures), 1):
                success, message, content = future.result()
                source_url = sources[i-1]
                
                if success and content:
                    try:
                        tree = etree.fromstring(content.encode("utf-8"))
                        self.process_channels(tree, source_url)
                        self.process_programs(tree, source_url)
                        successful_sources += 1
                        logging.info(f"✅ 处理成功: {source_url[:50]}...")
                    except Exception as e:
                        logging.warning(f"❌ 解析失败 {source_url[:30]}: {e}")
                else:
                    logging.warning(f"❌ 获取失败 {source_url[:30]}: {message}")
        
        logging.info(f"成功处理 {successful_sources}/{len(sources)} 个源")
        
        if successful_sources == 0:
            logging.error("❌ 所有EPG源都处理失败！")
            return
        
        # 填充缺失的频道
        self.fill_missing_channels()
        
        # 保存EPG
        self.save_epg()

if __name__ == "__main__":
    # 设置日志级别
    logging.getLogger().setLevel(logging.INFO)
    
    generator = EPGGenerator()
    generator.run()
