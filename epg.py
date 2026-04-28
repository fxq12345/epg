import os
import gzip
import re
import logging
import io
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
LOG_FILE = "epg_merge.log"

# 时间范围
DAYS_BEFORE = 7
DAYS_AFTER = 7

# 请求配置
TIMEOUT = 10
RETRY_COUNT = 2

# 山东台别名映射
SHANDONG_ALIAS = {
    "山东卫视": ["山东卫视", "山东卫视HD", "山东卫视高清", "SDWS"],
    "山东齐鲁": ["山东齐鲁", "齐鲁频道", "山东齐鲁HD", "SDQL"],
    "山东体育": ["山东体育", "山东体育休闲", "山东体育HD", "SDTY"],
    "山东农科": ["山东农科", "农科频道", "SDNK"],
    "山东文旅": ["山东文旅", "文旅频道", "山东影视", "SDWL"],
    "山东生活": ["山东生活", "生活频道", "SDSH"],
    "山东综艺": ["山东综艺", "综艺频道", "SDZY"],
    "山东公共": ["山东公共", "公共频道", "SDPB"],
    "山东少儿": ["山东少儿", "少儿频道", "SDSE"]
}

# 时区修正规则
TZ_ADJUSTMENTS = {
    "iHOT": 8,  # iHOT 源需要 +8 小时
    "epg.112114.xyz": 8,  # 某些源需要时区修正
    "51zmt": 0,  # 51zmt 已经是北京时间
    "fanmingming": 0,  # fanmingming 已经是北京时间
}

# ==================================================

# 创建日志配置
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

class EnhancedEPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channels: Dict[str, Dict] = {}  # 频道ID -> 频道信息
        self.programs: Dict[str, List] = {}  # 频道ID -> 节目列表
        self.source_stats: List[Dict] = []  # 源状态统计
        
        now = datetime.now()
        self.today = datetime(now.year, now.month, now.day, 0, 0, 0)
        self.start_cutoff = self.today - timedelta(days=DAYS_BEFORE)
        self.end_cutoff = self.today + timedelta(days=DAYS_AFTER)
        
        # 创建频道别名反向映射
        self.alias_reverse: Dict[str, str] = {}
        for main_name, aliases in SHANDONG_ALIAS.items():
            for alias in aliases:
                self.alias_reverse[alias] = main_name
        
        logging.info("=" * 60)
        logging.info("🚀 EPG智能合并系统启动")
        logging.info(f"📅 时间范围: {self.start_cutoff.date()} 至 {self.end_cutoff.date()}")
        logging.info("=" * 60)
    
    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=RETRY_COUNT,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504, 522, 524]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml,text/xml,*/*;q=0.9",
            "Accept-Encoding": "gzip, deflate"
        })
        return session
    
    def read_config(self) -> List[str]:
        """读取config.txt中的源列表"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"❌ 配置文件 {CONFIG_FILE} 不存在")
            return []
        
        urls = []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                for i, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    
                    # 支持多种格式
                    if line.startswith(("http://", "https://")):
                        urls.append(line)
                    elif "://" in line:  # 其他协议
                        urls.append(line)
                    else:
                        logging.warning(f"⚠️  跳过第 {i} 行（非URL格式）: {line}")
        except Exception as e:
            logging.error(f"❌ 读取配置文件失败: {e}")
        
        logging.info(f"📋 读取到 {len(urls)} 个EPG源")
        return urls
    
    def detect_format(self, content: bytes, url: str) -> Tuple[str, str]:
        """
        检测EPG源的格式
        返回: (格式类型, 解码后的内容)
        支持: xml, gz, txt, json, m3u
        """
        # 检查是否为GZIP压缩
        if content.startswith(b'\x1f\x8b'):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                    decompressed = f.read()
                # 递归检测解压后的格式
                return self.detect_format(decompressed, url)
            except Exception as e:
                logging.warning(f"GZIP解压失败: {e}")
                return "unknown", content.decode('utf-8', errors='ignore')
        
        # 尝试解码为字符串
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text = content.decode('gbk', errors='ignore')
            except:
                text = content.decode('latin-1', errors='ignore')
        
        # 根据内容特征判断格式
        if '<?xml' in text[:100]:
            return "xml", text
        elif '<tv' in text[:100]:
            return "xml", text
        elif 'http://' in text[:100] or '#EXTM3U' in text[:100]:
            return "m3u", text
        elif '{' in text[:100] and '}' in text[:100]:
            return "json", text
        elif re.search(r'\d{14}', text[:200]):  # 包含时间戳
            return "txt", text
        else:
            return "unknown", text
    
    def fetch_epg_source(self, url: str, index: int) -> Optional[str]:
        """获取EPG源内容"""
        try:
            logging.info(f"[{index}] 📡 获取: {url[:60]}...")
            response = self.session.get(url, timeout=TIMEOUT)
            
            if response.status_code != 200:
                logging.warning(f"[{index}] ❌ HTTP {response.status_code}: {url[:50]}...")
                return None
            
            # 检测格式并解码
            format_type, content = self.detect_format(response.content, url)
            logging.info(f"[{index}] ✅ 格式: {format_type.upper()}, 大小: {len(content):,} 字符")
            
            return content
            
        except requests.exceptions.Timeout:
            logging.warning(f"[{index}] ❌ 超时: {url[:50]}...")
            return None
        except requests.exceptions.ConnectionError:
            logging.warning(f"[{index}] ❌ 连接失败: {url[:50]}...")
            return None
        except Exception as e:
            logging.warning(f"[{index}] ❌ 获取失败: {url[:50]}... 错误: {str(e)[:50]}")
            return None
    
    def normalize_channel_name(self, name: str) -> str:
        """标准化频道名称"""
        if not name:
            return ""
        
        # 清理空格和特殊字符
        name = name.strip()
        name = re.sub(r'[\[\]【】()（）]', '', name)
        
        # 移除后缀
        suffixes = ['HD', '高清', '高码率', '超清', 'FHD', '4K']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # 检查是否为山东台别名
        for alias, main_name in self.alias_reverse.items():
            if alias in name or name in alias:
                return main_name
        
        return name
    
    def parse_xml_epg(self, content: str, url: str) -> Tuple[Dict, Dict]:
        """解析XML格式的EPG"""
        channels = {}
        programs = {}
        
        try:
            # 清理XML内容
            content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
            content = content.replace('& ', '&amp; ')
            
            # 解析XML
            root = etree.fromstring(content.encode('utf-8'))
            
            # 解析频道
            for channel in root.xpath("//channel"):
                cid = channel.get("id", "").strip()
                if not cid:
                    continue
                
                # 获取显示名称
                display_names = channel.xpath(".//display-name/text()")
                if display_names:
                    display_name = display_names[0].strip()
                else:
                    display_name = cid
                
                # 标准化频道名
                normalized_name = self.normalize_channel_name(display_name)
                if normalized_name:
                    channels[cid] = {
                        "id": cid,
                        "name": normalized_name,
                        "element": channel
                    }
            
            # 解析节目
            for program in root.xpath("//programme"):
                channel_id = program.get("channel", "").strip()
                if not channel_id or channel_id not in channels:
                    continue
                
                # 获取时间
                start_time = self.parse_program_time(program.get("start", ""))
                if not start_time:
                    continue
                
                # 检查时间范围
                if not (self.start_cutoff <= start_time <= self.end_cutoff):
                    continue
                
                # 获取节目标题
                title_elem = program.find("title")
                title = title_elem.text.strip() if title_elem is not None and title_elem.text else "节目"
                
                # 跳过占位符节目
                if self.is_placeholder(title):
                    continue
                
                # 时区调整
                adjusted_program = self.adjust_timezone(program, url)
                
                # 添加到节目列表
                channel_key = channels[channel_id]["name"]
                if channel_key not in programs:
                    programs[channel_key] = []
                
                programs[channel_key].append(adjusted_program)
            
            return channels, programs
            
        except Exception as e:
            logging.warning(f"XML解析失败: {e}")
            return {}, {}
    
    def parse_m3u_epg(self, content: str, url: str) -> Tuple[Dict, Dict]:
        """解析M3U格式的EPG（简单实现）"""
        channels = {}
        programs = {}
        
        lines = content.split('\n')
        current_channel = None
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('#EXTINF:'):
                # 解析频道信息
                match = re.search(r'tvg-id="([^"]+)"', line)
                if match:
                    tvg_id = match.group(1)
                    channel_name = re.search(r',([^,]+)$', line)
                    if channel_name:
                        name = channel_name.group(1).strip()
                        channels[tvg_id] = {
                            "id": tvg_id,
                            "name": self.normalize_channel_name(name),
                            "element": None
                        }
                        current_channel = tvg_id
            elif line.startswith('http://') or line.startswith('https://'):
                # 这是频道URL，可以跳过
                pass
        
        return channels, programs
    
    def parse_txt_epg(self, content: str, url: str) -> Tuple[Dict, Dict]:
        """解析TXT格式的EPG（简单实现）"""
        # 这里可以扩展解析特定的TXT格式
        return {}, {}
    
    def parse_program_time(self, time_str: str) -> Optional[datetime]:
        """解析节目时间，支持多种格式"""
        if not time_str:
            return None
        
        try:
            # 移除时区信息
            time_part = time_str.split()[0] if ' ' in time_str else time_str
            
            # 支持多种格式
            formats = [
                "%Y%m%d%H%M%S",  # 20250128120000
                "%Y-%m-%d %H:%M:%S",  # 2025-01-28 12:00:00
                "%Y/%m/%d %H:%M:%S",  # 2025/01/28 12:00:00
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(time_part[:len(fmt)], fmt)
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    def is_placeholder(self, title: str) -> bool:
        """判断是否为占位符节目"""
        placeholders = ["精彩节目", "敬请期待", "节目", "Live", "直播", "即将开始", "暂无信息"]
        return any(ph in title for ph in placeholders)
    
    def adjust_timezone(self, program, url: str):
        """调整节目时区"""
        for key, hours in TZ_ADJUSTMENTS.items():
            if key in url and hours != 0:
                for attr in ["start", "stop"]:
                    time_val = program.get(attr, "")
                    if not time_val or " " not in time_val:
                        continue
                    
                    time_part, tz_part = time_val.split(" ", 1)
                    if len(time_part) >= 14:
                        try:
                            dt = datetime.strptime(time_part[:14], "%Y%m%d%H%M%S")
                            dt += timedelta(hours=hours)
                            program.set(attr, dt.strftime("%Y%m%d%H%M%S") + " " + tz_part)
                        except:
                            pass
        
        return program
    
    def merge_channels_and_programs(self, all_channels: Dict, all_programs: Dict, url: str, index: int):
        """合并频道和节目数据"""
        # 合并频道
        for cid, channel_info in all_channels.items():
            if cid not in self.channels:
                self.channels[cid] = channel_info
        
        # 合并节目
        for channel_name, prog_list in all_programs.items():
            if channel_name not in self.programs:
                self.programs[channel_name] = []
            
            # 去重：相同开始时间的节目只保留一个
            seen_times = set()
            for prog in prog_list:
                start_time = self.parse_program_time(prog.get("start", ""))
                if start_time:
                    time_key = f"{channel_name}_{start_time.strftime('%Y%m%d%H%M')}"
                    if time_key not in seen_times:
                        seen_times.add(time_key)
                        self.programs[channel_name].append(prog)
        
        # 统计山东台
        shandong_channels = [name for name in self.programs.keys() if "山东" in name]
        
        return {
            "index": index,
            "url": url[:50],
            "status": "✅ 成功",
            "channels": len(all_channels),
            "programs": sum(len(progs) for progs in all_programs.values()),
            "shandong_channels": shandong_channels
        }
    
    def fill_missing_shandong(self):
        """填充缺失的山东台节目"""
        logging.info("检查山东台节目完整性...")
        
        for shandong_name in SHANDONG_ALIAS.keys():
            if shandong_name not in self.programs or len(self.programs[shandong_name]) < 5:
                logging.warning(f"⚠️  频道 {shandong_name} 节目不足，添加占位符")
                self.add_placeholder_programs(shandong_name)
    
    def add_placeholder_programs(self, channel_name: str):
        """为频道添加占位符节目"""
        if channel_name not in self.channels:
            # 创建频道
            channel_elem = etree.Element("channel")
            channel_elem.set("id", channel_name)
            display_name = etree.SubElement(channel_elem, "display-name")
            display_name.text = channel_name
            self.channels[channel_name] = {
                "id": channel_name,
                "name": channel_name,
                "element": channel_elem
            }
        
        if channel_name not in self.programs:
            self.programs[channel_name] = []
        
        # 为未来几天添加节目
        for day_offset in range(DAYS_AFTER + 1):
            date = self.today + timedelta(days=day_offset)
            
            for hour in [8, 12, 16, 20, 22]:
                start_time = datetime(date.year, date.month, date.day, hour, 0, 0)
                end_time = start_time + timedelta(hours=2)
                
                if self.start_cutoff <= start_time <= self.end_cutoff:
                    program = etree.Element("programme")
                    program.set("start", start_time.strftime("%Y%m%d%H%M%S +0800"))
                    program.set("stop", end_time.strftime("%Y%m%d%H%M%S +0800"))
                    program.set("channel", channel_name)
                    
                    title = etree.SubElement(program, "title")
                    title.text = f"{channel_name}节目"
                    
                    desc = etree.SubElement(program, "desc")
                    desc.text = f"请关注{channel_name}节目预告"
                    
                    self.programs[channel_name].append(program)
    
    def generate_epg(self) -> bytes:
        """生成最终的EPG XML"""
        root = etree.Element("tv")
        root.set("generator-info-name", "EPG智能合并系统")
        root.set("generator-info-url", "")
        root.set("date", datetime.now().strftime("%Y%m%d%H%M%S"))
        
        # 添加频道
        for channel_info in self.channels.values():
            if "element" in channel_info and channel_info["element"] is not None:
                root.append(channel_info["element"])
        
        # 添加节目（按时间排序）
        all_programs = []
        for channel_name, prog_list in self.programs.items():
            for prog in prog_list:
                all_programs.append((prog, self.parse_program_time(prog.get("start", "")) or datetime.min))
        
        # 按时间排序
        all_programs.sort(key=lambda x: x[1])
        
        for prog, _ in all_programs:
            root.append(prog)
        
        # 生成XML
        xml_bytes = etree.tostring(
            root,
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True
        )
        
        return xml_bytes
    
    def save_epg(self, xml_bytes: bytes):
        """保存EPG为GZIP文件"""
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        with gzip.open(output_path, "wb") as f:
            f.write(xml_bytes)
        
        return output_path
    
    def print_statistics(self):
        """打印统计信息"""
        logging.info("=" * 60)
        logging.info("📊 EPG合并统计报告")
        logging.info("=" * 60)
        
        # 源状态汇总
        success_count = 0
        for stat in self.source_stats:
            status_icon = "✅" if "成功" in stat["status"] else "❌"
            logging.info(f"[{stat['index']}] {status_icon} {stat['status']} | "
                        f"📺 {stat['channels']} 频道 | 📅 {stat['programs']} 节目")
            
            if "成功" in stat["status"]:
                success_count += 1
                if stat.get("shandong_channels"):
                    logging.info(f"     🟢 包含山东台: {', '.join(stat['shandong_channels'][:3])}" + 
                                (f" 等{len(stat['shandong_channels'])}个" if len(stat['shandong_channels']) > 3 else ""))
        
        logging.info("-" * 60)
        logging.info(f"总计: {success_count}/{len(self.source_stats)} 个源成功")
        
        # 频道和节目统计
        shandong_count = len([name for name in self.programs.keys() if "山东" in name])
        total_programs = sum(len(progs) for progs in self.programs.values())
        
        logging.info(f"📺 总频道数: {len(self.channels)}")
        logging.info(f"📅 总节目数: {total_programs}")
        logging.info(f"🏠 山东台数: {shandong_count}")
        
        # 山东台详情
        if shandong_count > 0:
            logging.info("\n🔍 山东台节目统计:")
            for channel_name in sorted(self.programs.keys()):
                if "山东" in channel_name:
                    prog_count = len(self.programs[channel_name])
                    status = "🟢" if prog_count > 10 else "🟡" if prog_count > 0 else "🔴"
                    logging.info(f"  {status} {channel_name:15} -> {prog_count:4} 个节目")
    
    def run(self):
        """主运行函数"""
        urls = self.read_config()
        if not urls:
            logging.error("❌ 没有可用的EPG源，退出")
            return
        
        # 处理每个源
        for i, url in enumerate(urls, 1):
            content = self.fetch_epg_source(url, i)
            if not content:
                self.source_stats.append({
                    "index": i,
                    "url": url[:50],
                    "status": "❌ 失败",
                    "channels": 0,
                    "programs": 0
                })
                continue
            
            # 检测并解析格式
            format_type, _ = self.detect_format(content.encode('utf-8'), url)
            
            if format_type == "xml":
                channels, programs = self.parse_xml_epg(content, url)
            elif format_type == "m3u":
                channels, programs = self.parse_m3u_epg(content, url)
            elif format_type == "txt":
                channels, programs = self.parse_txt_epg(content, url)
            else:
                logging.warning(f"[{i}] ⚠️  不支持格式: {format_type}")
                channels, programs = {}, {}
            
            # 合并数据
            stat = self.merge_channels_and_programs(channels, programs, url, i)
            self.source_stats.append(stat)
        
        # 填充缺失的山东台
        self.fill_missing_shandong()
        
        # 生成EPG
        if self.channels and any(len(progs) > 0 for progs in self.programs.values()):
            xml_bytes = self.generate_epg()
            output_path = self.save_epg(xml_bytes)
            
            # 打印统计
            self.print_statistics()
            
            logging.info("=" * 60)
            logging.info(f"✅ EPG生成完成: {output_path}")
            logging.info("=" * 60)
        else:
            logging.error("❌ 没有有效的频道或节目，无法生成EPG")

if __name__ == "__main__":
    generator = EnhancedEPGGenerator()
    generator.run()
