import os
import gzip
import re
import time
import logging
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
CORE_RETRY_COUNT = 2

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 核心频道配置（山东本地→央视→其他卫视优先级）
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 最新完整版酷9专用ID映射表（包含最新4K频道）
COOL9_ID_MAPPING = {
    # =========== 山东本地频道 ===========
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", 
    "382": "山东农科", "383": "山东齐鲁", "384": "山东文旅",
    "sdws": "山东卫视", "sdetv": "山东教育", "sdxw": "山东新闻",
    "sdnk": "山东农科", "sdql": "山东齐鲁", "sdwl": "山东文旅",
    
    # =========== 央视高清频道 ===========
    # CCTV1高清各种变体
    "1": "CCTV1高清", "cctv1": "CCTV1高清", "CCTV1": "CCTV1高清",
    "cctv1hd": "CCTV1高清", "CCTV1HD": "CCTV1高清", "cctv1高清": "CCTV1高清",
    "cctv-1": "CCTV1高清", "CCTV-1": "CCTV1高清", "cctv-1hd": "CCTV1高清",
    "cctv1high": "CCTV1高清", "cctv1.hd": "CCTV1高清", "cctv1.high": "CCTV1高清",
    
    # CCTV2-17高清
    "2": "CCTV2高清", "3": "CCTV3高清", "4": "CCTV4高清", "5": "CCTV5高清",
    "6": "CCTV6高清", "7": "CCTV7高清", "8": "CCTV8高清", "9": "CCTV9高清",
    "10": "CCTV10高清", "11": "CCTV11高清", "12": "CCTV12高清", "13": "CCTV13高清",
    "14": "CCTV14高清", "15": "CCTV15高清", "16": "CCTV16高清", "17": "CCTV17高清",
    
    # =========== 最新4K超高清频道（2024-2025年新增） ===========
    # 央视4K频道
    "101": "CCTV4K", "cctv4k": "CCTV4K", "CCTV4K": "CCTV4K", "cctv-4k": "CCTV4K",
    "cctv4kuhd": "CCTV4K", "4k-cctv": "CCTV4K", "cctv4k超高清": "CCTV4K",
    
    # 央视奥林匹克4K（CCTV16 4K）
    "116": "CCTV16-4K", "cctv164k": "CCTV16-4K", "CCTV164K": "CCTV16-4K",
    "cctv16-4k": "CCTV16-4K", "cctv16.4k": "CCTV16-4K", "央视16套4k": "CCTV16-4K",
    "奥林匹克4k": "CCTV16-4K", "奥运4k": "CCTV16-4K",
    
    # 央视8K试验频道（如果有）
    "cctv8k": "CCTV8K", "CCTV8K": "CCTV8K", "8k-cctv": "CCTV8K", "cctv-8k": "CCTV8K",
    
    # 卫视4K频道（最新补充）
    "102": "浙江卫视4K", "zjws4k": "浙江卫视4K", "浙江卫视4k": "浙江卫视4K", "zj4k": "浙江卫视4K",
    "103": "湖南卫视4K", "hnws4k": "湖南卫视4K", "湖南卫视4k": "湖南卫视4K", "hn4k": "湖南卫视4K",
    "104": "东方卫视4K", "dfws4k": "东方卫视4K", "东方卫视4k": "东方卫视4K", "df4k": "东方卫视4K",
    "105": "北京卫视4K", "bjws4k": "北京卫视4K", "北京卫视4k": "北京卫视4K", "bj4k": "北京卫视4K",
    "106": "广东卫视4K", "gdws4k": "广东卫视4K", "广东卫视4k": "广东卫视4K", "gd4k": "广东卫视4K",
    "107": "深圳卫视4K", "szws4k": "深圳卫视4K", "深圳卫视4k": "深圳卫视4K", "sz4k": "深圳卫视4K",
    "108": "山东卫视4K", "sdws4k": "山东卫视4K", "山东卫视4k": "山东卫视4K", "sd4k": "山东卫视4K",
    
    # 最新新增的其他卫视4K频道
    "109": "江苏卫视4K", "jsws4k": "江苏卫视4K", "江苏卫视4k": "江苏卫视4K", "js4k": "江苏卫视4K",
    "110": "安徽卫视4K", "ahws4k": "安徽卫视4K", "安徽卫视4k": "安徽卫视4K", "ah4k": "安徽卫视4K",
    "111": "四川卫视4K", "scws4k": "四川卫视4K", "四川卫视4k": "四川卫视4K", "sc4k": "四川卫视4K",
    "112": "天津卫视4K", "tjws4k": "天津卫视4K", "天津卫视4k": "天津卫视4K", "tj4k": "天津卫视4K",
    "113": "湖北卫视4K", "hbws4k": "湖北卫视4K", "湖北卫视4k": "湖北卫视4K", "hb4k": "湖北卫视4K",
    
    # =========== 特色4K频道 ===========
    "4k电影": "4K电影", "4k影院": "4K电影", "4kdianying": "4K电影",
    "4k综艺": "4K综艺", "4kzongyi": "4K综艺", "4k娱乐": "4K综艺",
    "4k纪录片": "4K纪录片", "4kjilupian": "4K纪录片", "4k纪实": "4K纪录片",
    "4k体育": "4K体育", "4kty": "4K体育", "4ktiyu": "4K体育",
    "4k少儿": "4K少儿", "4kse": "4K少儿", "4kshaoer": "4K少儿",
    
    # =========== 省卫视高清映射 ===========
    "zjws": "浙江卫视高清", "hnws": "湖南卫视高清", "jsws": "江苏卫视高清",
    "dfws": "东方卫视高清", "bjws": "北京卫视高清", "ahws": "安徽卫视高清",
    "gdws": "广东卫视高清", "henws": "河南卫视高清", "szws": "深圳卫视高清",
    "scws": "四川卫视高清", "cqws": "重庆卫视高清", "tjws": "天津卫视高清",
    "hbws": "湖北卫视高清", "lnws": "辽宁卫视高清", "heilj": "黑龙江卫视高清",
    
    # =========== 央视其他频道 ===========
    "cctv5+": "CCTV5+高清", "cctv5plus": "CCTV5+高清", "cctv5+高清": "CCTV5+高清",
    "cctv4欧洲": "CCTV4欧洲", "cctv4美洲": "CCTV4美洲", "cctv4亚洲": "CCTV4亚洲",
    "cctv戏曲": "CCTV戏曲", "cctv音乐": "CCTV音乐", "cctv高尔夫": "CCTV高尔夫",
    
    # =========== 数字ID备用映射 ===========
    "5001": "CCTV4K", "5002": "浙江卫视4K", "5003": "湖南卫视4K",
    "5004": "东方卫视4K", "5005": "北京卫视4K", "5006": "广东卫视4K",
    "5007": "深圳卫视4K", "5008": "山东卫视4K", "5009": "江苏卫视4K",
    "5010": "安徽卫视4K", "5011": "四川卫视4K", "5012": "天津卫视4K",
}
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.priority_channels = {cat[0]: [] for cat in CHANNEL_PRIORITY}
        self.other_channels: List = []
        self.all_programs: List = []
        # 4K频道专用统计
        self.stats_4k = {
            "found": 0,
            "channels": []
        }
        
    def _create_session(self) -> requests.Session:
        """创建带重试机制的会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT + 2,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def enhanced_id_mapping(self, original_id: str, channel_name: str) -> str:
        """增强型ID映射，专门优化4K频道识别"""
        # 清理ID
        clean_id = original_id.lower().strip()
        clean_name = channel_name.lower()
        
        # 1. 直接映射
        if clean_id in COOL9_ID_MAPPING:
            mapped = COOL9_ID_MAPPING[clean_id]
            if "4k" in mapped.lower() and mapped not in self.stats_4k["channels"]:
                self.stats_4k["found"] += 1
                self.stats_4k["channels"].append(mapped)
            return mapped
        
        # 2. 从名称识别4K频道（即使ID不匹配）
        is_4k_channel = False
        potential_4k_name = None
        
        # 检查名称中的4K关键词
        if "4k" in clean_name or "4K" in channel_name or "uhd" in clean_name or "超高清" in channel_name:
            is_4k_channel = True
            
            # 尝试从名称推断标准频道名
            if "cctv" in clean_name:
                if "16" in clean_name or "奥运" in clean_name or "奥林匹克" in clean_name:
                    potential_4k_name = "CCTV16-4K"
                elif "8k" in clean_name:
                    potential_4k_name = "CCTV8K"
                else:
                    potential_4k_name = "CCTV4K"
            elif "浙江" in channel_name or "zj" in clean_name:
                potential_4k_name = "浙江卫视4K"
            elif "湖南" in channel_name or "hn" in clean_name:
                potential_4k_name = "湖南卫视4K"
            elif "东方" in channel_name or "df" in clean_name:
                potential_4k_name = "东方卫视4K"
            elif "北京" in channel_name or "bj" in clean_name:
                potential_4k_name = "北京卫视4K"
            elif "广东" in channel_name or "gd" in clean_name:
                potential_4k_name = "广东卫视4K"
            elif "深圳" in channel_name or "sz" in clean_name:
                potential_4k_name = "深圳卫视4K"
            elif "山东" in channel_name or "sd" in clean_name:
                potential_4k_name = "山东卫视4K"
            elif "江苏" in channel_name or "js" in clean_name:
                potential_4k_name = "江苏卫视4K"
            elif "安徽" in channel_name or "ah" in clean_name:
                potential_4k_name = "安徽卫视4K"
            elif "四川" in channel_name or "sc" in clean_name:
                potential_4k_name = "四川卫视4K"
            elif "天津" in channel_name or "tj" in clean_name:
                potential_4k_name = "天津卫视4K"
        
        if is_4k_channel and potential_4k_name:
            if potential_4k_name not in self.stats_4k["channels"]:
                self.stats_4k["found"] += 1
                self.stats_4k["channels"].append(potential_4k_name)
            logging.info(f"识别到4K频道: {channel_name} -> {potential_4k_name}")
            return potential_4k_name
        
        # 3. 默认返回原ID
        return original_id

    def read_epg_sources(self) -> List[str]:
        """读取配置文件中的EPG源"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            raise FileNotFoundError(f"找不到配置文件: {CONFIG_FILE}")
            
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                sources = []
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if line.startswith(("http://", "https://")):
                            sources.append(line)
                        else:
                            logging.warning(f"第{line_num}行格式错误，已跳过: {line}")
                
                if len(sources) < 3:
                    logging.warning(f"仅找到{len(sources)}个有效EPG源，建议至少配置3个")
                
                return sources[:8]
                
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容中的无效字符"""
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, any]:
        """并发获取单个EPG源数据"""
        try:
            start_time = time.time()
            logging.info(f"开始抓取: {source}")
            
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            # 处理gzip压缩
            if source.endswith('.gz') or 'gzip' in response.headers.get('content-encoding', ''):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            # 清理XML内容，避免解析失败
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功抓取: {source} | 耗时: {cost_time:.2f}s")
            return True, source, xml_tree
            
        except Exception as e:
            logging.error(f"抓取失败 {source}: {str(e)}")
            return False, source, None

    def process_channels(self, xml_tree, source: str) -> int:
        """处理频道数据，重点识别4K频道"""
        channels = xml_tree.xpath("//channel")
        shandong_count = 0
        
        for channel in channels:
            cid = channel.get("id", "").strip()
            if not cid:
                continue
                
            # 获取频道名称
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else ""
            
            # 应用增强型ID映射（特别优化4K识别）
            original_id = cid
            mapped_id = self.enhanced_id_mapping(cid, channel_name)
            
            if mapped_id in self.channel_ids:
                continue
                
            # 更新频道ID
            channel.set("id", mapped_id)
            self.channel_ids.add(mapped_id)
            
            # 按优先级分类
            channel_added = False
            for cat_name, keywords in CHANNEL_PRIORITY:
                if any(kw in channel_name for kw in keywords):
                    self.priority_channels[cat_name].append(channel)
                    channel_added = True
                    if "山东" in channel_name:
                        shandong_count += 1
                    break
                    
            if not channel_added:
                self.other_channels.append(channel)
                
        return shandong_count

    def process_programs(self, xml_tree):
        """处理节目单数据，确保4K频道节目单对应"""
        programs = xml_tree.xpath("//programme")
        for program in programs:
            channel_id = program.get("channel", "")
            
            # 获取节目名称用于辅助映射
            program_titles = program.xpath(".//title/text()")
            program_name = program_titles[0].strip() if program_titles else ""
            
            # 使用相同的增强映射逻辑
            mapped_id = self.enhanced_id_mapping(channel_id, program_name)
            
            if mapped_id:
                program.set("channel", mapped_id)
                
            self.all_programs.append(program)

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源数据并处理"""
        successful_sources = 0
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {
                executor.submit(self.fetch_single_source, source): source 
                for source in sources
            }
            
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, _, xml_tree = future.result()
                    if success and xml_tree is not None:
                        shandong_count = self.process_channels(xml_tree, source)
                        self.process_programs(xml_tree)
                        successful_sources += 1
                        logging.info(f"处理完成: {source} | 山东频道: {shandong_count}个")
                        
                except Exception as e:
                    logging.error(f"处理源数据失败 {source}: {str(e)}")
        
        return successful_sources > 0

    def generate_final_xml(self) -> str:
        """生成最终的EPG XML文件（按优先级排序）"""
        # 创建XML根节点
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="optimized-epg-generator" 
    generator-info-url="https://github.com/fxq12345/epg" 
    last-update="{time.strftime("%Y%m%d%H%M%S")}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        # 按优先级添加频道（山东本地→央视→其他卫视→其他频道）
        insert_position = 0
        for category, _ in CHANNEL_PRIORITY:
            for channel in self.priority_channels[category]:
                root.insert(insert_position, channel)
                insert_position += 1
                
        # 添加其他频道
        for channel in self.other_channels:
            root.insert(insert_position, channel)
            insert_position += 1
            
        # 添加所有节目单
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_epg_files(self, xml_content: str):
        """保存EPG文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz', '.log')):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except Exception as e:
                    logging.warning(f"删除旧文件失败 {f}: {str(e)}")
        
        # 保存XML文件
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        # 保存GZIP压缩文件
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        
        logging.info(f"EPG文件已保存: {xml_path}, {gz_path}")

    def print_statistics(self):
        """打印详细统计报告，特别关注4K频道"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        logging.info("\n" + "="*60)
        logging.info("📊 EPG生成统计报告（包含最新4K频道）")
        logging.info("="*60)
        
        for category, _ in CHANNEL_PRIORITY:
            count = len(self.priority_channels[category])
            logging.info(f"  {category}: {count}个频道")
            
        other_count = len(self.other_channels)
        logging.info(f"  其他频道: {other_count}个")
        logging.info(f"  总频道数: {total_channels}个")
        logging.info(f"  总节目数: {total_programs}个")
        
        # 4K频道专项统计
        logging.info(f"\n📺 4K频道专项统计:")
        logging.info(f"  发现4K频道数: {self.stats_4k['found']}个")
        if self.stats_4k["channels"]:
            logging.info("  具体4K频道列表:")
            for channel in sorted(self.stats_4k["channels"]):
                logging.info(f"    - {channel}")
        else:
            logging.info("  ⚠️ 未发现4K频道，请检查EPG源")
        
        logging.info("="*60)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("=== EPG生成开始（包含最新4K频道） ===")
        logging.info("注：本版本特别优化了CCTV16-4K、央视8K等最新4K频道的识别")
        
        try:
            sources = self.read_epg_sources()
            logging.info(f"读取到 {len(sources)} 个EPG源")
            
            if not self.fetch_all_sources(sources):
                logging.error("所有EPG源获取失败")
                return False
                
            xml_content = self.generate_final_xml()
            self.save_epg_files(xml_content)
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== 生成完成! 耗时: {total_time:.2f}秒 ===")
            
            # 4K频道检测结果
            if self.stats_4k["found"] > 0:
                logging.info(f"✅ 成功识别到 {self.stats_4k['found']} 个4K频道")
            else:
                logging.warning("⚠️ 未检测到4K频道，可能EPG源中不包含4K频道数据")
                
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}")
            return False

def main():
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
