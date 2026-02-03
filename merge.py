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
MAX_WORKERS = 3  # 并发线程数
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

# 核心频道配置（只影响排序，不影响显示）
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 酷9专用ID映射表（优化版，包含更多频道格式）
COOL9_ID_MAPPING = {
    # 山东本地频道
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", 
    "382": "山东农科", "383": "山东齐鲁", "384": "山东文旅",
    "sdws": "山东卫视", "sdetv": "山东教育", "sdxw": "山东新闻",
    "sdnk": "山东农科", "sdql": "山东齐鲁", "sdwl": "山东文旅",
    
    # 央视频道（高清版）
    "1": "CCTV1高清", "2": "CCTV2高清", "3": "CCTV3高清", "4": "CCTV4高清",
    "5": "CCTV5高清", "6": "CCTV6高清", "7": "CCTV7高清", "8": "CCTV8高清",
    "9": "CCTV9高清", "10": "CCTV10高清", "11": "CCTV11高清", "12": "CCTV12高清",
    "13": "CCTV13高清", "14": "CCTV14高清", "15": "CCTV15高清", "16": "CCTV16高清",
    "17": "CCTV17高清",
    
    # 央视频道（标准版）
    "cctv1": "CCTV1", "cctv2": "CCTV2", "cctv3": "CCTV3", "cctv4": "CCTV4",
    "cctv5": "CCTV5", "cctv6": "CCTV6", "cctv7": "CCTV7", "cctv8": "CCTV8",
    "cctv9": "CCTV9", "cctv10": "CCTV10", "cctv11": "CCTV11", "cctv12": "CCTV12",
    "cctv13": "CCTV13", "cctv14": "CCTV14", "cctv15": "CCTV15", "cctv16": "CCTV16",
    "cctv17": "CCTV17",
    
    # 4K超高清频道
    "101": "CCTV4K", "102": "浙江卫视4K", "103": "湖南卫视4K",
    "104": "东方卫视4K", "105": "北京卫视4K", "106": "广东卫视4K",
    "107": "深圳卫视4K", "108": "山东卫视4K", "109": "江苏卫视4K",
    "110": "安徽卫视4K", "111": "四川卫视4K", "112": "天津卫视4K",
    "113": "湖北卫视4K", "114": "重庆卫视4K", "115": "辽宁卫视4K",
    
    # 省卫视高清映射
    "zjws": "浙江卫视高清", "hnws": "湖南卫视高清", "jsws": "江苏卫视高清",
    "dfws": "东方卫视高清", "bjws": "北京卫视高清", "ahws": "安徽卫视高清",
    "gdws": "广东卫视高清", "henws": "河南卫视高清", "szws": "深圳卫视高清",
    "scws": "四川卫视高清", "cqws": "重庆卫视高清", "tjws": "天津卫视高清",
    "hbws": "湖北卫视高清", "lnws": "辽宁卫视高清",
    
    # 央视特殊频道
    "cctv5+": "CCTV5+高清", "cctv5plus": "CCTV5+高清", "cctv5+高清": "CCTV5+高清",
    "cctv4欧洲": "CCTV4欧洲", "cctv4美洲": "CCTV4美洲", "cctv4亚洲": "CCTV4亚洲",
    "cctv戏曲": "CCTV戏曲", "cctv音乐": "CCTV音乐", "cctv高尔夫": "CCTV高尔夫",
}

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.priority_channels = {cat[0]: [] for cat in CHANNEL_PRIORITY}
        self.other_channels: List = []
        self.all_programs: List = []
        self.stats = {
            "total_channels": 0,
            "processed_channels": 0,
            "mapped_channels": 0,
            "cctv1_found": False,
            "cctv1_original_id": None
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
        """增强型ID映射，支持多种格式"""
        self.stats["total_channels"] += 1
        
        # 1. 直接映射
        if original_id in COOL9_ID_MAPPING:
            self.stats["mapped_channels"] += 1
            mapped_id = COOL9_ID_MAPPING[original_id]
            if "cctv1" in mapped_id.lower():
                self.stats["cctv1_found"] = True
                self.stats["cctv1_original_id"] = original_id
                logging.info(f"✅ 发现CCTV1频道: 源ID={original_id}, 名称={channel_name}, 映射为={mapped_id}")
            return mapped_id
        
        # 2. 清理常见后缀后映射
        clean_id = original_id.split('.')[0].split('_')[0].split('-')[0].strip()
        if clean_id in COOL9_ID_MAPPING:
            self.stats["mapped_channels"] += 1
            mapped_id = COOL9_ID_MAPPING[clean_id]
            if "cctv1" in mapped_id.lower():
                self.stats["cctv1_found"] = True
                self.stats["cctv1_original_id"] = original_id
                logging.info(f"✅ 发现CCTV1频道(清理后): 源ID={original_id}->{clean_id}, 名称={channel_name}, 映射为={mapped_id}")
            return mapped_id
        
        # 3. 从名称识别CCTV1高清
        clean_name = channel_name.lower()
        if ("cctv1" in clean_name or "央视1" in clean_name or "中央1" in clean_name) and ("高清" in channel_name or "hd" in clean_name):
            self.stats["mapped_channels"] += 1
            self.stats["cctv1_found"] = True
            self.stats["cctv1_original_id"] = original_id
            logging.info(f"✅ 从名称识别CCTV1高清: 源ID={original_id}, 名称={channel_name}")
            return "CCTV1高清"
        
        # 4. 保持原ID
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
        """获取单个EPG源数据"""
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
                
            # 清理XML内容
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功抓取: {source} | 耗时: {cost_time:.2f}s")
            return True, source, xml_tree
            
        except Exception as e:
            logging.error(f"抓取失败 {source}: {str(e)}")
            return False, source, None

    def process_channels(self, xml_tree, source: str) -> int:
        """处理频道数据 - 已移除所有过滤"""
        channels = xml_tree.xpath("//channel")
        shandong_count = 0
        
        for channel in channels:
            cid = channel.get("id", "").strip()
            if not cid:
                continue
                
            # 获取频道名称
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else ""
            
            # 应用智能ID映射
            original_id = cid
            mapped_id = self.enhanced_id_mapping(cid, channel_name)
            
            # 记录处理过的频道
            self.stats["processed_channels"] += 1
            
            if mapped_id in self.channel_ids:
                logging.debug(f"跳过重复频道: {channel_name} ({mapped_id})")
                continue
                
            # 更新频道ID（统一格式）
            channel.set("id", mapped_id)
            self.channel_ids.add(mapped_id)
            
            # 按优先级分类（只影响排序，不影响显示）
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
                
        logging.info(f"源处理完成: {source} | 频道: {len(channels)}个 | 山东频道: {shandong_count}个")
        return shandong_count

    def process_programs(self, xml_tree):
        """处理节目单数据，确保与频道ID一致"""
        programs = xml_tree.xpath("//programme")
        for program in programs:
            channel_id = program.get("channel", "")
            
            # 获取节目名称用于辅助映射
            program_titles = program.xpath(".//title/text()")
            program_name = program_titles[0].strip() if program_titles else ""
            
            # 使用相同的增强映射逻辑
            mapped_id = self.enhanced_id_mapping(channel_id, program_name)
            
            if mapped_id and mapped_id != channel_id:
                program.set("channel", mapped_id)
                
            self.all_programs.append(program)

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源数据"""
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
                        self.process_channels(xml_tree, source)
                        self.process_programs(xml_tree)
                        successful_sources += 1
                        
                except Exception as e:
                    logging.error(f"处理源数据失败 {source}: {str(e)}")
        
        return successful_sources > 0

    def generate_final_xml(self) -> str:
        """生成最终的EPG XML文件"""
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="optimized-epg-generator" 
    generator-info-url="https://github.com/fxq12345/epg" 
    last-update="{time.strftime("%Y%m%d%H%M%S")}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        # 按优先级添加频道（只影响排序）
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
        xml_size = os.path.getsize(xml_path)
        
        # 保存GZIP压缩文件
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        gz_size = os.path.getsize(gz_path)
        
        logging.info(f"EPG文件生成完成: XML={xml_size}字节, GZIP={gz_size}字节")

    def print_statistics(self):
        """打印详细统计报告"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        logging.info("\n" + "="*60)
        logging.info("📊 EPG生成统计报告（无过滤优化版）")
        logging.info("="*60)
        logging.info(f"总计处理频道: {self.stats['total_channels']}个")
        logging.info(f"成功映射频道: {self.stats['mapped_channels']}个")
        logging.info(f"最终保留频道: {total_channels}个")
        logging.info(f"总节目单数: {total_programs}个")
        
        # CCTV1检测结果
        if self.stats["cctv1_found"]:
            logging.info(f"✅ CCTV1高清: 已找到 (源ID: {self.stats['cctv1_original_id']})")
        else:
            logging.warning("⚠️ CCTV1高清: 未找到，可能EPG源中无此频道")
        
        logging.info("\n📁 频道分类（仅排序）:")
        for category, _ in CHANNEL_PRIORITY:
            count = len(self.priority_channels[category])
            logging.info(f"  {category}: {count}个")
        logging.info(f"  其他频道: {len(self.other_channels)}个")
        logging.info("="*60)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("=== EPG生成开始（无过滤优化版） ===")
        logging.info("说明: 已移除所有过滤逻辑，确保所有频道都能显示")
        logging.info("     频道分类仅影响排序，不影响显示")
        
        try:
            # 读取EPG源
            sources = self.read_epg_sources()
            logging.info(f"读取到 {len(sources)} 个EPG源")
            
            # 获取并处理所有源数据
            if not self.fetch_all_sources(sources):
                logging.error("所有EPG源获取失败，程序退出")
                return False
                
            # 生成最终XML
            xml_content = self.generate_final_xml()
            
            # 保存文件
            self.save_epg_files(xml_content)
            
            # 输出统计报告
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== 生成完成! 总耗时: {total_time:.2f}秒 ===")
            
            # 最终检查
            if self.stats["processed_channels"] > 0:
                logging.info(f"✅ 成功处理 {self.stats['processed_channels']} 个频道")
            else:
                logging.warning("⚠️ 未处理任何频道，请检查config.txt配置")
                
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}", exc_info=True)
            return False

def main():
    """主函数"""
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
