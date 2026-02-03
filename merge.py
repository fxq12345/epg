import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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

# 核心频道配置
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 扩展版酷9专用ID映射表
COOL9_ID_MAPPING = {
    # 山东本地频道
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", "382": "山东农科",
    "383": "山东齐鲁", "384": "山东文旅", "sdws": "山东卫视", "sdetv": "山东教育",
    "sdxw": "山东新闻", "sdnk": "山东农科", "sdql": "山东齐鲁", "sdwl": "山东文旅",
    
    # 央视频道
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4", "5": "CCTV5",
    "6": "CCTV6", "7": "CCTV7", "8": "CCTV8", "9": "CCTV9", "10": "CCTV10",
    "11": "CCTV11", "12": "CCTV12", "13": "CCTV13", "14": "CCTV14", "15": "CCTV15",
    "16": "CCTV16", "17": "CCTV17", "cctv1": "CCTV1", "cctv2": "CCTV2", "cctv13": "CCTV13",
    "cctv4k": "CCTV4K", "cctv5plus": "CCTV5+",
    
    # 4K超高清频道
    "101": "CCTV4K", "102": "浙江卫视4K", "103": "湖南卫视4K", "104": "东方卫视4K",
    "105": "北京卫视4K", "106": "广东卫视4K", "107": "深圳卫视4K", "108": "山东卫视4K",
    
    # 常见省卫视（名称标准化）
    "zjws": "浙江卫视", "hnws": "湖南卫视", "jsws": "江苏卫视", "dfws": "东方卫视",
    "bjws": "北京卫视", "ahws": "安徽卫视", "gdws": "广东卫视", "henws": "河南卫视",
    "scws": "四川卫视", "cqws": "重庆卫视", "tjws": "天津卫视", "hbws": "湖北卫视",
    
    # 地方频道
    "gzpd": "广州综合", "szse": "深圳卫视", "nmws": "内蒙古卫视", "xzws": "西藏卫视"
}

# 扩展国内频道关键词
DOMESTIC_KEYWORDS = [
    "山东", "CCTV", "卫视", "央视", "中国", "东方", "浙江", "湖南", "江苏", "北京",
    "安徽", "广东", "河南", "深圳", "四川", "重庆", "天津", "湖北", "江西", "河北",
    "山西", "陕西", "甘肃", "青海", "宁夏", "新疆", "内蒙古", "辽宁", "吉林", "黑龙江",
    "上海", "福建", "广西", "海南", "贵州", "云南", "西藏", "香港", "澳门", "台湾",
    "农林", "教育", "新闻", "公共", "都市", "经济", "生活", "影视", "体育", "卡通"
]

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.priority_channels = {cat[0]: [] for cat in CHANNEL_PRIORITY}
        self.other_channels: List = []
        self.all_programs: List = []
        self.mapping_stats = {"total": 0, "mapped": 0, "filtered": 0}
        
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
        """
        增强型ID映射，支持多种匹配方式
        返回映射后的ID，如无映射则返回原ID
        """
        self.mapping_stats["total"] += 1
        
        # 1. 直接数字ID映射
        if original_id in COOL9_ID_MAPPING:
            self.mapping_stats["mapped"] += 1
            return COOL9_ID_MAPPING[original_id]
        
        # 2. 名称关键词映射（应对ID格式不一致的情况）
        for key_id, name_pattern in COOL9_ID_MAPPING.items():
            if name_pattern in channel_name:
                self.mapping_stats["mapped"] += 1
                logging.debug(f"名称映射: {original_id}({channel_name}) -> {name_pattern}")
                return name_pattern
        
        # 3. 处理带后缀的ID（如"CCTV1.hd" -> "CCTV1"）
        clean_id = original_id.split('.')[0].split('_')[0].split('-')[0]
        if clean_id in COOL9_ID_MAPPING:
            self.mapping_stats["mapped"] += 1
            logging.debug(f"后缀清理映射: {original_id} -> {clean_id} -> {COOL9_ID_MAPPING[clean_id]}")
            return COOL9_ID_MAPPING[clean_id]
        
        # 4. 尝试标准化名称匹配（处理大小写不一致）
        clean_name = channel_name.upper().replace("高清", "").replace("HD", "").strip()
        for key_id, name_pattern in COOL9_ID_MAPPING.items():
            if name_pattern.upper() in clean_name:
                self.mapping_stats["mapped"] += 1
                logging.debug(f"标准化映射: {original_id}({channel_name}) -> {name_pattern}")
                return name_pattern
        
        return original_id

    def is_domestic_channel(self, channel_name: str, channel_id: str) -> bool:
        """判断是否为国内频道，放宽过滤条件"""
        # 如果已在映射表中，自动视为国内频道
        if any(mapped_name in channel_id for mapped_name in COOL9_ID_MAPPING.values()):
            return True
            
        # 检查是否包含国内关键词
        return any(kw in channel_name for kw in DOMESTIC_KEYWORDS)

    def categorize_channel(self, channel, channel_name: str, channel_id: str):
        """频道分类逻辑"""
        channel_added = False
        for cat_name, keywords in CHANNEL_PRIORITY:
            if any(kw in channel_name for kw in keywords):
                self.priority_channels[cat_name].append(channel)
                channel_added = True
                logging.debug(f"频道分类: {channel_name} -> {cat_name}")
                break
                
        if not channel_added:
            self.other_channels.append(channel)
            logging.debug(f"频道分类: {channel_name} -> 其他频道")

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
                
                return sources[:8]  # 限制最大源数量，避免过度抓取
                
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容中的无效字符，避免解析报错"""
        # 移除控制字符和非XML标准字符
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        # 修复常见的XML转义问题
        content_clean = content_clean.replace('& ', '&amp; ')
        content_clean = re.sub(r'&(?!(amp|lt|gt|quot|apos);)', '&amp;', content_clean)
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
        """处理频道数据，含分类、过滤、统计 - 增强版"""
        channels = xml_tree.xpath("//channel")
        shandong_count = 0
        source_mapped_count = 0
        
        for channel in channels:
            cid = channel.get("id", "").strip()
            if not cid:
                continue
                
            # 获取频道名称
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else ""
            
            # 应用增强型ID映射
            original_id = cid
            mapped_id = self.enhanced_id_mapping(cid, channel_name)
            
            if mapped_id != original_id:
                source_mapped_count += 1
                logging.debug(f"频道ID映射: {original_id} -> {mapped_id} ({channel_name})")
                
            if mapped_id in self.channel_ids:
                continue  # 跳过重复频道
                
            # 过滤国外频道（放宽条件）
            if not self.is_domestic_channel(channel_name, mapped_id):
                self.mapping_stats["filtered"] += 1
                logging.debug(f"频道过滤: {channel_name}({mapped_id}) - 不符合国内频道条件")
                continue
                
            # 更新频道ID（统一格式）
            channel.set("id", mapped_id)
            self.channel_ids.add(mapped_id)
            
            # 按优先级分类
            self.categorize_channel(channel, channel_name, mapped_id)
            if "山东" in channel_name:
                shandong_count += 1  # 统计山东本地频道
        
        if source_mapped_count > 0:
            logging.info(f"频道映射统计: 源{source} 总数{len(channels)} 映射{source_mapped_count}个")
            
        return shandong_count

    def process_programs(self, xml_tree):
        """处理节目单数据，增强ID映射一致性"""
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
                logging.debug(f"节目单映射: {channel_id} -> {mapped_id} ({program_name})")
                
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
                
        # 添加其他国内频道
        for channel in self.other_channels:
            root.insert(insert_position, channel)
            insert_position += 1
            
        # 添加所有节目单
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_epg_files(self, xml_content: str):
        """保存EPG文件（XML+GZIP），清理旧文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件，避免占用空间
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
        
        # 保存GZIP压缩文件（节省空间，机顶盒支持自动解压）
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        gz_size = os.path.getsize(gz_path)
        
        logging.info(f"EPG文件生成完成: XML={xml_size}字节, GZIP={gz_size}字节")

    def print_statistics(self):
        """打印详细统计报告，方便核对"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        logging.info("\n" + "="*60)
        logging.info("📊 EPG生成统计报告（酷9优化版）")
        logging.info("="*60)
        
        for category, _ in CHANNEL_PRIORITY:
            count = len(self.priority_channels[category])
            logging.info(f"  {category}: {count}个频道")
            
        other_count = len(self.other_channels)
        logging.info(f"  其他国内频道: {other_count}个")
        logging.info(f"  总频道数: {total_channels}个")
        logging.info(f"  总节目数: {total_programs}个")
        
        # 映射统计
        logging.info(f"  频道ID处理: {self.mapping_stats['total']}个")
        logging.info(f"  成功映射: {self.mapping_stats['mapped']}个")
        logging.info(f"  过滤排除: {self.mapping_stats['filtered']}个")
        logging.info("="*60)

    def run(self):
        """主运行方法，统一调度所有流程"""
        start_time = time.time()
        logging.info("=== EPG生成开始（酷9优化版） ===")
        
        try:
            # 读取配置文件中的EPG源
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个EPG源")
            
            # 并发获取并处理所有源数据
            if not self.fetch_all_sources(sources):
                logging.error("所有EPG源获取失败，程序退出")
                return False
                
            # 生成最终的XML内容
            xml_content = self.generate_final_xml()
            
            # 保存文件（XML+GZIP）
            self.save_epg_files(xml_content)
            
            # 输出统计报告
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== EPG生成完成! 总耗时: {total_time:.2f}秒 ===")
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}")
            return False

def main():
    """主函数，程序入口"""
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
