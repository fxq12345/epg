import os
import gzip
import re
import time
import logging
from typing import List, Set
from datetime import datetime, timedelta
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"  # 网络源配置文件
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
TIMEOUT = 30
# 本地潍坊EPG文件路径（由weifang_epg_spider.py生成）
LOCAL_WEIFANG_EPG = "weifang_epg.xml"
# 潍坊本地频道配置（用于校验本地EPG）
WEIFANG_CHANNELS = [
    {"id": "1001", "name": "潍坊新闻综合频道", "alias": "潍坊新闻"},
    {"id": "1002", "name": "潍坊经济生活频道", "alias": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共频道", "alias": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化频道", "alias": "潍坊科教文化"},
    {"id": "1008", "name": "寿光蔬菜频道", "alias": "寿光蔬菜"},
    {"id": "1009", "name": "昌乐综合频道", "alias": "昌乐综合"},
    {"id": "1011", "name": "奎文娱乐频道", "alias": "奎文娱乐"}
]
# ==================================================

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EPGMerger:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def _create_session(self) -> requests.Session:
        """创建带重试的会话"""
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        return session

    def _clean_xml(self, content: str) -> str:
        """清理XML非法字符"""
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content).replace('& ', '&amp; ')

    def _read_config(self) -> List[str]:
        """读取config.txt中的网络EPG源（最多5条）"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            return []
        
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                sources = [
                    line.strip() for line in f
                    if line.strip() and not line.startswith("#") and line.startswith(("http://", "https://"))
                ]
            # 限制最多5条网络源
            sources = sources[:5]
            logging.info(f"从{CONFIG_FILE}读取到{len(sources)}条网络EPG源")
            for i, source in enumerate(sources, 1):
                logging.info(f"  {i}. {source[:60]}...")
            return sources
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            return []

    def _fetch_network_epg(self, source: str) -> etree._Element:
        """抓取单条网络EPG源"""
        try:
            logging.info(f"抓取网络源: {source[:60]}...")
            resp = self.session.get(source, timeout=TIMEOUT)
            resp.raise_for_status()
            
            # 处理GZIP压缩
            if source.endswith('.gz'):
                content = gzip.decompress(resp.content).decode('utf-8')
            else:
                content = resp.text
            
            xml_tree = etree.fromstring(self._clean_xml(content).encode('utf-8'))
            logging.info(f"成功抓取网络源: {source[:30]}...")
            return xml_tree
        except Exception as e:
            logging.error(f"抓取网络源失败: {source[:30]}... -> {str(e)[:50]}")
            return etree.Element("tv")  # 返回空节点，不中断流程

    def _process_local_weifang_epg(self) -> etree._Element:
        """读取并处理本地潍坊EPG文件"""
        if not os.path.exists(LOCAL_WEIFANG_EPG):
            logging.warning(f"本地潍坊EPG文件不存在: {LOCAL_WEIFANG_EPG}，跳过合并")
            return etree.Element("tv")
        
        try:
            logging.info(f"开始合并本地潍坊EPG文件: {LOCAL_WEIFANG_EPG}")
            with open(LOCAL_WEIFANG_EPG, "r", encoding="utf-8") as f:
                content = f.read()
            content_clean = self._clean_xml(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            logging.info(f"成功读取本地潍坊EPG文件")
            return xml_tree
        except Exception as e:
            logging.error(f"处理本地潍坊EPG失败: {str(e)}，跳过合并")
            return etree.Element("tv")  # 返回空节点，不中断流程

    def _merge_all_epg(self, xml_trees: List[etree._Element]) -> etree._Element:
        """合并所有EPG源（网络源+本地源）"""
        final_root = etree.Element("tv", 
            generator_info_name="EPG合并器（网络+本地）", 
            last_update=datetime.now().strftime("%Y%m%d%H%M%S")
        )

        # 合并频道和节目
        for tree in xml_trees:
            # 合并频道（去重）
            for channel in tree.xpath("//channel"):
                channel_id = channel.get("id")
                if channel_id and channel_id not in self.channel_ids:
                    self.channel_ids.add(channel_id)
                    final_root.append(channel)
            # 合并节目
            for program in tree.xpath("//programme"):
                final_root.append(program)

        return final_root

    def _save_epg(self, xml_root: etree._Element):
        """保存最终EPG文件"""
        xml_content = etree.tostring(xml_root, encoding="utf-8", pretty_print=True).decode("utf-8")
        
        # 保存XML文件
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        # 保存GZIP压缩文件
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))

        # 统计信息
        total_channels = len(self.channel_ids)
        total_programs = len(xml_root.xpath("//programme"))
        logging.info(f"\n💾 EPG文件保存成功:")
        logging.info(f"  - 总频道数: {total_channels}")
        logging.info(f"  - 总节目数: {total_programs}")
        logging.info(f"  - XML文件: {xml_path}")
        logging.info(f"  - GZIP文件: {gz_path}")

    def run(self):
        """主运行逻辑"""
        start_time = time.time()
        logging.info("🚀 开始EPG合并流程（网络源+本地潍坊源）")

        # 1. 读取网络源配置
        network_sources = self._read_config()
        # 2. 抓取所有网络源
        network_trees = [self._fetch_network_epg(source) for source in network_sources]
        # 3. 读取本地潍坊EPG
        local_tree = self._process_local_weifang_epg()
        # 4. 合并所有源（网络源+本地源）
        all_trees = network_trees + [local_tree]
        final_tree = self._merge_all_epg(all_trees)
        # 5. 保存文件
        self._save_epg(final_tree)

        logging.info(f"\n✅ 合并流程完成，总耗时: {time.time() - start_time:.2f}秒")
        return True

if __name__ == "__main__":
    merger = EPGMerger()
    merger.run()
