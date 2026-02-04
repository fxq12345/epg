import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple
from datetime import datetime
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 独立配置区 =====================
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
TIMEOUT = 30
# 潍坊本地EPG抓取接口（集成到merge脚本中）
WEIFANG_CHANNELS = [
    {"id": "1001", "name": "潍坊新闻综合频道", "alias": "潍坊新闻"},
    {"id": "1002", "name": "潍坊经济生活频道", "alias": "潍坊经济生活"},
    {"id": "1003", "name": "潍坊公共频道", "alias": "潍坊公共"},
    {"id": "1004", "name": "潍坊科教文化频道", "alias": "潍坊科教文化"},
    {"id": "1008", "name": "寿光蔬菜频道", "alias": "寿光蔬菜"},
    {"id": "1009", "name": "昌乐综合频道", "alias": "昌乐综合"},
    {"id": "1011", "name": "奎文娱乐频道", "alias": "奎文娱乐"}
]
# 外部EPG源（可直接写在脚本内，无需config.txt）
EXTERNAL_EPG_SOURCES = [
    # 示例：添加你需要的外部EPG源
    "https://example.com/epg.xml"
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

class IndependentEPGMerger:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.channel_programs: Dict[str, List] = {}
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def _create_session(self) -> requests.Session:
        """创建带重试的会话"""
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
        return session

    def _clean_xml(self, content: str) -> str:
        """清理XML非法字符"""
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content).replace('& ', '&amp; ')

    # ========== 新增：集成潍坊本地EPG抓取 ==========
    def _fetch_weifang_epg(self) -> etree._Element:
        """直接抓取潍坊本地EPG并生成XML树"""
        logging.info("开始抓取潍坊本地EPG...")
        programmes = []
        headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"}

        # 创建根节点
        root = etree.Element("tv")

        # 添加潍坊频道
        for channel in WEIFANG_CHANNELS:
            channel_elem = etree.SubElement(root, "channel")
            channel_elem.set("id", channel["id"])
            etree.SubElement(channel_elem, "display-name", lang="zh-CN").text = channel["name"]
            etree.SubElement(channel_elem, "display-name", lang="zh-CN").text = channel["alias"]

        # 抓取3天节目
        for day_offset in range(3):
            target_date = (datetime.today() + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")
            for channel in WEIFANG_CHANNELS:
                try:
                    url = f"https://sd.iqilu.com/api/tv/program?channel={channel['alias']}&date={target_date}"
                    resp = self.session.get(url, headers=headers, timeout=TIMEOUT)
                    resp.raise_for_status()
                    data = resp.json()

                    for prog in data.get("data", []):
                        # 转换时间格式
                        start = f"{prog['start_time'].replace('-', '').replace(':', '')} +0800"
                        stop = f"{prog['end_time'].replace('-', '').replace(':', '')} +0800"
                        
                        # 创建节目节点
                        prog_elem = etree.SubElement(root, "programme", channel=channel["id"], start=start, stop=stop)
                        etree.SubElement(prog_elem, "title", lang="zh-CN").text = prog["program_name"]
                        if prog.get("program_desc"):
                            etree.SubElement(prog_elem, "desc", lang="zh-CN").text = prog["program_desc"]

                except Exception as e:
                    logging.warning(f"抓取{channel['name']}节目失败: {str(e)}")

        logging.info("潍坊本地EPG抓取完成")
        return root
    # ==============================================

    def _fetch_external_epg(self, source: str) -> etree._Element:
        """抓取外部EPG源"""
        try:
            resp = self.session.get(source, timeout=TIMEOUT)
            resp.raise_for_status()
            content = gzip.decompress(resp.content).decode('utf-8') if source.endswith('.gz') else resp.text
            return etree.fromstring(self._clean_xml(content).encode('utf-8'))
        except Exception as e:
            logging.error(f"外部EPG源{source}抓取失败: {str(e)}")
            return etree.Element("tv")  # 返回空节点

    def _merge_epg(self, xml_trees: List[etree._Element]):
        """合并所有EPG数据"""
        # 创建最终根节点
        final_root = etree.Element("tv", generator_info_name="独立EPG合并器", last_update=datetime.now().strftime("%Y%m%d%H%M%S"))

        # 合并频道和节目
        for tree in xml_trees:
            # 合并频道
            for channel in tree.xpath("//channel"):
                channel_id = channel.get("id")
                if channel_id not in self.channel_ids:
                    self.channel_ids.add(channel_id)
                    final_root.append(channel)
            
            # 合并节目
            for program in tree.xpath("//programme"):
                final_root.append(program)

        return final_root

    def _save_epg(self, xml_root: etree._Element):
        """保存XML和GZIP文件"""
        xml_content = etree.tostring(xml_root, encoding="utf-8", pretty_print=True).decode("utf-8")
        
        # 保存XML
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        # 保存GZIP
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))

        logging.info(f"EPG文件已保存至{OUTPUT_DIR}")

    def run(self):
        """主运行逻辑"""
        start_time = time.time()
        logging.info("独立EPG合并开始")

        # 1. 抓取潍坊本地EPG
        weifang_tree = self._fetch_weifang_epg()

        # 2. 抓取外部EPG源
        external_trees = [self._fetch_external_epg(source) for source in EXTERNAL_EPG_SOURCES]

        # 3. 合并所有EPG
        all_trees = [weifang_tree] + external_trees
        final_tree = self._merge_epg(all_trees)

        # 4. 保存文件
        self._save_epg(final_tree)

        logging.info(f"合并完成，耗时{time.time() - start_time:.2f}秒")
        return True

if __name__ == "__main__":
    merger = IndependentEPGMerger()
    merger.run()
