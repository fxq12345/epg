import os
import sys
import json
import time
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EPGGenerator:
    def __init__(self):
        # 初始化频道数据容器
        self.channel_ids = {}
        self.programs = []

    def load_data(self, data_file):
        """加载频道数据"""
        if not os.path.exists(data_file):
            logger.error(f"数据文件不存在: {data_file}")
            return False
        
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 假设数据格式包含 channel_ids
                self.channel_ids = data.get('channel_ids', {})
                logger.info(f"成功加载 {len(self.channel_ids)} 个频道ID")
                return True
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            return False

    def merge_epg(self, new_programs):
        """
        合并新的节目单数据
        这是报错所在的逻辑区域
        """
        count = 0
        for program in new_programs:
            cid = program.get('channel_id')
            
            # --- 修复点：在这里添加了 'in' ---
            # 原错误代码: if cid not self.channel_ids:
            if cid not in self.channel_ids:
                # 如果频道ID不在我们的列表中，可以选择跳过或记录警告
                logger.debug(f"跳过未知频道: {cid}")
                continue
            
            # 如果频道存在，添加节目
            self.programs.append(program)
            count += 1
            
        logger.info(f"成功合并 {count} 个节目")
        return count

    def save_output(self, output_file):
        """保存最终结果"""
        output_data = {
            'channel_ids': self.channel_ids,
            'programs': self.programs,
            'update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            logger.info(f"EPG 数据已保存至: {output_file}")
            return True
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            return False

def main():
    # 初始化生成器
    generator = EPGGenerator()
    
    # 1. 加载基础频道数据 (模拟路径)
    # 注意：这里路径需要根据你的实际项目结构调整
    data_path = 'data/channels.json' 
    if not generator.load_data(data_path):
        sys.exit(1)

    # 2. 模拟获取新的节目数据 (这里仅作演示，实际应从API或文件读取)
    mock_new_programs = [
        {'channel_id': 'CCTV1', 'title': '新闻联播', 'start': '19:00'},
        {'channel_id': 'UNKNOWN_CHANNEL', 'title': '测试节目', 'start': '20:00'} 
    ]
    
    # 3. 执行合并
    generator.merge_epg(mock_new_programs)
    
    # 4. 保存结果
    generator.save_output('output/epg_data.json')

if __name__ == "__main__":
    main()
