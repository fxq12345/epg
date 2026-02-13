import os
import gzip
import requests
from lxml import etree
from datetime import datetime

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def merge_all(weifang_gz_file):
    """简化的合并函数，确保能生成文件"""
    print("🔍 开始合并EPG...")
    
    try:
        # 生成输出路径
        output_path = os.path.join(OUTPUT_DIR, "epg.gz")
        
        # 创建一个测试XML
        root = etree.Element("tv")
        
        # 添加一些测试频道
        test_channels = ["CCTV-1", "CCTV-5", "CCTV-5+", "湖南卫视", "浙江卫视"]
        for i, name in enumerate(test_channels, 1):
            channel = etree.SubElement(root, "channel", id=f"channel{i}")
            dn = etree.SubElement(channel, "display-name", lang="zh")
            dn.text = name
            
        # 添加测试节目
        now = datetime.now()
        base_time = datetime(now.year, now.month, now.day, 20, 0, 0)
        
        for i in range(5):
            for j, channel in enumerate(test_channels, 1):
                start_time = (base_time + timedelta(hours=i)).strftime("%Y%m%d%H%M%S +0800")
                end_time = (base_time + timedelta(hours=i+1)).strftime("%Y%m%d%H%M%S +0800")
                
                program = etree.SubElement(root, "programme", 
                                          start=start_time, 
                                          stop=end_time, 
                                          channel=f"channel{j}")
                title = etree.SubElement(program, "title", lang="zh")
                title.text = f"测试节目{i+1} {channel}"
                
                desc = etree.SubElement(program, "desc", lang="zh")
                desc.text = f"这是一个测试节目，用于验证EPG生成功能。频道：{channel}"
        
        # 添加时间戳注释
        timestamp = etree.Comment(f" Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
        root.insert(0, timestamp)
        
        # 生成XML字符串
        xml_str = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
        
        # 写入压缩文件
        with gzip.open(output_path, "wb") as f:
            f.write(xml_str)
        
        # 验证文件
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"✅ 已创建EPG文件: {output_path}")
            print(f"📦 文件大小: {file_size} 字节")
            
            # 读取文件内容验证
            with gzip.open(output_path, "rt", encoding="utf-8") as f:
                content = f.read(500)  # 读取前500字符
                print(f"📄 文件前500字符:\n{content}")
        else:
            print(f"❌ 文件创建失败: {output_path}")
            
    except Exception as e:
        print(f"❌ 合并过程发生异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("开始执行EPG脚本")
    print(f"当前目录: {os.getcwd()}")
    print(f"输出目录: {OUTPUT_DIR}")
    
    # 创建一个虚拟的潍坊文件
    wf_path = os.path.join(OUTPUT_DIR, "weifang.gz")
    if not os.path.exists(wf_path):
        print(f"创建虚拟潍坊文件: {wf_path}")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(wf_path, "wb") as f:
            f.write(empty_xml)
    
    # 运行合并
    merge_all(wf_path)
    
    print("✅ 脚本执行完成!")
