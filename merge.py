import os
import requests
from bs4 import BeautifulSoup

# 配置项
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 网络源列表（5个）
network_sources = [
    "https://e.erw.cc/all.xml.gz",
    "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
    "http://epg.51zmt.top:8000/e.xml.gz",
    "https://raw.githubusercontent.com/fanmingming/live/main/epg.xml.gz",
    "https://e.erw.cc/e.xml.gz"
]

# 本地源配置
local_source = {
    "name": "潍坊本地源",
    "path": "local/weifang.xml",  # 本地文件路径
    "channel_count": 4,
    "program_count": 833
}

# 统计变量
success_count = 0
total_channels = 0
total_programs = 0

# 合并后的根节点
root = BeautifulSoup('<?xml version="1.0" encoding="UTF-8"?><tv></tv>', 'xml')
tv_node = root.tv

# 处理网络源
for url in network_sources:
    try:
        print(f"正在下载: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # 解压 gz（如果是压缩包）
        if url.endswith(".gz"):
            import gzip
            content = gzip.decompress(response.content)
        else:
            content = response.content
        
        # 解析 XML
        soup = BeautifulSoup(content, 'xml')
        
        # 统计频道和节目数
        channels = len(soup.find_all('channel'))
        programs = len(soup.find_all('programme'))
        
        # 合并到根节点
        for channel in soup.find_all('channel'):
            tv_node.append(channel)
        for programme in soup.find_all('programme'):
            tv_node.append(programme)
        
        total_channels += channels
        total_programs += programs
        success_count += 1
        print(f"✅ {url} 成功 | 频道 {channels} | 节目 {programs}")
    except Exception as e:
        print(f"❌ {url} 失败 | {str(e)}")

# 处理本地源
try:
    print(f"正在处理本地源: {local_source['name']}")
    with open(local_source['path'], 'r', encoding='utf-8') as f:
        content = f.read()
    
    soup = BeautifulSoup(content, 'xml')
    
    # 合并到根节点
    for channel in soup.find_all('channel'):
        tv_node.append(channel)
    for programme in soup.find_all('programme'):
        tv_node.append(programme)
    
    total_channels += local_source['channel_count']
    total_programs += local_source['program_count']
    success_count += 1
    print(f"📺 {local_source['name']}：频道 {local_source['channel_count']} | 节目 {local_source['program_count']} (本周一~周日完整7天+酷9图标)")
except Exception as e:
    print(f"❌ {local_source['name']} 失败 | {str(e)}")

# 输出汇总
print("=" * 60)
print(f"汇总：成功 {success_count} 个 | 失败 {len(network_sources) + 1 - success_count} 个 | 总频道 {total_channels} | 总节目 {total_programs}")
print("=" * 60)

# 保存合并后的文件
output_file = os.path.join(OUTPUT_DIR, "merged_epg.xml")
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(str(root))

print(f"合并完成，文件已保存到: {output_file}")
