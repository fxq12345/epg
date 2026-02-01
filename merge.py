import gzip
import os
import requests
from lxml import etree

# 步骤1：定义EPG源列表（从config.txt读取，这里示例）
epg_sources = [
    "https://epg.example.com/epg.xml",
    # 其他源...
]

# 步骤2：抓取并合并EPG内容
merged_root = etree.Element("tv")  # XMLTV根节点
for source in epg_sources:
    try:
        response = requests.get(source, timeout=10)
        response.encoding = "utf-8"
        # 解析单个源的XML
        tree = etree.fromstring(response.text.encode("utf-8"))
        # 合并到根节点
        for child in tree:
            merged_root.append(child)
    except Exception as e:
        print(f"跳过源{source}：{str(e)}")

# 步骤3：生成合并后的XML内容
xml_content = etree.tostring(merged_root, encoding="utf-8", pretty_print=True).decode("utf-8")

# 步骤4：写入文件（强制生成两个文件）
os.makedirs("output", exist_ok=True)

# 写入epg.xml
xml_path = "output/epg.xml"
with open(xml_path, "w", encoding="utf-8") as f:
    f.write(xml_content)
print(f"生成{xml_path}：{os.path.getsize(xml_path)}字节")

# 写入epg.gz
gz_path = "output/epg.gz"
with gzip.open(gz_path, "wb") as f:
    f.write(xml_content.encode("utf-8"))
print(f"生成{gz_path}：{os.path.getsize(gz_path)}字节")
