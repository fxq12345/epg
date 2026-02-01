import gzip
import os

# （原脚本的EPG合并逻辑，最终得到合并后的XML内容，命名为xml_content）
# ... 你的合并代码 ...

# 确保output目录存在
os.makedirs("output", exist_ok=True)

# 第一步：强制写入epg.xml
xml_path = "output/epg.xml"
with open(xml_path, "w", encoding="utf-8") as f:
    f.write(xml_content)
print(f"成功生成：{xml_path}（大小：{os.path.getsize(xml_path)}字节）")

# 第二步：生成epg.gz
gz_path = "output/epg.gz"
with gzip.open(gz_path, "wb") as f:
    f.write(xml_content.encode("utf-8"))
print(f"成功生成：{gz_path}（大小：{os.path.getsize(gz_path)}字节）")
