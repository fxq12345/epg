def merge_all(weifang_gz_file):
    print("🔍 开始合并EPG...")
    
    # 确保有输出
    output_path = os.path.join(OUTPUT_DIR, "epg.gz")
    
    # 创建一个简单的XML
    root = etree.Element("tv")
    
    # 添加一个测试频道
    channel = etree.SubElement(root, "channel", id="test")
    dn = etree.SubElement(channel, "display-name", lang="zh")
    dn.text = "测试频道"
    
    # 添加一个测试节目
    program = etree.SubElement(root, "programme", 
                              start="20250213000000 +0800", 
                              stop="20250213010000 +0800", 
                              channel="test")
    title = etree.SubElement(program, "title", lang="zh")
    title.text = "测试节目 - 确保文件有变更"
    
    xml_str = etree.tostring(root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    
    with gzip.open(output_path, "wb") as f:
        f.write(xml_str)
    
    print(f"✅ 已创建测试EPG文件: {output_path}")
    print(f"📦 文件大小: {os.path.getsize(output_path)} bytes")
