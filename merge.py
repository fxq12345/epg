# ====================== 工具函数更新 ======================
def clean_program_title(title):
    """清理节目标题 - 简化版本"""
    if not title:
        return ""
    
    # 简单去除多余空格
    title = title.strip()
    
    # 保留常见节目信息，不要过滤太多
    # 只移除明显的广告词
    ad_keywords = ["广告", "报时", "测试"]
    for kw in ad_keywords:
        if kw in title:
            return ""
    
    return title

def get_channel_id_from_display_name(tree, display_name):
    """根据display-name查找对应的channel id"""
    if not display_name or not tree:
        return None
    
    # 在XML树中查找匹配的channel id
    for channel in tree.findall(".//channel"):
        dn = channel.find("display-name")
        if dn is not None and dn.text:
            if display_name.strip() == dn.text.strip():
                return channel.get("id")
    
    return None

# ====================== 改进的merge_all函数 ======================
def merge_all(weifang_gz_file):
    all_channels = []
    all_programs = []
    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        print("❌ 未找到 config.txt 文件")
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    if not urls:
        print("❌ config.txt 中没有找到有效的URL")
        return

    print("=" * 60)
    print("EPG 源抓取统计（失败自动重试）")
    print("=" * 60)

    # 存储所有的XML树用于后续查找
    xml_trees = []
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_map = {executor.submit(fetch_with_retry, u): u for u in urls}
        for fut in future_map:
            u = future_map[fut]
            ok, tree, ch, pg, retry_cnt = fut.result()
            if ok:
                success_cnt += 1
                total_ch += ch
                total_pg += pg
                log_retry = f"[重试{retry_cnt-1}次]" if retry_cnt > 1 else ""
                print(f"✅ {u[:55]}... {log_retry}成功 | 频道 {ch:>4} | 节目 {pg:>6}")
                xml_trees.append(tree)
            else:
                fail_cnt += 1

    # 处理所有源的数据
    for tree in xml_trees:
        # 先添加所有频道
        channels = tree.findall(".//channel")
        for ch in channels:
            # 检查频道是否已存在（通过id比较）
            ch_id = ch.get('id')
            existing = False
            for existing_ch in all_channels:
                if existing_ch.get('id') == ch_id:
                    existing = True
                    break
            
            if not existing:
                all_channels.append(ch)
        
        # 添加所有节目
        programs = tree.findall(".//programme")
        for prog in programs:
            # 确保节目有有效的channel属性
            channel_id = prog.get('channel')
            if not channel_id:
                continue
                
            title_elem = prog.find("title")
            if title_elem is None or not title_elem.text:
                continue
                
            title = title_elem.text.strip()
            if not title or len(title) < 2:
                continue
            
            # 检查节目是否已存在
            start_time = prog.get('start')
            if not start_time:
                continue
                
            # 简单的去重检查：同频道、同开始时间、同标题
            existing_prog = False
            for existing_prog in all_programs:
                if (existing_prog.get('channel') == channel_id and 
                    existing_prog.get('start') == start_time and 
                    existing_prog.find("title").text == title):
                    existing_prog = True
                    break
            
            if not existing_prog:
                all_programs.append(prog)

    print(f"汇总：成功 {success_cnt} 个 | 失败 {fail_cnt} 个 | 总频道 {len(all_channels)} | 总节目 {len(all_programs)}")
    print("=" * 60)

    # 添加潍坊本地源
    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))
            wf_channels = wf_tree.findall(".//channel")
            wf_programs = wf_tree.findall(".//programme")
            
            if wf_channels and wf_programs:
                print(f"📺 潍坊本地源：频道 {len(wf_channels)} | 节目 {len(wf_programs)}")
                
                # 添加潍坊频道
                for ch in wf_channels:
                    ch_id = ch.get('id')
                    existing = False
                    for existing_ch in all_channels:
                        if existing_ch.get('id') == ch_id:
                            existing = True
                            break
                    if not existing:
                        all_channels.append(ch)
                
                # 添加潍坊节目
                for prog in wf_programs:
                    channel_id = prog.get('channel')
                    if not channel_id:
                        continue
                        
                    title_elem = prog.find("title")
                    if title_elem is None or not title_elem.text:
                        continue
                        
                    title = title_elem.text.strip()
                    if not title or len(title) < 2:
                        continue
                    
                    start_time = prog.get('start')
                    if not start_time:
                        continue
                    
                    existing_prog = False
                    for existing_prog in all_programs:
                        if (existing_prog.get('channel') == channel_id and 
                            existing_prog.get('start') == start_time and 
                            existing_prog.find("title").text == title):
                            existing_prog = True
                            break
                    
                    if not existing_prog:
                        all_programs.append(prog)
            else:
                print("⚠️ 潍坊本地源抓取失败，已跳过")
    except Exception as e:
        print(f"⚠️ 潍坊本地源读取失败: {e}")

    print(f"处理前: 频道 {len(all_channels)} 个, 节目 {len(all_programs)} 个")
    
    # ====================== 修复频道对应关系 ======================
    # 创建频道映射：数字ID -> 频道名称
    channel_id_to_name = {}
    channel_name_to_id = {}
    
    for ch in all_channels:
        ch_id = ch.get('id')
        dn = ch.find("display-name")
        if dn is not None and dn.text:
            channel_name = dn.text.strip()
            channel_id_to_name[ch_id] = channel_name
            channel_name_to_id[channel_name] = ch_id
    
    print(f"频道映射表: {len(channel_id_to_name)} 个频道")
    
    # ====================== 改进的节目去重 ======================
    # 更简单的去重策略：同频道、同开始时间、同标题视为重复
    program_dict = {}
    duplicate_count = 0
    
    for prog in all_programs:
        try:
            channel_id = prog.get('channel')
            start_time = prog.get('start')
            title_elem = prog.find("title")
            
            if not channel_id or not start_time or title_elem is None:
                continue
                
            title = title_elem.text.strip() if title_elem.text else ""
            if not title or len(title) < 2:
                continue
            
            # 尝试解析标题，过滤广告节目
            title_lower = title.lower()
            if any(ad in title_lower for ad in ["广告", "报时", "测试", "垫片"]):
                continue
            
            # 创建节目键
            # 使用更精确的去重：频道 + 开始时间 + 标题前20字符
            title_key = title[:30]  # 只取前30个字符比较
            
            # 对于数字channel ID，尝试转换为频道名称
            if channel_id.isdigit():
                # 查找对应的频道名称
                channel_name = channel_id_to_name.get(channel_id)
                if channel_name:
                    program_key = f"{channel_name}|{start_time[:12]}|{title_key}"
                else:
                    program_key = f"{channel_id}|{start_time[:12]}|{title_key}"
            else:
                program_key = f"{channel_id}|{start_time[:12]}|{title_key}"
            
            if program_key in program_dict:
                duplicate_count += 1
                # 保留节目信息更完整的版本
                existing_title_len = len(program_dict[program_key].find("title").text or "")
                if len(title) > existing_title_len:
                    program_dict[program_key] = prog
            else:
                program_dict[program_key] = prog
                
        except Exception as e:
            print(f"⚠️ 处理节目时出错: {e}")
            continue
    
    unique_programs = list(program_dict.values())
    
    print(f"节目去重后: {len(unique_programs)} 个唯一节目")
    print(f"🎯 去重率: {(len(all_programs) - len(unique_programs)) / len(all_programs) * 100:.1f}%")
    print(f"🎯 合并的重复节目数: {duplicate_count}")
    
    # 按频道和开始时间排序节目
    unique_programs.sort(key=lambda x: (
        x.get('channel', ''),
        x.get('start', '')
    ))
    
    # 生成最终XML
    final_root = etree.Element("tv")
    
    # 添加所有频道
    for ch in all_channels:
        final_root.append(ch)
    
    # 添加所有节目
    for p in unique_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    output_path = os.path.join(OUTPUT_DIR, "epg.gz")
    
    with gzip.open(output_path, "wb") as f:
        f.write(xml_str)
    
    # 计算文件大小
    file_size_mb = os.path.getsize(output_path) / 1024 / 1024
    
    # 统计各频道的节目数量
    channel_program_count = {}
    for prog in unique_programs:
        channel_id = prog.get('channel', '')
        channel_program_count[channel_id] = channel_program_count.get(channel_id, 0) + 1
    
    print("=" * 60)
    print(f"✅ 最终输出：频道 {len(all_channels)} 个 | 节目 {len(unique_programs)} 个")
    print(f"📦 文件大小：{file_size_mb:.2f} MB")
    print(f"📁 输出文件：{output_path}")
    
    # 显示前10个频道的节目数量
    print("📊 各频道节目数量（前10）：")
    sorted_channels = sorted(channel_program_count.items(), key=lambda x: x[1], reverse=True)[:10]
    for ch_id, count in sorted_channels:
        ch_name = channel_id_to_name.get(ch_id, ch_id)
        print(f"  {ch_name}: {count} 个节目")
    
    print("=" * 60)
