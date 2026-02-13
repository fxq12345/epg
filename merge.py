# ====================== 修复版本：统一ID为频道名称，解决潍坊台乱码问题 ======================
def merge_all(weifang_gz_file):
    print("🔍 调试：开始 merge_all 函数（已修复ID统一问题）")

    if os.path.exists(weifang_gz_file):
        file_size = os.path.getsize(weifang_gz_file)
        print(f"🔍 调试：潍坊文件存在，大小: {file_size} bytes")
    else:
        print(f"❌ 调试：潍坊文件不存在: {weifang_gz_file}")
        return

    all_channels = []
    all_programs = []
    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        print("❌ 未找到 config.txt 文件")
        empty_output = os.path.join(OUTPUT_DIR, "epg.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(empty_output, "wb") as f:
            f.write(empty_xml)
        print(f"⚠️ 已创建空的EPG文件: {empty_output}")
        return

    with open("config.txt.txt", "r", encoding="utf-8") as f:
        urls = []
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line and line.startswith("http"):
                urls.append(line)
                print(f"🔍 配置第{line_num}行: {line[:60]}...")
            elif line:
                print(f"🔍 配置第{line_num}行(跳过): {line[:60]}...")

    if not urls:
        print("❌ config.txt 中没有找到有效的URL")
        empty_output = os.path.join(OUTPUT_DIR, "epg.gz")
        empty_xml = b'<?xml version="1.0" encoding="utf-8"?>\n<tv></tv>'
        with gzip.open(empty_output, "wb") as f:
            f.write(empty_xml)
        print(f"⚠️ 已创建空的EPG文件: {empty_output}")
        return

    print("=" * 60)
    print(f"🔍 调试：找到 {len(urls)} 个URL")
    print("EPG 源抓取统计（失败自动重试）")
    print("=" * 60)

    xml_trees = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {executor.submit(fetch_with_retry, u): u for u in urls}
        for fut in future_map:
            u = future_map[fut]
            try:
                ok, tree, ch, pg, retry_cnt = fut.result(timeout=30)
                if ok:
                    success_cnt += 1
                    total_ch += ch
                    total_pg += pg
                    log_retry = f"[重试{retry_cnt-1}次]" if retry_cnt > 1 else ""
                    print(f"✅ {u[:55]}... {log_retry}成功 | 频道 {ch:>4} | 节目 {pg:>6}")
                    if tree is not None:
                        xml_trees.append(tree)
                else:
                    fail_cnt += 1
                    print(f"❌ {u[:55]}... 抓取失败")
            except Exception as e:
                fail_cnt += 1
                print(f"❌ {u[:55]}... 执行异常: {e}")

    print(f"🔍 调试：抓取完成，成功 {success_cnt} 个，失败 {fail_cnt} 个")
    print(f"🔍 调试：获取到 {len(xml_trees)} 个XML树")

    # ========== 全局频道映射表：数字ID → 频道名 ==========
    global_channel_map = {}  # key: old_id, value: 频道名称

    # 先收集所有频道
    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            ch_id = ch.get("id", "").strip()
            dn_elem = ch.find("display-name")
            ch_name = dn_elem.text.strip() if (dn_elem is not None and dn_elem.text) else ch_id
            if ch_id and ch_id not in global_channel_map:
                global_channel_map[ch_id] = ch_name

    # ========== 统一频道：用【频道名称】作为唯一ID ==========
    unique_channel_ids = set()

    for tree in xml_trees:
        for ch in tree.findall(".//channel"):
            old_id = ch.get("id", "").strip()
            name = global_channel_map.get(old_id, old_id)
            if name not in unique_channel_ids:
                unique_channel_ids.add(name)
                ch.set("id", name)
                all_channels.append(ch)

    # ========== 统一节目：把 channel="数字" 改成 channel="名称" ==========
    for tree in xml_trees:
        for prog in tree.findall(".//programme"):
            old_ch_id = prog.get("channel", "").strip()
            new_ch_id = global_channel_map.get(old_ch_id, old_ch_id)
            if new_ch_id:
                prog.set("channel", new_ch_id)
            # 简单过滤
            title_elem = prog.find("title")
            if not title_elem or not title_elem.text or len(title_elem.text.strip()) < 2:
                continue
            all_programs.append(prog)

    # ========== 处理潍坊本地源（同样统一ID） ==========
    try:
        print(f"🔍 调试：开始处理潍坊本地源: {weifang_gz_file}")
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))

        # 收集潍坊频道
        wf_channel_map = {}
        for ch in wf_tree.findall(".//channel"):
            ch_id = ch.get("id", "").strip()
            dn_elem = ch.find("display-name")
            ch_name = dn_elem.text.strip() if (dn_elem is not None and dn_elem.text) else ch_id
            wf_channel_map[ch_id] = ch_name
            if ch_name not in unique_channel_ids:
                unique_channel_ids.add(ch_name)
                ch.set("id", ch_name)
                all_channels.append(ch)

        # 潍坊节目也统一ID
        for prog in wf_tree.findall(".//programme"):
            old_ch = prog.get("channel", "").strip()
            new_ch = wf_channel_map.get(old_ch, old_ch)
            if new_ch:
                prog.set("channel", new_ch)
            title_elem = prog.find("title")
            if not title_elem or not title_elem.text or len(title_elem.text.strip()) < 2:
                continue
            all_programs.append(prog)

        print(f"🔍 调试：潍坊源处理完成")
    except Exception as e:
        print(f"⚠️ 潍坊本地源读取失败: {e}")

    # ========== 节目去重 ==========
    print(f"处理前: 频道 {len(all_channels)} 个, 节目 {len(all_programs)} 个")

    if len(all_channels) == 0 and len(all_programs) == 0:
        print("⚠️ 没有数据，生成空文件")
        final_root = etree.Element("tv")
        xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
        output_path = os.path.join(OUTPUT_DIR, "epg.gz")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with gzip.open(output_path, "wb") as f:
            f.write(xml_str)
        return

    unique_programs = []
    seen = set()

    for p in all_programs:
        try:
            ch = p.get("channel", "")
            st = p.get("start", "")
            title = p.find("title").text.strip() if (p.find("title") is not None and p.find("title").text) else ""
            if not ch or not st or not title:
                continue
            key = f"{ch}|{st}|{title}"
            if key not in seen:
                seen.add(key)
                unique_programs.append(p)
        except:
            continue

    unique_programs.sort(key=lambda x: (x.get("channel", ""), x.get("start", "")))

    # ========== 输出最终文件 ==========
    final_root = etree.Element("tv")
    for ch in all_channels:
        final_root.append(ch)
    for p in unique_programs:
        final_root.append(p)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "epg.gz")
    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True, xml_declaration=True)
    with gzip.open(output_path, "wb") as f:
        f.write(xml_str)

    file_size_mb = os.path.getsize(output_path) / 1024 / 1024
    print("=" * 60)
    print(f"✅ 合并完成！频道：{len(all_channels)} ｜ 节目：{len(unique_programs)}")
    print(f"📦 文件：{output_path} ({file_size_mb:.2f}MB)")
    print("🎉 潍坊台 + 网络源 已完全统一格式！")
