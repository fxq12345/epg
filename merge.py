def merge_all(weifang_gz_file):
    all_channels = []
    all_programs = []
    total_ch = 0
    total_pg = 0
    success_cnt = 0
    fail_cnt = 0

    if not os.path.exists("config.txt"):
        return

    with open("config.txt", "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip() and l.startswith("http")]

    print("=" * 60)
    print("EPG 源抓取统计（失败自动重试）")
    print("=" * 60)

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
                for node in tree:
                    if node.tag == "channel":
                        all_channels.append(node)
                    elif node.tag == "programme":
                        all_programs.append(node)
            else:
                fail_cnt += 1

    if fail_cnt > 0:
        print(f"❌ 共 {fail_cnt} 个源经{MAX_RETRY}次重试后仍失败，已跳过")

    print("=" * 60)
    print(f"汇总：成功 {success_cnt} 个 | 失败 {fail_cnt} 个 | 总频道 {total_ch} | 总节目 {total_pg}")
    print("=" * 60)

    try:
        with gzip.open(weifang_gz_file, "rb") as f:
            wf_content = f.read().decode("utf-8")
            wf_tree = etree.fromstring(wf_content.encode("utf-8"))
            wf_ch = len(wf_tree.xpath("//channel"))
            wf_pg = len(wf_tree.xpath("//programme"))

        if wf_ch > 0 and wf_pg > 0:
            print(f"📺 潍坊本地源：频道 {wf_ch} | 节目 {wf_pg}（本周一~周日完整7天+酷9图标）")
            for node in wf_tree:
                if node.tag == "channel":
                    all_channels.append(node)
                elif node.tag == "programme":
                    all_programs.append(node)
        else:
            print("⚠️ 潍坊本地源抓取失败，已跳过")
    except:
        print("⚠️ 潍坊本地源读取失败，已跳过")

    # ====================== 修改：名称相同，无论ID，只保留第一个 ======================
    seen_channel_names = set()
    unique_channels = []
    for ch in all_channels:
        display_name_node = ch.find("display-name")
        if display_name_node and display_name_node.text:
            channel_name = display_name_node.text.strip()
            # 只要名称没出现过，就保留；出现过就跳过，不管ID是否相同
            if channel_name not in seen_channel_names:
                seen_channel_names.add(channel_name)
                unique_channels.append(ch)
        else:
            # 没有display-name的频道直接保留
            unique_channels.append(ch)

    # 生成最终XML（用去重后的频道 + 所有节目）
    final_root = etree.Element("tv")
    for ch in unique_channels:
        final_root.append(ch)
    for p in all_programs:
        final_root.append(p)

    xml_str = etree.tostring(final_root, encoding="utf-8", pretty_print=True)
    with gzip.open(os.path.join(OUTPUT_DIR, "epg.gz"), "wb") as f:
        f.write(xml_str)

    print(f"✅ 最终输出：频道 {len(unique_channels)} 个 | 节目 {len(all_programs)} 个")
