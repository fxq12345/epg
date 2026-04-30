# ==================== 解析 JSON（dy2.fun 百川源专属修复版） ====================
def parse_json(content, index):
    channels = {}
    programs = []
    try:
        data = json.loads(content.decode('utf-8', 'ignore'))
        now_dt = datetime.now()
        current_base_day = today

        for item in data:
            tvid = item.get("tvid") or item.get("id")
            name = item.get("name")
            plist = item.get("list", [])
            if not tvid or not name or not plist:
                continue

            # 纯净频道ID，适配播放器匹配
            cid = re.sub(r'[^\w\u4e00-\u9fa5]', '', tvid)
            if not cid:
                cid = re.sub(r'[^\w\u4e00-\u9fa5]', '', name)

            if cid not in channels:
                ch = etree.Element("channel", id=cid)
                dn = etree.SubElement(ch, "display-name", attrib={"lang": "zh"})
                dn.text = name.strip()
                channels[cid] = ch

            # 单频道节目列表按时间排序，自动分配日期、自动算结束时间
            prog_list_sorted = []
            for prog in plist:
                t_str = prog.get("time", "")
                title = prog.get("program", "").strip()
                if not t_str or not title:
                    continue
                try:
                    hm = datetime.strptime(t_str.strip(), "%H:%M")
                    prog_list_sorted.append((hm.hour * 60 + hm.minute, t_str, title))
                except:
                    continue

            # 按时分从小到大排序
            prog_list_sorted.sort()
            total_cnt = len(prog_list_sorted)

            for idx, (_, t_str, title) in enumerate(prog_list_sorted):
                try:
                    hh, mm = map(int, t_str.split(":"))
                    # 跨天判断：凌晨节目自动往后排一天
                    if 0 <= hh < 6:
                        use_day = current_base_day + timedelta(days=1)
                    else:
                        use_day = current_base_day

                    start_dt = datetime.combine(use_day, datetime.min.time()).replace(hour=hh, minute=mm)

                    # 动态计算结束时间：取下一档节目开始时间，最后一档固定60分钟
                    if idx < total_cnt - 1:
                        next_hh, next_mm = map(int, prog_list_sorted[idx+1][1].split(":"))
                        next_start = datetime.combine(use_day, datetime.min.time()).replace(hour=next_hh, minute=next_mm)
                        # 下一档凌晨则换次日
                        if next_hh >= 0 and next_hh < 6:
                            next_start += timedelta(days=1)
                        stop_dt = next_start
                    else:
                        # 最后一个节目默认60分钟
                        stop_dt = start_dt + timedelta(minutes=60)

                    # 全局时间范围过滤
                    if start_dt < start_cutoff or start_dt > end_cutoff:
                        continue

                    # 标准EPG时间格式 +0800时区
                    p = etree.Element("programme")
                    p.set("start", start_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("stop", stop_dt.strftime("%Y%m%d%H%M%S +0800"))
                    p.set("channel", cid)
                    title_elem = etree.SubElement(p, "title", attrib={"lang": "zh"})
                    title_elem.text = title
                    programs.append(p)

                except Exception as e:
                    logging.debug(f"JSON单节目解析失败:{t_str} {str(e)[:30]}")
                    continue

        return channels, programs, len(channels), len(programs)
    except Exception as e:
        logging.warning(f"[{index}] JSON整体解析异常: {str(e)[:50]}")
        return {}, [], 0, 0
