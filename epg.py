def detect_baichuan_format(data):
    """检测是否为百川格式及具体类型"""
    if isinstance(data, list):
        if len(data) > 0:
            first = data[0]
            # 格式1: [{"tvid":"xxx","name":"xxx","list":[...]}]
            if 'tvid' in first and 'name' in first and 'list' in first:
                return "baichuan_v1"
            # 格式2: [{"channel":"xxx","time":"12:00","title":"xxx"}]
            elif 'channel' in first and 'time' in first:
                return "baichuan_flat"
            # 格式3: [{"id":"xxx","title":"xxx","epg":[...]}]
            elif 'id' in first and 'epg' in first:
                return "baichuan_v3"
    
    elif isinstance(data, dict):
        # 格式4: {"data":{"channels":[...],"epgs":{...}}}
        if 'data' in data and isinstance(data['data'], dict):
            if 'channels' in data['data']:
                return "baichuan_v2"
        # 格式5: {"channels":[{"cid":"xxx","cname":"xxx","epg":[...]}]}
        elif 'channels' in data:
            return "baichuan_v5"
        # 格式6: {"code":0,"msg":"success","data":[...]}
        elif 'code' in data and 'data' in data:
            return "baichuan_api"
    
    return None


def parse_json_format(content, index):
    """解析JSON格式，增强百川源支持"""
    channels = {}
    programs = []
    channel_count = 0
    program_count = 0
    
    try:
        text = content.decode('utf-8', errors='ignore')
        data = json.loads(text)
        
        # 检测百川格式
        baichuan_type = detect_baichuan_format(data)
        
        if baichuan_type:
            logging.info(f"[{index}] 🟦 检测到百川格式: {baichuan_type}")
            
            if baichuan_type == "baichuan_v1":
                return parse_baichuan_v1(data, index)
            elif baichuan_type == "baichuan_v2":
                return parse_baichuan_v2(data, index)
            elif baichuan_type == "baichuan_v3":
                return parse_baichuan_v3(data, index)
            elif baichuan_type == "baichuan_v5":
                return parse_baichuan_v5(data, index)
            elif baichuan_type == "baichuan_flat":
                return parse_baichuan_flat(data, index)
            elif baichuan_type == "baichuan_api":
                return parse_baichuan_api(data, index)
        
        # 如果不是百川格式，尝试其他JSON格式
        logging.info(f"[{index}] ⚠️ 非百川JSON格式，尝试通用解析")
        return parse_generic_json(data, index)
        
    except Exception as e:
        logging.error(f"[{index}] ❌ JSON解析失败: {str(e)}")
        return channels, programs, channel_count, program_count


def parse_baichuan_v1(data, index):
    """解析标准百川格式: [{"tvid":"xxx","name":"xxx","list":[...]}]"""
    channels = {}
    programs = []
    
    for channel in data:
        tvid = channel.get('tvid') or channel.get('id', '')
        name = channel.get('name', '未知频道')
        program_list = channel.get('list', [])
        
        if not tvid:
            continue
        
        # 创建频道
        channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', str(tvid))
        if channel_id not in channels:
            channel_elem = etree.Element("channel")
            channel_elem.set("id", channel_id)
            display_name = etree.SubElement(channel_elem, "display-name")
            display_name.text = name
            channels[channel_id] = channel_elem
        
        # 解析节目单
        for prog in program_list:
            time_str = prog.get('time', '')
            title = prog.get('program', prog.get('title', '未知节目'))
            
            if not time_str:
                continue
            
            try:
                # 解析时间（支持多种格式）
                start_dt = parse_baichuan_time(time_str)
                if not start_dt:
                    continue
                
                # 时间范围过滤
                if start_cutoff <= start_dt <= end_cutoff:
                    # 计算结束时间（默认30分钟）
                    duration = int(prog.get('duration', 1800))  # 秒
                    stop_dt = start_dt + timedelta(seconds=duration)
                    
                    # 创建节目元素
                    prog_elem = etree.Element("programme")
                    prog_elem.set("start", start_dt.strftime("%Y%m%d%H%M%S 0"))
                    prog_elem.set("stop", stop_dt.strftime("%Y%m%d%H%M%S 0"))
                    prog_elem.set("channel", channel_id)
                    
                    title_elem = etree.SubElement(prog_elem, "title")
                    title_elem.text = title
                    
                    # 可选：添加描述
                    desc = prog.get('desc', '')
                    if desc:
                        desc_elem = etree.SubElement(prog_elem, "desc")
                        desc_elem.text = desc
                    
                    programs.append(prog_elem)
                    
            except Exception as e:
                logging.debug(f"[{index}] 节目解析失败: {e}")
                continue
    
    logging.info(f"[{index}] ✅ 百川V1解析完成: {len(channels)}频道, {len(programs)}节目")
    return channels, programs, len(channels), len(programs)


def parse_baichuan_time(time_str):
    """解析百川时间格式"""
    if not time_str:
        return None
    
    try:
        # 格式1: "12:00" 或 "12:00:00"
        if re.match(r'^\d{1,2}:\d{2}(:\d{2})?$', time_str):
            time_part = time_str[:5]  # 取 HH:MM
            time_obj = datetime.strptime(time_part, "%H:%M").time()
            dt = datetime.combine(today, time_obj)
            
            # 如果时间已过，可能是明天的节目
            if dt < now:
                dt += timedelta(days=1)
            return dt
        
        # 格式2: "2026-04-29 12:00:00"
        if re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$', time_str):
            time_str = time_str.replace('T', ' ')
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        
        # 格式3: 时间戳（秒）
        if time_str.isdigit() and len(time_str) == 10:
            return datetime.fromtimestamp(int(time_str))
        
        # 格式4: 时间戳（毫秒）
        if time_str.isdigit() and len(time_str) == 13:
            return datetime.fromtimestamp(int(time_str) / 1000)
        
    except Exception:
        pass
    
    return None


def parse_baichuan_flat(data, index):
    """解析扁平百川格式: [{"channel":"xxx","time":"12:00","title":"xxx"}]"""
    channels = {}
    programs = []
    
    # 按频道分组
    channel_programs = {}
    for item in data:
        channel_name = item.get('channel', '')
        if not channel_name:
            continue
        
        if channel_name not in channel_programs:
            channel_programs[channel_name] = []
        
        channel_programs[channel_name].append(item)
    
    # 创建频道和节目
    for channel_name, prog_list in channel_programs.items():
        channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', channel_name)
        
        if channel_id not in channels:
            channel_elem = etree.Element("channel")
            channel_elem.set("id", channel_id)
            display_name = etree.SubElement(channel_elem, "display-name")
            display_name.text = channel_name
            channels[channel_id] = channel_elem
        
        # 解析节目
        for prog in prog_list:
            time_str = prog.get('time', '')
            title = prog.get('title', '未知节目')
            
            if not time_str:
                continue
            
            start_dt = parse_baichuan_time(time_str)
            if not start_dt or not (start_cutoff <= start_dt <= end_cutoff):
                continue
            
            # 计算结束时间
            end_str = prog.get('end', '')
            if end_str:
                stop_dt = parse_baichuan_time(end_str)
            else:
                # 默认30分钟
                stop_dt = start_dt + timedelta(minutes=30)
            
            prog_elem = etree.Element("programme")
            prog_elem.set("start", start_dt.strftime("%Y%m%d%H%M%S 0"))
            prog_elem.set("stop", stop_dt.strftime("%Y%m%d%H%M%S 0"))
            prog_elem.set("channel", channel_id)
            
            title_elem = etree.SubElement(prog_elem, "title")
            title_elem.text = title
            
            programs.append(prog_elem)
    
    logging.info(f"[{index}] ✅ 扁平百川解析完成: {len(channels)}频道, {len(programs)}节目")
    return channels, programs, len(channels), len(programs)


def parse_baichuan_api(data, index):
    """解析API格式百川: {"code":0,"msg":"success","data":[...]}"""
    actual_data = data.get('data', [])
    if isinstance(actual_data, list):
        # 递归调用检测
        return parse_json_format(json.dumps(actual_data).encode(), index)
    return {}, [], 0, 0
