def crawl_weifang_single(ch_name, base_url, day_str, current_day):
    # 增加请求头，模拟真实浏览器
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

    for attempt in range(1, MAX_RETRY + 1):
        try:
            url = f"{base_url}/{day_str}"
            print(f"正在抓取：{url}")  # 打印URL，方便调试

            # 发送请求
            resp = requests.get(url, headers=headers, timeout=5)
            resp.encoding = "utf-8"
            
            # 检查响应内容是否包含验证特征（可选，用于更精确的判断）
            if "验证" in resp.text or "请输入验证码" in resp.text:
                print(f"警告：{url} 需要验证，尝试使用代理或更换User-Agent")
                time.sleep(2) # 增加等待时间
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 查找节目列表的容器（根据tvsou的页面结构调整）
            # 常见的容器标签：ul, div, li
            container = soup.find("ul", class_="epg-list") or soup.find("div", class_="epg-content")
            
            if not container:
                print(f"警告：{url} 未找到节目列表容器")
                continue

            program_list = []
            # 遍历节目项
            for item in container.find_all(["li", "div"]):
                txt = item.get_text(strip=True)
                # 匹配时间格式：08:00 节目名
                match = re.match(r"(\d{1,2}:\d{2})\s*(.+)", txt)
                if not match:
                    continue
                time_str, title = match.groups()
                if len(title) < 2 or "广告" in title:
                    continue
                hh, mm = time_str.split(":")
                prog_time = datetime.combine(current_day, datetime.min.time().replace(hour=int(hh), minute=int(mm)))
                program_list.append((prog_time, title))
            
            # 如果没有抓取到节目，跳过
            if not program_list:
                print(f"警告：{url} 抓取到的节目列表为空")
                continue

            # 生成精准时间
            precise_programs = []
            for i in range(len(program_list)):
                start_time, title = program_list[i]
                if i == len(program_list) - 1:
                    stop_time = start_time + timedelta(minutes=60)
                else:
                    stop_time = program_list[i+1][0]
                start = start_time.strftime("%Y%m%d%H%M%S +0800")
                stop = stop_time.strftime("%Y%m%d%H%M%S +0800")
                precise_programs.append((start, stop, title))
            
            time.sleep(0.3)
            return precise_programs
        except Exception as e:
            print(f"抓取失败 {url}：{e}")
            time.sleep(1)
            continue
    return []
