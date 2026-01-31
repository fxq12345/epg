# 在抓取和解析EPG源的函数中，添加错误处理
def fetch_and_parse_epg(url):
    try:
        print(f"正在尝试抓取: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # 检查HTTP错误

        # 尝试解压缩
        if url.endswith('.gz'):
            try:
                data = gzip.decompress(response.content)
            except gzip.BadGzipFile:
                print(f"⚠️  {url} 不是有效的gzip文件，跳过该源")
                return None
        else:
            data = response.content

        # 解析XML
        epg_data = parse_xml(data)
        return epg_data

    except requests.exceptions.RequestException as e:
        print(f"❌ 抓取 {url} 失败: {e}，跳过该源")
        return None
    except Exception as e:
        print(f"❌ 处理 {url} 时出错: {e}，跳过该源")
        return None

# 在主逻辑中，过滤掉无效的EPG源
def main():
    epg_sources = [...]  # 你的EPG源列表
    all_epg_data = []

    for url in epg_sources:
        epg_data = fetch_and_parse_epg(url)
        if epg_data:
            all_epg_data.append(epg_data)
        else:
            print(f"⚠️  跳过无效的EPG源: {url}")

    # 合并有效的EPG数据
    if all_epg_data:
        merge_epg(all_epg_data)
        print("✅ EPG更新完成")
    else:
        print("❌ 没有有效的EPG源，更新失败")
