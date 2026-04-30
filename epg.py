import os
import gzip
import requests
import sys

# 强制输出缓冲，方便看日志
sys.stdout.reconfigure(line_buffering=True)

# ==================== 配置 ====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
OUTPUT_FILE = "epg.gz"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    # 移除了 Accept-Encoding，让 requests 自动处理解压
}

def main():
    # 1. 读取配置
    if not os.path.exists(CONFIG_FILE):
        print("❌ config.txt 不存在")
        return
    
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    
    if not lines:
        print("❌ config.txt 为空")
        return
    
    # 只取第一个源
    url = lines[0]
    print(f"🔽 正在请求: {url}")

    try:
        # 2. 发起请求 (注意: 不带 Accept-Encoding 让 requests 自动解压)
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        
        print(f"✅ HTTP 状态码: {r.status_code}")
        print(f"   Content-Type: {r.headers.get('Content-Type', '未知')}")
        
        # 3. 关键检查：判断内容是 XML 还是 HTML 错误页
        content_text = r.content.decode('utf-8', errors='ignore') # 尝试转为文本查看
        
        if content_text.startswith('<?xml'):
            print("📝 检测到 XML 内容，准备打包...")
            final_content = r.content # 使用原始二进制数据
        elif '<html' in content_text[:50].lower() or '<!doctype' in content_text[:15].lower():
            print("❌ 错误: 源站返回的是 HTML 页面，而不是 XML 数据！")
            print("   这通常是因为源站地址错误、需要特定参数或源站已宕机。")
            print("--- 返回的前 200 字符内容预览 ---")
            print(repr(content_text[:200])) # 打印前200字符的 repr，看看到底是什么
            print("--------------------------------")
            return
        else:
            print("⚠️ 警告: 未检测到标准 XML 头，但尝试继续处理...")
            final_content = r.content

        # 4. 写入 Gzip
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with gzip.open(out_path, "wb") as f:
            f.write(final_content)
            
        print(f"✅ 打包成功！文件大小: {os.path.getsize(out_path)} 字节")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求异常: {e}")
    except Exception as e:
        print(f"❌ 处理异常: {e}")

if __name__ == "__main__":
    main()
