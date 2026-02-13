name: EPG自动更新
on:
  workflow_dispatch:
  schedule:
    - cron: '30 16 * * *'  # 每天 16:30 UTC 运行

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - name: 检出代码仓库
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: 同步远程最新代码
        run: |
          git fetch --all
          git reset --hard origin/master

      - name: 配置Python环境
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: 安装依赖库
        run: pip install requests lxml beautifulsoup4

      - name: 自动创建output目录
        run: mkdir -p output

      - name: 运行合并脚本生成EPG文件
        run: python merge.py

      - name: 复制文件到根目录并提交
        run: |
          cp output/epg.gz epg.gz
          git config --global user.name "GitHub Actions"
          git config --global user.email "actions@github.com"
          
          # 强制添加文件，即使内容没有变化
          git add -f epg.gz
          
          # 检查是否有变更，如果没有就创建一个空提交
          if git diff --staged --quiet; then
            echo "⚠️ 没有文件变更，创建空提交以触发更新"
            git commit --allow-empty -m "✅ EPG自动更新 (空提交) $(date +'%Y-%m-%d %H:%M:%S')"
          else
            git commit -m "✅ EPG自动更新 $(date +'%Y-%m-%d %H:%M:%S')"
          fi
          
          git push -f origin master
