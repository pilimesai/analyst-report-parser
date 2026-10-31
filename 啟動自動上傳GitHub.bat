@echo off
chcp 65001 >nul
title GitHub 自動同步監視器
echo 正在啟動 GitHub 自動同步...
python auto_git_sync.py
pause

