"""
自動同步變更至 GitHub 腳本 (auto_git_sync.py)
功能：
- 背景監控專案目錄。
- 每隔固定秒數檢查是否有檔案異動（未暫存/未提交的變更）。
- 偵測到變更時，自動執行 git add -> git commit -> git push 到 GitHub。
- 具備防抖（debounce）機制，確保檔案儲存完畢後才提交。
"""

import os
import sys
import time
import subprocess
from datetime import datetime

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
CHECK_INTERVAL_SECONDS = 10  # 檢查間隔（秒）
DEBOUNCE_WAIT_SECONDS = 5    # 變更後的緩衝等待（秒）

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

def run_git(args, check=True):
    """執行 git 指令並回傳 stdout"""
    result = subprocess.run(
        ["git"] + args,
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        check=check
    )
    return result.stdout.strip()

def has_changes():
    """檢查是否有未提交的變更或未追蹤的新檔案"""
    try:
        status = run_git(["status", "--porcelain"])
        return len(status) > 0
    except Exception:
        return False

def sync_to_github():
    """執行完整 commit 與 push 流程"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{now_str}] 偵測到專案檔案有更動，準備同步至 GitHub...")
    try:
        # 1. 取得遠端最新進度 (避免 push 衝突)
        try:
            run_git(["pull", "--rebase", "origin", "main"], check=False)
        except Exception as e:
            print(f"⚠️ Git pull rebase 提示: {e}")

        # 2. 加入所有變更
        run_git(["add", "-A"])

        # 檢查是否確實有內容需要 commit
        status = run_git(["status", "--porcelain"])
        if not status:
            print("ℹ️ 無需提交的更動。")
            return

        # 3. 提交變更
        commit_msg = f"auto: 自動同步更新 ({now_str})"
        run_git(["commit", "-m", commit_msg])
        print(f"📝 已建立 Commit: {commit_msg}")

        # 4. 推送至 GitHub
        push_out = run_git(["push", "origin", "main"])
        print(f"🚀 成功推送至 GitHub！\n{push_out}")

    except subprocess.CalledProcessError as e:
        err = e.stderr if hasattr(e, "stderr") and e.stderr else str(e)
        print(f"❌ 同步失敗: {err}")
    except Exception as e:
        print(f"❌ 發生例外錯誤: {e}")

def main():
    print("=" * 55)
    print("🚀 GitHub 自動同步監視器已啟動")
    print(f"📂 監控目錄: {REPO_DIR}")
    print(f"⏱️ 檢查間隔: 每 {CHECK_INTERVAL_SECONDS} 秒")
    print("💡 任何檔案儲存後，將會自動 commit 並 push 到 GitHub")
    print("🛑 按下 Ctrl + C 即可停止監控")
    print("=" * 55)

    last_had_change = False

    while True:
        try:
            if has_changes():
                if not last_had_change:
                    print(f"👀 偵測到更動，緩衝等待 {DEBOUNCE_WAIT_SECONDS} 秒以確認儲存完成...")
                    time.sleep(DEBOUNCE_WAIT_SECONDS)
                    last_had_change = True
                    continue
                else:
                    sync_to_github()
                    last_had_change = False
            else:
                last_had_change = False

            time.sleep(CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n👋 自動同步已手動終止。")
            break
        except Exception as e:
            print(f"⚠️ 監控迴圈異常: {e}")
            time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

