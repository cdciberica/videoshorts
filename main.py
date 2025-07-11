import os
import subprocess

# ✅ Auto-install dependencies
try:
    import pandas as pd
    import requests
    from tqdm import tqdm
except ImportError:
    subprocess.check_call(['pip', 'install', 'pandas', 'requests', 'tqdm'])
    import pandas as pd
    import requests
    from tqdm import tqdm

from pathlib import Path

# ✅ File upload for Replit
def upload_csv():
    from replit import file
    print("📁 Upload your CSV file (with columns: id, user_id, video_id)...")
    uploaded = file.upload()
    csv_path = list(uploaded.keys())[0]
    print(f"✅ Uploaded: {csv_path}")
    return csv_path

# ✅ Check if video is a Short (no redirect = short)
def is_youtube_short(video_id):
    url = f"https://www.youtube.com/shorts/{video_id}"
    try:
        response = requests.get(url, allow_redirects=False, timeout=10)
        return 'short' if response.status_code == 200 else 'long'
    except Exception as e:
        print(f"⚠️ Error checking {video_id}: {e}")
        return 'error'

# ✅ Main processor
def process_videos(csv_path, batch_size):
    df = pd.read_csv(csv_path)
    total_videos = len(df)
    print(f"\n🔢 Total videos to process: {total_videos}")
    
    os.makedirs("exports", exist_ok=True)

    for start in range(0, total_videos, batch_size):
        end = min(start + batch_size, total_videos)
        batch_df = df.iloc[start:end].copy()
        print(f"\n🚀 Processing batch {start // batch_size + 1}: {start + 1} to {end}")
        
        batch_df["video_type"] = [
            is_youtube_short(vid) for vid in tqdm(batch_df["video_id"], desc="🔍 Analyzing")
        ]
        
        export_path = f"exports/export_{start // batch_size + 1}.csv"
        batch_df.to_csv(export_path, index=False)
        print(f"✅ Saved: {export_path}")

if __name__ == "__main__":
    print("📦 YouTube Shorts Checker — Replit Edition")
    csv_path = upload_csv()
    batch_size = int(input("🔢 Enter batch size: "))
    process_videos(csv_path, batch_size)
