# --- Auto-install required modules ---
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import pandas as pd
except ImportError:
    install("pandas")
    import pandas as pd

try:
    import requests
except ImportError:
    install("requests")
    import requests

# --- Script starts here ---
import os
import time

INPUT_FILE = "videoquery.csv"
BATCH_SIZE = 1000  # Adjust if needed
EXPORT_PREFIX = "export_"
TIMEOUT = 5  # seconds for URL check timeout

def check_video_url(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        return response.status_code == 200
    except requests.RequestException:
        return False

def process_csv():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File '{INPUT_FILE}' not found. Please upload it to your Replit files.")
        return

    df = pd.read_csv(INPUT_FILE)
    total_rows = len(df)
    print(f"📄 Total rows: {total_rows}")

    batch_num = 1
    for start in range(0, total_rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_rows)
        batch = df.iloc[start:end].copy()
        print(f"\n🚀 Processing batch {batch_num} — rows {start + 1} to {end}")

        for i in range(len(batch)):
            url = batch.iloc[i]["video_url"]
            is_valid = check_video_url(url)
            batch.at[batch.index[i], "video_type"] = "short" if is_valid else "long"

            remaining = len(batch) - i
            print(f"⏳ Countdown: {remaining}", end="\r")

        export_file = f"{EXPORT_PREFIX}{batch_num}.csv"
        batch.to_csv(export_file, index=False)
        print(f"\n✅ Exported batch {batch_num} to '{export_file}'")
        batch_num += 1

if __name__ == "__main__":
    process_csv()
