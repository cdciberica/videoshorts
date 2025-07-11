import pandas as pd
import requests
import time
import re
import os
import signal
import json
from isodate import parse_duration
from tqdm import tqdm
from multiprocessing import Pool, Manager, current_process, Lock

# === Config ===
INPUT_CSV = "your_videos.csv"
OUTPUT_CSV = "videos_with_type.csv"
FAIL_LOG = "failures.log"
API_KEY_FILE = "youtube_api.txt"
NUM_WORKERS = 8
SAVE_INTERVAL = 500  # Save every N videos

# === Global flags for graceful shutdown ===
shutdown = False
def handle_sigint(sig, frame):
    global shutdown
    shutdown = True
    print("\n⛔ Graceful shutdown triggered. Saving progress...")

signal.signal(signal.SIGINT, handle_sigint)

# === Load API keys ===
with open(API_KEY_FILE, "r") as f:
    api_keys = [line.strip() for line in f if line.strip()]

# === Fetch from YouTube API with key rotation ===
def fetch_video_details(video_id, key_queue):
    tried_keys = set()

    while not key_queue.empty() and len(tried_keys) < len(api_keys):
        current_key = key_queue.get()
        tried_keys.add(current_key)

        url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,player&id={video_id}&key={current_key}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                key_queue.put(current_key)
                return response.json()
            elif response.status_code == 403:
                print(f"[{current_process().name}] Quota hit. Rotating key.")
                continue
            else:
                return None
        except Exception as e:
            continue

    return None

def is_short_video(duration_iso, embed_html):
    try:
        duration = parse_duration(duration_iso).total_seconds()
        if duration > 60:
            return False
        match = re.search(r'width="(\d+)" height="(\d+)"', embed_html)
        if match:
            width, height = int(match.group(1)), int(match.group(2))
            return height >= width
    except:
        pass
    return False

def process_video(args):
    video_id, key_queue = args
    retries = 3
    while retries > 0:
        data = fetch_video_details(video_id, key_queue)
        if data and "items" in data and data["items"]:
            item = data["items"][0]
            duration = item.get("contentDetails", {}).get("duration", "")
            embed_html = item.get("player", {}).get("embedHtml", "")
            result = "short" if is_short_video(duration, embed_html) else "standard"
            return (video_id, result)
        retries -= 1
        time.sleep(1)
    return (video_id, "failed")

def save_progress(partial_results, output_lock):
    output_lock.acquire()
    try:
        existing = pd.read_csv(OUTPUT_CSV) if os.path.exists(OUTPUT_CSV) else pd.DataFrame(columns=["video_id", "video_type"])
        existing_ids = set(existing["video_id"])
        new_df = pd.DataFrame(partial_results, columns=["video_id", "video_type"])
        new_df = new_df[~new_df["video_id"].isin(existing_ids)]
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.to_csv(OUTPUT_CSV, index=False)
    finally:
        output_lock.release()

def log_failures(failed_ids, lock):
    lock.acquire()
    try:
        with open(FAIL_LOG, "a") as f:
            for vid in failed_ids:
                f.write(f"{vid}\n")
    finally:
        lock.release()

# === Main ===
def main():
    df = pd.read_csv(INPUT_CSV)
    all_video_ids = set(df["video_id"])

    if os.path.exists(OUTPUT_CSV):
        done_df = pd.read_csv(OUTPUT_CSV)
        done_ids = set(done_df["video_id"])
    else:
        done_ids = set()

    remaining_ids = list(all_video_ids - done_ids)
    print(f"🎬 {len(remaining_ids)} videos remaining (out of {len(all_video_ids)})")

    with Manager() as manager:
        key_queue = manager.Queue()
        for key in api_keys:
            key_queue.put(key)

        lock = manager.Lock()
        fail_lock = manager.Lock()
        args = [(vid, key_queue) for vid in remaining_ids]
        results = []
        failed_ids = []

        with Pool(processes=NUM_WORKERS) as pool:
            for i, result in enumerate(tqdm(pool.imap_unordered(process_video, args), total=len(args))):
                if shutdown:
                    break
                video_id, video_type = result
                results.append((video_id, video_type))
                if video_type == "failed":
                    failed_ids.append(video_id)

                if len(results) >= SAVE_INTERVAL:
                    save_progress(results, lock)
                    log_failures(failed_ids, fail_lock)
                    results = []
                    failed_ids = []

        # Final flush
        if results:
            save_progress(results, lock)
        if failed_ids:
            log_failures(failed_ids, fail_lock)

    print("✅ Done. Output saved to", OUTPUT_CSV)

if __name__ == "__main__":
    main()
