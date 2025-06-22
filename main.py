import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from modules import batch_merge

# Setup environments
CSV_PATH = "./input/products.csv"
CHECKPOINT_FILE = "processed_ids.txt"
TEMP_DIR = "temp"
OUTPUT_DIR = "output"
API_BASE_URL = "https://api.tiki.vn/product-detail/api/v1/products/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
MAX_WORKERS = 32
RETRY_ATTEMPTS = 3


def load_checkpoint():
    processed = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            for line in f:
                processed.add(line.strip())
    return processed


def save_checkpoint(product_id):
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(f"{product_id}\n")
        f.flush()


def fetch_product(product_id):
    url = f"{API_BASE_URL}{product_id}"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                product_data = response.json()
                if "description" in product_data and isinstance(
                    product_data["description"], str
                ):
                    soup = BeautifulSoup(product_data["description"], "html.parser")
                    clean_text = soup.get_text(separator=" ", strip=True)
                    product_data["description"] = clean_text
                    keys = ["id", "name", "url_key", "price", "description"]
                    trunc_data = {k: product_data.get(k) for k in keys}
                    return product_id, trunc_data
            elif response.status_code == 403:
                log_error(f"403 Forbidden: {url}")
                return product_id, None
        except (requests.RequestException, ConnectionError) as e:
            log_error(f"Attempt {attempt + 1} failed for {product_id}: {str(e)}")
            time.sleep(2**attempt)  # Exponential backoff
    return product_id, None


def process_id(product_id):
    pid, data = fetch_product(product_id)
    if data:
        output_path = os.path.join(TEMP_DIR, f"{pid}.json")
        with open(output_path, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        save_checkpoint(pid)
        log_progress(f"Processed {pid}")
    return pid, bool(data)


def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)


def main():
    processed = load_checkpoint()
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Read and filter IDs
    with open(CSV_PATH, "r") as f:
        reader = csv.reader(f)
        all_ids = [row[0] for row in reader]

    unprocessed = [pid for pid in all_ids if pid not in processed]
    total = len(unprocessed)
    log_progress(f"Starting processing. Total: {total} items")

    # Thread pool execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_id, pid): pid for pid in unprocessed}

        for i, future in enumerate(as_completed(futures), 1):
            pid, success = future.result()
            if not success:
                log_error(f"Permanent failure for {pid}")
            if i % 100 == 0:
                log_progress(f"Progress: {i}/{total} ({i / total:.1%})")


if __name__ == "__main__":
    main()
    print("Merging files!")
    time.sleep(5)
    batch_merge(1000, TEMP_DIR, OUTPUT_DIR)
