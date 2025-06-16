from modules import crawl
from modules import batch_merge
from modules import log_progress
from datetime import datetime
import os

# Setup environments
PID_FILE = './input/products.csv'
TEMP_DIR = './temp'
OUTPUT_DIR = 'output'
BATCH_SIZE = 1000
API_URL = 'https://api.tiki.vn/product-detail/api/v1/products/{}'
HEADERS = {
    "User-Agent": "curl/8.13.0",
     "Accept": "*/*",
}
runtime_format = "%Y-%m-%d"
now = datetime.now()
runtime = now.strftime(runtime_format)
os.makedirs("./logs/", exist_ok=True)
log_file = f"./logs/log_progress_at_{runtime}.txt"

# Logging start time
log_progress(log_file, "Sequence started!")

# Call function to start crawling
crawl(PID_FILE, TEMP_DIR, API_URL, HEADERS)

# Call function to merge 1000 product each json file
batch_merge(BATCH_SIZE, TEMP_DIR, OUTPUT_DIR)

# Clearing temp json file
for file_name in os.listdir(TEMP_DIR):
    file_path = os.path.join(TEMP_DIR, file_name)
    if os.path.isfile(file_path):  # Check if it's a file
        os.remove(file_path)

# Logging stop time
log_progress(log_file, "Sequence completed!")