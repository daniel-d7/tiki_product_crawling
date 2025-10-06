# tiki_product_crawling

Tiki Product Crawling is a Python-based tool for efficiently crawling product data from Tiki.vn using their public API. It supports large-scale, multi-threaded crawling, checkpointing, and batch merging of results for further analysis or data science tasks.

## Features
- **Multi-threaded crawling** for high performance
- **Checkpointing** to resume interrupted crawls
- **Batch merging** of crawled product data into JSON files
- **Automatic cleaning** of product descriptions (HTML to plain text)
- **Configurable batch size and worker count**

## Project Structure

```
├── main.py                # Main script to run the crawler
├── modules/
│   ├── __init__.py
│   └── merge.py           # Batch merging logic
├── input/
│   └── products.csv       # List of product IDs to crawl
├── temp/                  # Temporary JSON files for each product
├── output/                # Final merged JSON batches
├── README.md
```

## Setup
1. **Install dependencies:**
	- Python 3.7+
	- Install required packages:
	  ```bash
	  pip install requests beautifulsoup4
	  ```
2. **Prepare input:**
	- Place your product IDs in `input/products.csv` (one ID per line, with a header `id`).

## Usage
Run the crawler from the project root:

```bash
python main.py
```

The script will:
- Read product IDs from `input/products.csv`
- Crawl product data from Tiki.vn
- Save each product as a JSON file in `temp/`
- Merge results into batches in `output/`

## Output
- Individual product JSONs: `temp/<product_id>.json`
- Merged batches: `output/products_batch_<n>.json`

## Checkpointing & Resume
The script maintains a `processed_ids.txt` file to avoid re-crawling products. You can safely stop and restart the script.

## Customization
- Adjust `MAX_WORKERS` and `batch_merge` batch size in `main.py` for performance tuning.

## License
MIT License