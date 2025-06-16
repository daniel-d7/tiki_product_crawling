import pandas as pd
import requests
import json
import os

def crawl(pid_file, output_dir, api_url, headers):
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(pid_file)
    product_ids = df['id'].astype(str).tolist()

    for pid in product_ids:
        url = api_url.format(pid)
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                product_data = response.json()
                json_path = os.path.join(output_dir, f'{pid}.json')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(product_data, f, ensure_ascii=False, indent=2)
                print(f'Success: {pid}')
            else:
                print(f'Failed to fetch {pid}: HTTP {response.status_code}')
        except Exception as e:
            print(f'Error fetching {pid}: {e}')