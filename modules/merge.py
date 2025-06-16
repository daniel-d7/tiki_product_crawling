import os
import json

def batch_merge(batch_size, temp_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    json_files = [
        f for f in sorted(os.listdir(temp_dir)) 
        if f.endswith('.json')
    ]
    
    for batch_num, i in enumerate(range(0, len(json_files), batch_size), 1):
        batch_files = json_files[i:i+batch_size]
        merged_data = []
        
        for file_name in batch_files:
            file_path = os.path.join(temp_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                merged_data.append(json.load(f))
        
        output_path = os.path.join(output_dir, f'products_batch_{batch_num}.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)