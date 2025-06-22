import json
import logging
import os


def batch_merge(batch_size, temp_dir, output_dir):
    try:
        os.makedirs(output_dir, exist_ok=True)
        json_files = sorted(
            f
            for f in os.listdir(temp_dir)
            if f.endswith(".json") and os.path.isfile(os.path.join(temp_dir, f))
        )
        total_files = len(json_files)
        logging.info(f"Found {total_files} JSON files to process in {temp_dir}")
        for batch_index, start_idx in enumerate(range(0, total_files, batch_size), 1):
            batch_files = json_files[start_idx : start_idx + batch_size]
            merged_data = []
            for file_name in batch_files:
                file_path = os.path.join(temp_dir, file_name)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        merged_data.append(json.load(f))
                except Exception as e:
                    logging.error(f"Error reading {file_name}: {str(e)}")
            output_path = os.path.join(output_dir, f"products_batch_{batch_index}.json")
            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(merged_data, f, ensure_ascii=False, indent=2)
                logging.info(
                    f"Saved batch {batch_index} with {len(batch_files)} files to {
                        output_path
                    }"
                )
            except Exception as e:
                logging.error(f"Error writing batch {batch_index}: {str(e)}")
    except Exception as e:
        logging.exception(f"Critical error in batch_merge: {str(e)}")
