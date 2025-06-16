from datetime import datetime

def log_progress(log_file, message):
    time_format = "%Y-%m-%d %H:%M:%S"  # Corrected format
    now = datetime.now()
    timestamp = now.strftime(time_format)
    print(timestamp + " " + message)
    with open(log_file, "a") as f:
        f.write(timestamp + " " + message + "\n")