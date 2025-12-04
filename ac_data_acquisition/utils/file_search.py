from config import SOURCE_FOLDER_BASE_PATH, DATA_ACQUISITION_BASE_PATH, TIMEZONE
import os
import datetime
from zoneinfo import ZoneInfo


# Return a list of unprocessed csv files, and last processed timestamp.
def find_unprocessed_csv(lock):
    last_processed_txt_path = os.path.join(DATA_ACQUISITION_BASE_PATH, 'last_processed_timestamp.txt')

    lock.acquire()
    try:  # Find last processed timestamp and set it as timestamp.
        txt = open(last_processed_txt_path, 'r')
        last_processed_timestamp = str(txt.read())
        txt.close()

    finally:
        lock.release()

    # Set timestamp format and time zone.
    last_processed_timestamp = datetime.datetime.strptime(last_processed_timestamp, '%Y-%m-%d %H:%M:%S')
    last_processed_timestamp = last_processed_timestamp.replace(tzinfo=ZoneInfo(TIMEZONE))

    targeted_prefix = 'air_compressor_runtime_data'  # Targeted prefix.
    targeted_suffix = '.csv'  # Targeted suffix.

    match_list = []
    for o in os.listdir(SOURCE_FOLDER_BASE_PATH):
        # Filtering: files with targeted prefix and suffix.
        if not o.startswith(targeted_prefix):
            continue
        if not o.endswith(targeted_suffix):
            continue

        match_list.append(o)  # Only append files that pass both filtering.

    return sorted(map(lambda x: os.path.join(SOURCE_FOLDER_BASE_PATH, x), match_list)), last_processed_timestamp


if __name__ == '__main__':
    import threading

    lock_main = threading.Lock()
    print(find_unprocessed_csv(lock_main))
