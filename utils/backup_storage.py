from config import DATA_ACQUISITION_BASE_PATH, SOURCE_FOLDER_BASE_PATH
import os


# Move input file into backup folder. Argument is file path.
def move_to_backup(file_path):
    os.rename(file_path, os.path.join(SOURCE_FOLDER_BASE_PATH, 'backup', os.path.basename(file_path)))


# Update the last processed timestamp in string format.
def update_backup_time(last_processed_timestamp_str, lock):
    lock.acquire()
    try:
        txt = open(os.path.join(DATA_ACQUISITION_BASE_PATH, 'last_processed_timestamp.txt'), 'w')
        txt.write(last_processed_timestamp_str)
        txt.close()

    finally:
        lock.release()
