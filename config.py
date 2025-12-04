"""All configurations."""

import os


# File settings.
DATA_ACQUISITION_BASE_PATH = os.path.dirname(os.path.abspath(__file__))  # Where python files are stored.

# Source folder.
SOURCE_FOLDER_BASE_PATH = os.path.join(os.path.dirname(DATA_ACQUISITION_BASE_PATH), 'source', 'air_compressor')


# MySQL database connection settings.
DB_IP = '10.52.198.92'
DB_PORT = '3306'
DB_USERNAME = 'suez'
DB_PASSWORD = 'suezAdmin!3'
DB_SCHEMA = 'mw_task1_pm_db'
DB_POOL_SIZE = 30
DB_MAX_OVERFLOW = 15


# Logger settings.
DEFAULT_LOG_LEVEL = 20  # CRITICAL: 50; ERROR: 40; WARNING: 30; INFO: 20; DEBUG: 10.
DF_ALIGNMENT = 'center'  # Alignment of data frame columns.


# Time zone settings.
TIMEZONE = 'Asia/Singapore'


# Time series data retention settings.
# Number of days of data before now to be retained.
RETENTION_LENGTH = 1500
