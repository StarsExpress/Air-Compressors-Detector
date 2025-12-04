"""All configurations."""

import os


# File path settings.
APP_BASE_PATH = os.path.dirname(os.path.abspath(__file__))  # Where python files are stored.

# Source folder.
SOURCE_FOLDER_BASE_PATH = os.path.join(os.path.dirname(APP_BASE_PATH), 'source', 'air_compressor')


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


# Runtime query settings.
# Additional number of days of runtime data needed for DBSCAN fit and predict.
ADDITIONAL_RUNTIME_DAYS = 365

RUNTIME_COLUMNS_LIST = ['id', 'datetime', 'asset_code', 'runtime', 'status']  # Columns needed for runtime data.


# DBSCAN clustering settings.
DBSCAN_PARAMETERS_DICT = {'IV': {'minimal_samples': 12, 'epsilon': 5},
                          'MSR_2': {'minimal_samples': 12, 'epsilon': 1}}

RUNTIME_LEVEL = 30  # Runtime threshold. Unit: minutes.


# Cluster names settings.
CLUSTER_NAMES_DICT = {0: 'Outlier', 2: 'Minority'}  # Dictionary to name clusters.


# Unsent result status settings.
UNSENT_RESULT_STATUS_COLUMNS_LIST = ['Time', 'Tag Name', 'Asset Code', 'Severity']
