from config import CLUSTER_NAMES_DICT
from dao.result_status_dao import insert_and_update
from utils.tag_names_getter import get_tag_names_dict
from utils.id_generator import generate_id
from logger import Logger


class UploadStep:
    """Do step 3: insert and update abnormal clusters into MySQL table: ts_air_compressor_result_status."""

    def __init__(self):
        self.logger = Logger.get_instance()  # Set up logger.

    def conduct(self, abnormal_cluster_data, load_date_str):
        if len(abnormal_cluster_data) <= 0:  # If result status data is empty.
            self.logger.write('error', 'Upload result status step stopped: result status data empty.')
            return

        result_status_df = abnormal_cluster_data[['datetime', 'asset_code', 'cluster']]

        # Insert tag name column at the second last place, right before the cluster column.
        tag_name_index = len(result_status_df.columns) - 1
        # Use asset codes to map tag names.
        result_status_df.insert(tag_name_index, 'tag_name', result_status_df['asset_code'])
        tag_names_dict = get_tag_names_dict()
        result_status_df['tag_name'].replace(tag_names_dict, inplace=True)

        # Transform clusters from integer to string.
        result_status_df['cluster'].replace(CLUSTER_NAMES_DICT, inplace=True)

        result_status_df.rename(columns={'cluster': 'result_status'}, inplace=True)
        # Sort by ascending datetime and then asset code.
        result_status_df.sort_values(by=['datetime', 'asset_code'], ascending=[True, True], inplace=True)

        # ID is hash md5 of datetime and asset_code values.
        result_status_df.insert(0, 'id', '0')
        result_status_df['id'] = result_status_df['datetime'].astype(str) + result_status_df['asset_code']
        result_status_df['id'] = result_status_df['id'].apply(generate_id)
        # Drop duplicates by id.
        result_status_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

        insert_and_update(result_status_df, load_date_str, self.logger)
        self.logger.write('info', 'Upload result status step completed.')
