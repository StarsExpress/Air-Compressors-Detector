from config import SOURCE_FOLDER_BASE_PATH
import os
import pandas as pd


def get_tag_names_dict():
    # Read compressor codes csv.
    air_compressors_df = pd.read_csv(os.path.join(SOURCE_FOLDER_BASE_PATH, 'air_compressor_codes.csv'))
    air_compressors_df['asset_code'] = air_compressors_df['asset_code'].astype(str)
    air_compressors_df['tag_name'] = air_compressors_df['tag_name'].astype(str)

    if len(air_compressors_df) <= 0:  # If csv is empty, return empty dictionary.
        return dict()

    tag_code_dict = dict()  # Dictionary of asset codes as keys and tag names as values.
    for i, row in air_compressors_df.iterrows():
        asset_code = air_compressors_df['asset_code'].loc[i]

        tag_name = air_compressors_df['tag_name'].loc[i]
        tag_name = tag_name.split('.')[0]  # Only take the string before the dot as tag name.
        tag_code_dict.update({asset_code: tag_name})

    return tag_code_dict


if __name__ == '__main__':
    print(get_tag_names_dict())
