import os
import pandas as pd
import json
from config import SOURCE_FOLDER_BASE_PATH
from dao.air_compressor_meta_dao import read_air_compressor_meta, truncate_and_insert
from utils.id_generator import generate_id


class AirCompressorMetaProcessor:
    """Process air compressor meta data."""

    def __init__(self, logger):
        self.logger = logger  # Set up logger.

    def truncate_and_upload(self, lock):
        air_compressor_meta_data_path = os.path.join(
            SOURCE_FOLDER_BASE_PATH, "air_compressor_meta.csv"
        )

        database_meta_data_df = read_air_compressor_meta(
            self.logger
        )  # Query database meta data.

        try:  # If air compressor meta data csv file exists.
            air_compressor_meta_df = pd.read_csv(air_compressor_meta_data_path)
            if (
                len(air_compressor_meta_df) <= 0
            ):  # If air compressor meta data is empty.
                self.logger.write("error", "Air compressor meta data empty.")
                return

            # Remove duplicates and sort by asset code.
            air_compressor_meta_df.drop_duplicates(inplace=True)
            air_compressor_meta_df.sort_values(
                by=["asset_code"], ascending=[True], inplace=True
            )

            # Calculate md5 of meta data from csv file.
            air_compressor_meta_df_values_list = air_compressor_meta_df.values.tolist()
            air_compressor_meta_df_values_str = ", ".join(
                map(str, air_compressor_meta_df_values_list)
            )
            current_md5 = generate_id(air_compressor_meta_df_values_str)
            air_compressor_meta_data_md5_path = os.path.join(
                SOURCE_FOLDER_BASE_PATH, "air_compressor_meta_data_md5.json"
            )

            try:  # If last saved static data md5 exists.
                lock.acquire()
                try:
                    file = open(air_compressor_meta_data_md5_path, "r")
                    saved_md5 = json.load(file)["air_compressor_meta_data_md5"]
                    file.close()

                finally:
                    lock.release()

                # If current md5 matches last saved md5 and database meta data is not empty, do nothing.
                if (current_md5 == saved_md5) & (len(database_meta_data_df) > 0):
                    self.logger.write(
                        "info",
                        "Air compressor meta data unchanged so no updates needed.",
                    )
                    return

                # Insert a column called id which is generated from each air compressor's info.
                air_compressor_meta_df.insert(
                    0,
                    "id",
                    air_compressor_meta_df.apply(
                        lambda x: ", ".join(map(str, x.tolist())), axis=1
                    ),
                )
                air_compressor_meta_df["id"] = air_compressor_meta_df["id"].apply(
                    generate_id
                )

                try:
                    truncate_and_insert(air_compressor_meta_df, self.logger)

                    lock.acquire()
                    try:
                        file = open(air_compressor_meta_data_md5_path, "w")
                        json.dump(
                            {"air_compressor_meta_data_md5": current_md5}, file
                        )  # Update new meta data md5.
                        file.close()

                    finally:
                        lock.release()

                except Exception as e:  # If truncation and insertion fails.
                    self.logger.write(
                        "error",
                        f"Air compressor meta data table truncation and insertion failed: {e}.",
                    )

            except FileNotFoundError:  # If meta data md5 json file doesn't exist.
                self.logger.write(
                    "error", "No air compressor meta data md5 json found."
                )

        except FileNotFoundError:  # If air compressor meta data csv file doesn't exist.
            self.logger.write("error", "No air compressor meta csv file found.")
