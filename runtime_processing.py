import os
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import swifter
from sqlalchemy import text
from config import SOURCE_FOLDER_BASE_PATH, TIMEZONE
from dao.db_connector import engine
from utils.file_search import find_unprocessed_csv
from utils.backup_storage import move_to_backup, update_backup_time
from utils.time_series_interpolation import clean_and_interpolate
from utils.id_generator import generate_id


class RuntimeProcessor:
    """Process air compressor's runtime."""

    def __init__(self, logger):
        self.logger = logger  # Set up logger.

        self.runtime_df = (
            pd.DataFrame()
        )  # Data frame to store all processed runtime data.

    def process(self, lock):
        # List of unprocessed runtime csv and last processed timestamp.
        unprocessed_runtime_csv_path_list, last_processed_timestamp = (
            find_unprocessed_csv(lock)
        )
        if (
            len(unprocessed_runtime_csv_path_list) <= 0
        ):  # If no unprocessed runtime csv, return.
            self.logger.write("warning", "No unprocessed runtime csv found.")
            return

        # Read compressor codes csv.
        air_compressors_df = pd.read_csv(
            os.path.join(SOURCE_FOLDER_BASE_PATH, "air_compressor_codes.csv")
        )
        if len(air_compressors_df) <= 0:  # If csv is empty, return.
            self.logger.write("error", "Air compressor code csv not found.")
            return

        # Dictionary of tag names as keys and asset codes as values.
        tag_code_dict = dict(
            zip(
                list(air_compressors_df["tag_name"]),
                list(air_compressors_df["asset_code"].astype(str)),
            )
        )

        # List of time labels for unprocessed runtime csv.
        time_labels_list = [
            str(path.split("_")[-1].replace("-", "").replace(".csv", ""))
            for path in unprocessed_runtime_csv_path_list
        ]

        output_df_list = []  # List to receive all processed runtime data frames.

        for i in range(len(unprocessed_runtime_csv_path_list)):
            runtime_df = pd.read_csv(
                unprocessed_runtime_csv_path_list[i]
            )  # Read iterated runtime data.
            move_to_backup(
                unprocessed_runtime_csv_path_list[i]
            )  # Move read csv file to backup folder.
            self.logger.write(
                "info",
                time_labels_list[i] + "'" + "s runtime csv moved to backup folder.",
            )

            if (
                len(runtime_df) <= 0
            ):  # If iterated runtime data is empty, continue to next iteration.
                self.logger.write(
                    "error", time_labels_list[i] + "'" + "s runtime csv empty."
                )
                continue

            runtime_df.rename(
                columns={"SENSORDD": "datetime"}, inplace=True
            )  # Rename columns.
            # Set datetime as timestamp.
            runtime_df["datetime"] = (
                runtime_df["datetime"].astype("datetime64[s]").dt.tz_localize(TIMEZONE)
            )

            # Filter records that are later than last processed timestamp but earlier than now.
            runtime_df = runtime_df[
                (runtime_df["datetime"] < datetime.datetime.now(tz=ZoneInfo(TIMEZONE)))
                & (runtime_df["datetime"] > last_processed_timestamp)
            ][list(runtime_df.columns)]
            if (
                len(runtime_df) <= 0
            ):  # If filtered data is empty, continue to next iteration.
                self.logger.write(
                    "error",
                    time_labels_list[i]
                    + "'"
                    + "s runtime csv records all earlier"
                    + " than last processed time: "
                    + str(last_processed_timestamp)
                    + " or later than now.",
                )
                continue

            runtime_df.sort_values(by=["datetime"], ascending=[True], inplace=True)
            runtime_df.reset_index(drop=True, inplace=True)

            output_df = clean_and_interpolate(runtime_df, tag_code_dict)
            output_df_list.append(output_df)

        final_runtime_df = pd.concat(output_df_list)  # Concat all output data frames.
        if (
            len(final_runtime_df) <= 0
        ):  # If runtime data is empty after processing, return.
            self.logger.write("error", "Runtime data empty after processing.")
            return

        final_runtime_df.reset_index(drop=True, inplace=True)
        # Remove time zone from datetime.
        final_runtime_df["datetime"] = final_runtime_df["datetime"].dt.tz_localize(None)

        # ID is md5 of datetime and asset_code values.
        final_runtime_df.insert(0, "id", "0")
        final_runtime_df["id"] = (
            final_runtime_df["datetime"].astype(str) + final_runtime_df["asset_code"]
        )
        final_runtime_df["id"] = (
            final_runtime_df["id"].swifter.progress_bar(False).apply(generate_id)
        )

        # Remove duplicates from id column and store into property.
        final_runtime_df.drop_duplicates(subset=["id"], keep="last", inplace=True)
        final_runtime_df.sort_values(
            by=["datetime", "asset_code"], ascending=[True, True], inplace=True
        )
        final_runtime_df.reset_index(drop=True, inplace=True)
        self.runtime_df = final_runtime_df
        self.logger.write(
            "info", "Runtime interpolated and stored into processing class."
        )

    def insert_and_update(self, lock):  # Upload data to database.
        if len(self.runtime_df) <= 0:
            self.logger.write("error", "No processed runtime data at all.")
            return

        runtime_df = self.runtime_df  # Data to be inserted.
        # Latest timestamp of processed runtime data.
        latest_timestamp = (
            runtime_df["datetime"].max().isoformat(sep=" ", timespec="seconds")
        )

        runtime_df["last_7_day_avg"] = 0  # Last 7-day average is 0 in acquisition.
        # String of time of upload into database.
        runtime_df["load_date"] = (
            datetime.datetime.now(tz=ZoneInfo(TIMEZONE))
            .replace(tzinfo=None)
            .isoformat(sep=" ", timespec="seconds")
        )
        runtime_df["status"] = 0  # Status of app execution.
        runtime_df["datetime"] = runtime_df["datetime"].astype(
            str
        )  # Turn timestamps to string.

        runtime_df_col_names = ", ".join(
            list(runtime_df.columns)
        )  # Column names in runtime data.
        # A string that represents SQL update syntax.
        update_str = ", ".join(
            [
                "".join([column, " = VALUES(", column, ")"])
                for column in list(runtime_df.columns)
            ]
        )

        self.logger.write("debug", "MySQL connection ready.")
        connection = engine.connect()

        for i, row in runtime_df.iterrows():
            # For each row, put all values into a tuple and apply int() to int columns.
            values_tuple = (
                row["id"],
                row["datetime"],
                row["asset_code"],
                int(row["runtime"]),
                int(row["last_7_day_avg"]),
                row["load_date"],
                int(
                    row["status"],
                ),
            )
            # Use insert on duplicate key to prevent duplicates from entering database.
            query = (
                """INSERT INTO ts_air_compressor_runtime ("""
                + runtime_df_col_names
                + """)
                       VALUES """
                + str(values_tuple)
                + """
                       ON DUPLICATE KEY UPDATE """
                + update_str
                + """;"""
            )
            query = query.replace(
                "None", "Null"
            )  # Replace None with Null to match MySQL syntax.

            try:  # Insert record into database.
                connection.execute(text(query))
                connection.commit()

            except Exception as e:  # If insertion fails.
                self.logger.write("error", f"Runtime data insertion failed: {e}.")

        connection.close()
        self.logger.write("info", "Runtime data attempted database insertion.")
        self.logger.write("debug", "MySQL connection closed.")

        update_backup_time(
            latest_timestamp, lock
        )  # Update last processed storage time.
