import datetime
from zoneinfo import ZoneInfo
from config import TIMEZONE
from dao import air_compressor_meta_dao, runtime_dao
from air_compressor_meta_processing import AirCompressorMetaProcessor
from workflow.moving_avg_calculation_step import CalculationStep
from workflow.dbscan_clustering_step import ClusteringStep
from workflow.upload_result_status_step import UploadStep


class Application:
    """Run predictive maintenance on air compressors."""

    def __init__(self, logger):
        self.logger = logger  # Set up logger.

    def execute(self, lock):
        processing = AirCompressorMetaProcessor(
            self.logger
        )  # Air compressor meta processor.
        processing.truncate_and_upload(lock)  # Upload data after truncation.

        runtime_data = runtime_dao.read_runtime(self.logger)  # Runtime data.
        if len(runtime_data) <= 0:  # If runtime data is empty.
            print("App return: unprocessed runtime data empty.")
            self.logger.write("warning", "App return: unprocessed runtime data empty.")
            return

        # Air compressor meta data.
        air_compressor_meta_data = air_compressor_meta_dao.read_air_compressor_meta(
            self.logger
        )
        if len(air_compressor_meta_data) <= 0:  # If meta data is empty.
            self.logger.write("error", "App error: air compressor meta data empty.")
            return

        self.logger.write(
            "debug", "Step 1: calculate last 7-day average runtime for all records."
        )

        calculation_step = CalculationStep()
        calculation_step.conduct(runtime_data)
        runtime_data = calculation_step.runtime_df
        if (
            len(runtime_data) <= 0
        ):  # If runtime data is empty after last 7-day average calculation.
            print(
                "App return: runtime data empty after last 7-day average calculation."
            )
            self.logger.write(
                "warning",
                "App return: runtime data empty after last-7-day average calculation.",
            )
            return

        self.logger.write(
            "debug",
            "Step 2: apply DBSCAN clustering to pick out extreme positive points.",
        )

        clustering_step = ClusteringStep()
        clustering_step.conduct(
            air_compressor_meta_data, runtime_data
        )  # Do DBSCAN clustering.
        runtime_data = clustering_step.runtime_df  # Clustered runtime data.

        runtime_data["last_7_day_avg"] *= 60  # Turn minutes into seconds.

        # String of time of upload into database.
        load_date_str = (
            datetime.datetime.now(tz=ZoneInfo(TIMEZONE))
            .replace(tzinfo=None)
            .isoformat(sep=" ", timespec="seconds")
        )
        runtime_dao.update_execution_status(
            runtime_data, load_date_str
        )  # Update runtime data status.

        abnormal_cluster_data = clustering_step.abnormal_cluster_df
        if len(abnormal_cluster_data) <= 0:
            self.logger.write("info", "App return: no abnormal clusters found.")
            return

        self.logger.write(
            "debug", "Step 3: insert and update abnormal clusters from step 2."
        )

        upload_step = UploadStep()
        upload_step.conduct(abnormal_cluster_data, load_date_str)
        self.logger.write("info", "App return: all steps completed for runtime data.")


if __name__ == "__main__":
    import threading
    from logger import Logger

    logger_main = Logger.get_instance()
    lock_main = threading.Lock()

    app = Application(logger_main)  # Air compressor predictive maintenance application.
    app.execute(lock_main)
