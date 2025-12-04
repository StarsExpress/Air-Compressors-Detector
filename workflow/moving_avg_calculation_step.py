import pandas as pd
from joblib import Parallel, delayed
from utils.last_7_day_average import calculate_last_7_day_avg
from logger import Logger


class CalculationStep:
    """Do step 1: calculate last-7-day avg runtime for each tuple of timestamp and asset code."""

    def __init__(self):
        self.logger = Logger.get_instance()  # Set up logger.

        self.runtime_df = (
            pd.DataFrame()
        )  # Data frame for runtime data after moving average calculation.

    def conduct(self, runtime_data):
        asset_codes_list = sorted(
            set(runtime_data["asset_code"])
        )  # List of asset codes.
        runtime_df_list = Parallel(n_jobs=-1, prefer="threads")(
            delayed(calculate_last_7_day_avg)(
                runtime_data[runtime_data["asset_code"] == asset_code], self.logger
            )
            for asset_code in asset_codes_list
        )

        # Store calculated runtime data as property.
        runtime_df = pd.concat(runtime_df_list, axis=0)
        runtime_df.sort_values(
            by=["datetime", "asset_code"], ascending=[True, True], inplace=True
        )
        self.runtime_df = runtime_df

        self.logger.write("info", "Moving average calculation step completed.")
