from sklearn.cluster import DBSCAN
import pandas as pd
import numpy as np
from config import DF_ALIGNMENT, DBSCAN_PARAMETERS_DICT, RUNTIME_LEVEL


class DBSCANClusterer:
    """DBSCAN clustering on air compressors' hourly runtime deviation."""

    def __init__(self, runtime_data, logger=None):
        self.logger = logger  # Set up logger.

        if self.logger is not None:
            self.logger.write(
                "debug", "Air compressor meta data stored into clustering class."
            )

        runtime_data["runtime"] /= 60  # Turn seconds into minutes.
        runtime_data["last_7_day_avg"] = runtime_data["last_7_day_avg"] / 60
        self.runtime_df = runtime_data  # Store runtime data.
        if self.logger is not None:
            self.logger.write("debug", "Runtime data stored into clustering class.")

        self.abnormal_cluster_df = (
            pd.DataFrame()
        )  # Data frame for abnormal cluster data.

        # Use locations as keys to find matching parameters, and set up DBSCAN clusterer accordingly.
        self.dbscan_clusterers_dict = dict()
        for key in DBSCAN_PARAMETERS_DICT.keys():
            clusterer = DBSCAN(
                min_samples=DBSCAN_PARAMETERS_DICT[key]["minimal_samples"],
                eps=DBSCAN_PARAMETERS_DICT[key]["epsilon"],
                n_jobs=-1,
            )

            self.dbscan_clusterers_dict.update({key: clusterer})

    def predict_cluster(self, location, runtime_df):
        # Reshape deviation for DBSCAN clustering.
        deviation_reshaped = np.array(runtime_df["deviation"]).reshape(-1, 1)

        # Predict clusters.
        runtime_df["cluster"] = (
            self.dbscan_clusterers_dict[location].fit_predict(deviation_reshaped) + 1
        )
        runtime_df["cluster"].clip(
            upper=2, inplace=True
        )  # Clusters: 0 = outlier, 1 = majority and 2 = minority.

        # Minority and outlier clusters are only applied to positive deviations.
        runtime_df.loc[runtime_df["deviation"] <= 0, "cluster"] = 1
        # Runtime <= threshold is automatically labeled as majority.
        runtime_df.loc[runtime_df["runtime"] <= RUNTIME_LEVEL, "cluster"] = 1

        return runtime_df

    def fit_and_predict(self, meta_data):
        asset_codes_list = sorted(
            set(self.runtime_df["asset_code"])
        )  # List of asset codes in runtime data.
        if len(asset_codes_list) <= 0:  # If asset codes list is empty.
            if self.logger is not None:
                self.logger.write(
                    "error",
                    "DBSCAN clustering stopped: air compressor asset codes list empty.",
                )
            print("DBSCAN clustering stopped: air compressor asset codes list empty.")
            return

        runtime_df = self.runtime_df
        # Deviation = runtime - last 7-day average.
        runtime_df["deviation"] = runtime_df["runtime"] - runtime_df["last_7_day_avg"]

        cluster_df_list = []  # List of cluster data for air compressors.

        for (
            asset_code
        ) in asset_codes_list:  # Iterate through air compressors' asset codes.
            # If iterated asset code is not found in meta data, continue to next iteration.
            if asset_code not in list(meta_data["asset_code"]):
                continue

            location_iter = meta_data.loc[
                meta_data["asset_code"] == asset_code, "location"
            ].iloc[0]
            # If found location is neither IV nor MSR_2, continue to next iteration.
            if location_iter not in ["IV", "MSR_2"]:
                continue

            runtime_df_iter = runtime_df[runtime_df["asset_code"] == asset_code]
            cluster_df_iter = self.predict_cluster(location_iter, runtime_df_iter)
            cluster_df_list.append(cluster_df_iter)

        cluster_df = pd.concat(
            cluster_df_list, axis=0
        )  # Data frame of appended clustering results.
        del cluster_df_list  # Delete cluster data list from memory.

        runtime_df = runtime_df[
            runtime_df["status"] == 0
        ]  # Only keep runtime data with status = 0 after DBSCAN.

        # Inner join cluster column into runtime data and save into property.
        # Only runtime data with status = 0 and conducted DBSCAN will be kept.
        runtime_df = runtime_df.merge(
            cluster_df[["datetime", "asset_code", "cluster"]],
            left_on=["datetime", "asset_code"],
            right_on=["datetime", "asset_code"],
            how="inner",
        )
        self.runtime_df = runtime_df

        # Store abnormal clusters data (cluster != 1) into property.
        abnormal_cluster_df = runtime_df[runtime_df["cluster"] != 1]
        abnormal_cluster_df.sort_values(
            by=["datetime", "asset_code"], ascending=[True, True], inplace=True
        )
        abnormal_cluster_df.reset_index(drop=True, inplace=True)
        self.abnormal_cluster_df = abnormal_cluster_df

        if self.logger is not None:
            self.logger.write("info", "Air compressors clustering completed.")
            self.logger.write(
                "debug", "Clustering results stored into clustering class."
            )
            log_message = self.abnormal_cluster_df.to_string(
                justify=DF_ALIGNMENT, index=False
            )
            self.logger.write("debug", f"Abnormal clusters\n{log_message}")
