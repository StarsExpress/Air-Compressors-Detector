from config import APP_BASE_PATH, DBSCAN_PARAMETERS_DICT, RUNTIME_LEVEL
from sklearn.cluster import DBSCAN
import os
import pandas as pd
import numpy as np


class DBSCANTester:
    """Test DBSCAN performance on air compressors' runtime data."""

    def __init__(self, runtime_data):
        runtime_data['runtime'] /= 60  # Turn seconds into minutes.
        runtime_data['last_7_day_avg'] = runtime_data['last_7_day_avg'] / 60
        self.runtime_df = runtime_data.drop(columns=['id', 'status'])  # Store runtime data.

        self.abnormal_cluster_df = pd.DataFrame()  # Data frame for abnormal cluster details.

        # Use locations as keys to find matching parameters, and set up DBSCAN clusterer accordingly.
        self.dbscan_clusterers_dict = dict()
        for key in DBSCAN_PARAMETERS_DICT.keys():
            clusterer = DBSCAN(min_samples=DBSCAN_PARAMETERS_DICT[key]['minimal_samples'],
                               eps=DBSCAN_PARAMETERS_DICT[key]['epsilon'], n_jobs=-1)

            self.dbscan_clusterers_dict.update({key: clusterer})

    def predict_cluster(self, location, runtime_df):
        # Reshape deviation for DBSCAN clustering.
        deviation_reshaped = np.array(runtime_df['deviation']).reshape(-1, 1)

        # Predict clusters.
        runtime_df['cluster'] = self.dbscan_clusterers_dict[location].fit_predict(deviation_reshaped) + 1
        runtime_df['cluster'].clip(upper=2, inplace=True)  # Clusters: 0 = outlier, 1 = majority and 2 = minority.

        # Minority and outlier clusters are only applied to positive deviations.
        runtime_df.loc[runtime_df['deviation'] <= 0, 'cluster'] = 1
        # Runtime <= threshold is automatically labeled as majority.
        runtime_df.loc[runtime_df['runtime'] <= RUNTIME_LEVEL, 'cluster'] = 1

        return runtime_df

    def evaluate(self, meta_data, asset_codes_list=None, clustering_summary=True, export_csv=True):
        # If asset codes list is None or not list type, put all asset codes from runtime data into a list.
        if (asset_codes_list is None) | (type(asset_codes_list) is not list):
            asset_codes_list = sorted(set(self.runtime_df['asset_code']))

        if len(asset_codes_list) <= 0:  # If list of asset codes is empty.
            return

        runtime_df = self.runtime_df  # Filter runtime data by asset codes.
        runtime_df = runtime_df[runtime_df['asset_code'].isin(asset_codes_list)]
        # Deviation = runtime - last 7-day average.
        runtime_df['deviation'] = runtime_df['runtime'] - runtime_df['last_7_day_avg']

        cluster_df_list = []  # List of cluster data for air compressors.

        for asset_code in asset_codes_list:  # Iterate through air compressors' asset codes.
            # If iterated asset code is not found in meta data, continue to next iteration.
            if asset_code not in list(meta_data['asset_code']):
                continue

            location_iter = meta_data.loc[meta_data['asset_code'] == asset_code, 'location'].iloc[0]
            # If found location is neither IV nor MSR_2, continue to next iteration.
            if location_iter not in ['IV', 'MSR_2']:
                continue

            runtime_df_iter = runtime_df[runtime_df['asset_code'] == asset_code]
            cluster_df_iter = self.predict_cluster(location_iter, runtime_df_iter)
            cluster_df_list.append(cluster_df_iter)

        cluster_df = pd.concat(cluster_df_list, axis=0)  # Data frame of appended clustering results.
        del cluster_df_list  # Delete cluster data list from memory.

        # Inner join cluster column into runtime data and save into property.
        # Only runtime data with status = 0 and conducted DBSCAN will be kept.
        runtime_df = runtime_df.merge(cluster_df[['datetime', 'asset_code', 'cluster']],
                                      left_on=['datetime', 'asset_code'],
                                      right_on=['datetime', 'asset_code'], how='inner')
        self.runtime_df = runtime_df

        # Store abnormal clusters data (cluster != 1) into property.
        abnormal_cluster_df = runtime_df[runtime_df['cluster'] != 1]
        abnormal_cluster_df.sort_values(by=['datetime', 'asset_code'], ascending=[True, True], inplace=True)
        abnormal_cluster_df.reset_index(drop=True, inplace=True)
        self.abnormal_cluster_df = abnormal_cluster_df

        if clustering_summary:  # When printing a data frame, let index starts from 1 if not empty.
            if len(abnormal_cluster_df) > 0:
                abnormal_cluster_df.index += 1
            print('\n', abnormal_cluster_df)  # Print abnormal clusters.

        if export_csv:  # Save abnormal cluster data as csv.
            self.abnormal_cluster_df.to_csv(os.path.join(
                APP_BASE_PATH, 'test', 'air_compressor_alarms.csv'), index=False)


if __name__ == '__main__':
    from dao import air_compressor_meta_dao, runtime_dao
    from workflow.moving_avg_calculation_step import CalculationStep
    import datetime
    import time

    start = time.time()

    runtime_data_main = runtime_dao.read_runtime()  # Runtime data.

    if len(runtime_data_main) > 0:  # If runtime data exist.
        meta_data_main = air_compressor_meta_dao.read_air_compressor_meta()  # Air compressor meta data.

        # Only use data since 2020.
        datetime_threshold = datetime.datetime(2020, 1, 1, 0, 0, 0)
        runtime_data_main = runtime_data_main[runtime_data_main['datetime'] >= datetime_threshold]

        calculation_step = CalculationStep()
        calculation_step.conduct(runtime_data_main)
        runtime_data_main = calculation_step.runtime_df

        tester = DBSCANTester(runtime_data_main)
        tester.evaluate(meta_data_main)

        end = time.time()
        print('\nTotal running time: ' + str(round(end - start, 2)) + ' seconds.')
