from algorithm.dbscan_clustering import DBSCANClusterer
from logger import Logger
import pandas as pd


class ClusteringStep:
    """Do step 2: apply DBSCAN clustering to pick out extreme positive points."""

    def __init__(self, ):
        self.logger = Logger.get_instance()  # Set up logger.

        self.runtime_df = pd.DataFrame()  # Data frame for runtime data.
        self.abnormal_cluster_df = pd.DataFrame()  # Data frame for abnormal clusters.

    def conduct(self, air_compressor_meta_data, runtime_data):
        clustering = DBSCANClusterer(runtime_data, self.logger)  # DBSCAN clustering.
        clustering.fit_and_predict(air_compressor_meta_data)

        self.runtime_df = clustering.runtime_df  # Store runtime data that conducted DBSCAN.
        self.abnormal_cluster_df = clustering.abnormal_cluster_df  # Store abnormal cluster data as property.
        self.logger.write('info', 'DBSCAN clustering step completed.')
