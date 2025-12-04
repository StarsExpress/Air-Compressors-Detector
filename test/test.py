from config import APP_BASE_PATH
import os
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal


class TestAlarms(unittest.TestCase):
    """Verify if predicted air compressor alarms are as expected."""

    def test_air_compressor_results(self):
        test_folder = os.path.join(APP_BASE_PATH, 'test')

        expected_alarms_excel_path = os.path.join(test_folder, 'Air Compressor Alarms.xlsx')
        expected_alarms_df = pd.read_excel(expected_alarms_excel_path, sheet_name='Air Compressor Alarms')
        expected_alarms_df['datetime'] = expected_alarms_df['datetime'].astype('datetime64[s]')
        expected_alarms_df['asset_code'] = expected_alarms_df['asset_code'].astype(str)

        predicted_alarms_csv_path = os.path.join(test_folder, 'air_compressor_alarms.csv')
        predicted_alarms_df = pd.read_csv(predicted_alarms_csv_path)
        predicted_alarms_df['datetime'] = predicted_alarms_df['datetime'].astype('datetime64[s]')
        predicted_alarms_df['asset_code'] = predicted_alarms_df['asset_code'].astype(str)

        assert_frame_equal(predicted_alarms_df, expected_alarms_df)


if __name__ == '__main__':
    unittest.main()
