from air_compressor_meta_processing import AirCompressorMetaProcessor
from runtime_processing import RuntimeProcessor
from time_series_retention import TimeSeriesRetainer
from logger import Logger
import threading


if __name__ == '__main__':
    logger = Logger.get_instance()
    lock = threading.Lock()

    meta_processor = AirCompressorMetaProcessor(logger)  # Air compressor meta processor.
    meta_processor.truncate_and_upload(lock)  # Upload data after truncation.

    runtime_processor = RuntimeProcessor(logger)  # Runtime processor.
    runtime_processor.process(lock)  # Process runtime data.
    runtime_processor.insert_and_update(lock)  # Upload runtime data.

    retainer = TimeSeriesRetainer(logger)  # Time series retainer.
    retainer.delete()  # Delete time series data that are too old.
