from config import RETENTION_LENGTH
from dao import runtime_dao
import datetime


class TimeSeriesRetainer:
    """Delete runtime data that are too old."""

    def __init__(self, logger):
        self.logger = logger  # Set up logger.

        # Retention lower bound: data prior to it will be deleted.
        self.lower_bound_str = str(datetime.date.today() - datetime.timedelta(days=RETENTION_LENGTH)) + ' 00:00:00'

    def delete(self):
        # Delete runtime data that are too old from database.
        runtime_dao.delete(self.lower_bound_str, self.logger)


if __name__ == '__main__':
    from logger import Logger

    logger_main = Logger.get_instance()

    retainer = TimeSeriesRetainer(logger_main)  # Time series retainer.
    retainer.delete()  # Delete time series data that are too old.
