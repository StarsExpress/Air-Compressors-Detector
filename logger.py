import os
import logging
from pathlib import Path
from config import DATA_ACQUISITION_BASE_PATH, DEFAULT_LOG_LEVEL


class Logger:
    """Write logs."""

    __instance = None

    @staticmethod
    def get_instance():  # Static access method.
        if Logger.__instance is None:
            Logger()
        return Logger.__instance

    def __init__(self):  # Set up basic config.
        log_txt_path = os.path.join(
            DATA_ACQUISITION_BASE_PATH, "log", "task_1_ac_data_acquisition.log"
        )
        file = Path(log_txt_path)  # In case if log.txt is not already existing.
        file.touch(exist_ok=True)

        logging.basicConfig(
            filename=log_txt_path,
            format="%(asctime)s: %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            encoding="utf-8",
            level=DEFAULT_LOG_LEVEL,
        )
        self.logger = logging.getLogger()  # Create logger and store it as property.

        if Logger.__instance is not None:  # Virtually private constructor.
            raise Exception("This class is a singleton!")

        else:
            Logger.__instance = self

    def write(self, level, message):  # Write messages into txt.
        if level == "critical":  # Critical level.
            self.logger.critical(message)

        elif level == "error":  # Error level.
            self.logger.error(message)

        elif level == "warning":  # Warning level.
            self.logger.warning(message)

        elif level == "info":  # Info level.
            self.logger.info(message)

        else:  # Debug level.
            self.logger.debug(message)
