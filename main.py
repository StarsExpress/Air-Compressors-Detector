from air_compressor_pm_app import Application
from logger import Logger
import threading


if __name__ == '__main__':
    logger = Logger.get_instance()
    lock = threading.Lock()

    app = Application(logger)  # Air compressor predictive maintenance application.
    app.execute(lock)
