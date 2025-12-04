from sqlalchemy import Column, DateTime, String, Integer
import datetime
import pandas as pd
from config import RUNTIME_COLUMNS_LIST, ADDITIONAL_RUNTIME_DAYS
from dao.db_connector import Base, DBSession


class RuntimeDao(Base):
    """Dao of ts_air_compressor_runtime table."""

    __tablename__ = "ts_air_compressor_runtime"
    id = Column(String, primary_key=True)
    datetime = Column(DateTime)
    asset_code = Column(String)
    runtime = Column(Integer)  # Unit: seconds.
    last_7_day_avg = Column(Integer)  # Unit: seconds.
    load_date = Column(DateTime)
    status = Column(Integer)


def read_runtime(logger=None):
    if logger is not None:
        logger.write("debug", "MySQL connection ready.")

    session = DBSession()  # Unprocessed runtime filtering criterion: status = 0.
    results_unprocessed = (
        session.query(
            RuntimeDao.id,
            RuntimeDao.datetime,
            RuntimeDao.asset_code,
            RuntimeDao.runtime,
            RuntimeDao.status,
        )
        .filter(RuntimeDao.status == 0)
        .all()
    )
    session.close()

    if logger is not None:
        logger.write("debug", "MySQL connection closed.")

    unprocessed_runtime_values_list = []
    if results_unprocessed is not None:
        for result in results_unprocessed:
            unprocessed_runtime_values_list.append(
                [
                    result.id,
                    result.datetime,
                    result.asset_code,
                    result.runtime,
                    result.status,
                ]
            )

    unprocessed_runtime_df = pd.DataFrame(
        unprocessed_runtime_values_list, columns=RUNTIME_COLUMNS_LIST
    )
    if (
        len(unprocessed_runtime_df) <= 0
    ):  # If unprocessed runtime data is empty, return empty data frame.
        if logger is not None:
            logger.write("warning", "Unprocessed runtime data empty.")
        return pd.DataFrame()

    if logger is not None:
        logger.write("info", "Runtime data query successful.")

    # List of asset codes in unprocessed run time data.
    asset_codes_list = list(set(unprocessed_runtime_df["asset_code"]))
    # Threshold = 365 days before the earliest unprocessed timestamp.
    datetime_threshold = str(
        min(unprocessed_runtime_df["datetime"])
        - datetime.timedelta(days=ADDITIONAL_RUNTIME_DAYS)
    )

    if logger is not None:
        logger.write("debug", "MySQL connection ready.")

    session = DBSession()
    # Past runtime filtering criterion: status = 1, asset codes in list and not earlier than threshold.
    results_past = (
        session.query(
            RuntimeDao.id,
            RuntimeDao.datetime,
            RuntimeDao.asset_code,
            RuntimeDao.runtime,
            RuntimeDao.status,
        )
        .filter(
            (RuntimeDao.status == 1)
            & (RuntimeDao.asset_code.in_(asset_codes_list))
            & (RuntimeDao.datetime >= datetime_threshold)
        )
        .all()
    )
    session.close()

    if logger is not None:
        logger.write("info", "Past runtime data query successful.")
        logger.write("debug", "MySQL connection closed.")

    past_runtime_values_list = []
    if results_past is not None:
        for result in results_past:
            past_runtime_values_list.append(
                [
                    result.id,
                    result.datetime,
                    result.asset_code,
                    result.runtime,
                    result.status,
                ]
            )

    past_runtime_df = pd.DataFrame(
        past_runtime_values_list, columns=RUNTIME_COLUMNS_LIST
    )
    # Combine unprocessed and past runtime data.
    runtime_df = pd.concat([past_runtime_df, unprocessed_runtime_df], axis=0)
    # Return sorted runtime data.
    runtime_df.sort_values(
        by=["datetime", "asset_code"], ascending=[True, True], inplace=True
    )
    return runtime_df


def update_execution_status(runtime_df, load_date_str, logger=None):
    if logger is not None:
        logger.write("debug", "MySQL connection ready.")

    session = DBSession()
    for (
        i,
        row,
    ) in (
        runtime_df.iterrows()
    ):  # Update last-7-day average, load date and status by filtering ID.
        session.query(RuntimeDao).filter(RuntimeDao.id == row["id"]).update(
            {
                "last_7_day_avg": row["last_7_day_avg"],
                "load_date": load_date_str,
                "status": 1,
            }
        )
        session.commit()
    session.close()

    if logger is not None:
        logger.write("debug", "MySQL connection closed.")
        logger.write("info", "Runtime data status updated.")
