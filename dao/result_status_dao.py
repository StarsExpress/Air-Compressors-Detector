from sqlalchemy import Column, DateTime, String, Integer, text
import pandas as pd
from config import DF_ALIGNMENT, UNSENT_RESULT_STATUS_COLUMNS_LIST
from dao.db_connector import Base, engine, DBSession


class ResultStatusDao(Base):
    """Dao of ts_air_compressor_result_status table."""

    __tablename__ = "ts_air_compressor_result_status"
    id = Column(String, primary_key=True)
    datetime = Column(DateTime)
    asset_code = Column(String)
    tag_name = Column(String)
    result_status = Column(String)
    push_status = Column(Integer)
    load_date = Column(DateTime)


def insert_and_update(result_status_df, load_date_str, logger=None):
    if len(result_status_df) <= 0:  # If result status data is empty.
        if logger is not None:
            logger.write("debug", "Result status data empty.")
        return

    result_status_df_str = result_status_df.to_string(justify=DF_ALIGNMENT, index=False)
    result_status_df_str = result_status_df_str.replace("\n", "\n\t")
    if logger is not None:
        logger.write("debug", f"Abnormal clusters data\n{result_status_df_str}")

    result_status_df["datetime"] = result_status_df["datetime"].astype(
        str
    )  # Turn timestamps to string.
    result_status_df["push_status"] = 0  # Default value of push status column.
    result_status_df["load_date"] = load_date_str
    result_status_df_col_names = ", ".join(
        list(result_status_df.columns)
    )  # Column names in result status data.

    # String that represents SQL update syntax.
    update_str = ", ".join(
        ["".join([l, " = VALUES(", l, ")"]) for l in list(result_status_df.columns)]
    )

    if logger is not None:
        logger.write("debug", "MySQL connection ready.")
    connection = engine.connect()

    for (
        i,
        row,
    ) in result_status_df.iterrows():  # For each row, put all values into a tuple.
        values_tuple = (
            row["id"],
            row["datetime"],
            row["asset_code"],
            row["tag_name"],
            row["result_status"],
            int(row["push_status"]),
            row["load_date"],
        )

        # Use insert on duplicate key to prevent duplicates from entering database.
        query = (
            """INSERT INTO ts_air_compressor_result_status ("""
            + result_status_df_col_names
            + """)
                   VALUES """
            + str(values_tuple)
            + """
                   ON DUPLICATE KEY UPDATE """
            + update_str
            + """;"""
        )
        query = query.replace(
            "None", "Null"
        )  # Replace None with Null to match MySQL syntax.

        try:  # Insert records into database.
            connection.execute(text(query))
            connection.commit()

        except Exception as e:  # If insertion fails.
            if logger is not None:
                logger.write("error", f"Result status data insertion failed: {e}.")

    connection.close()
    if logger is not None:
        logger.write("info", "Result status data attempted database insertion.")
        logger.write("debug", "MySQL connection closed.")


def read_unsent_result_status(logger=None):
    if logger is not None:
        logger.write("debug", "MySQL connection ready.")

    session = DBSession()  # Unsent result status filtering criterion: push status = 0.
    unsent_results = (
        session.query(
            ResultStatusDao.datetime,
            ResultStatusDao.tag_name,
            ResultStatusDao.asset_code,
            ResultStatusDao.result_status,
        )
        .filter(ResultStatusDao.push_status == 0)
        .all()
    )
    session.close()

    if logger is not None:
        logger.write("debug", "MySQL connection closed.")

    unsent_result_status_values_list = []
    if unsent_results is not None:
        for unsent_result in unsent_results:
            unsent_result_status_values_list.append(
                [
                    unsent_result.datetime,
                    unsent_result.tag_name,
                    unsent_result.asset_code,
                    unsent_result.result_status,
                ]
            )

    unsent_result_status_df = pd.DataFrame(
        unsent_result_status_values_list, columns=UNSENT_RESULT_STATUS_COLUMNS_LIST
    )
    if (
        len(unsent_result_status_df) <= 0
    ):  # If unsent result status data is empty, return empty data frame.
        if logger is not None:
            logger.write("warning", "Unsent result status data empty.")
        return pd.DataFrame()

    if logger is not None:
        logger.write("info", "Unsent result status data query successful.")

    # Return sorted unsent result status data.
    unsent_result_status_df.sort_values(
        by=["Time", "Tag Name"], ascending=[True, True], inplace=True
    )
    return unsent_result_status_df


def update_push_status(sent_result_status_id_list, logger=None):
    if logger is not None:
        logger.write("debug", "MySQL connection ready.")

    # Update push status column of ts_air_compressor_result_status that are just sent by email.
    session = DBSession()
    session.query(ResultStatusDao).filter(
        ResultStatusDao.id.in_(sent_result_status_id_list)
    ).update({"push_status": 1})
    session.commit()
    session.close()

    if logger is not None:
        logger.write("debug", "MySQL connection closed.")
        logger.write("info", "Sent air compressor alarms' push status updated.")
