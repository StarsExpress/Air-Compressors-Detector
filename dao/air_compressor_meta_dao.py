from dao.db_connector import Base, DBSession
from sqlalchemy import Column, String, text
import pandas as pd


class AirCompressorDao(Base):
    """Dao of air_compressor_meta_data table."""

    __tablename__ = 'air_compressor_meta_data'
    id = Column(String, primary_key=True)
    name = Column(String)  # For each location: air_compressor_1, to air_compressor_n.
    asset_code = Column(String)
    location = Column(String)


def read_air_compressor_meta(logger=None):  # Read air compressor meta data.
    if logger is not None:
        logger.write('debug', 'MySQL connection ready.')

    session = DBSession()
    results = session.query(AirCompressorDao).all()
    session.close()

    if logger is not None:
        logger.write('info', 'Air compressor meta data query successful.')
        logger.write('debug', 'MySQL connection closed.')

    meta_data_values_list = []
    if results is not None:
        for result in results:
            meta_data_values_list.append([result.name, result.asset_code, result.location])

    return pd.DataFrame(meta_data_values_list, columns=['name', 'asset_code', 'location'])


def truncate_and_insert(data_frame, logger=None):  # Insert air compressor meta data into database.
    if logger is not None:
        logger.write('debug', 'MySQL connection ready.')

    session = DBSession()
    # Truncate air_compressor_meta_data table first.
    session.execute(text('''TRUNCATE air_compressor_meta_data;'''))
    # Insert all air compressor information.
    session.bulk_insert_mappings(AirCompressorDao, data_frame.to_dict(orient='records'))
    session.commit()
    session.close()

    if logger is not None:
        logger.write('info', 'Air compressor meta data attempted database insertion.')
        logger.write('debug', 'MySQL connection closed.')
