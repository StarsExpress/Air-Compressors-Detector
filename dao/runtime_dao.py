from dao.db_connector import Base, DBSession
from sqlalchemy import Column, DateTime, String, Integer


class RuntimeDao(Base):
    """Dao of ts_air_compressor_runtime table."""

    __tablename__ = 'ts_air_compressor_runtime'
    id = Column(String, primary_key=True)
    datetime = Column(DateTime)
    asset_code = Column(String)
    runtime = Column(Integer)  # Unit: seconds.
    last_7_day_avg = Column(Integer)  # Unit: seconds.
    load_date = Column(DateTime)
    status = Column(Integer)


def delete(retention_lower_bound, logger=None):  # Delete runtime data.
    if logger is not None:
        logger.write('debug', 'MySQL connection ready.')

    session = DBSession()
    session.query(RuntimeDao).filter(RuntimeDao.datetime < retention_lower_bound, RuntimeDao.id >= '0').delete()
    session.commit()
    session.close()

    if logger is not None:
        logger.write('info', 'Runtime data attempted deletion of too old records.')
        logger.write('debug', 'MySQL connection closed.')
