from sqlalchemy import create_engine, QueuePool
from sqlalchemy.orm import declarative_base, sessionmaker
from config import DB_IP, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_SCHEMA, DB_POOL_SIZE, DB_MAX_OVERFLOW


# Base class for object creation.
Base = declarative_base()

# Database connection engine.
engine = create_engine(
    "mysql+pymysql://"
    + DB_USERNAME
    + ":"
    + DB_PASSWORD
    + "@"
    + DB_IP
    + ":"
    + DB_PORT
    + "/"
    + DB_SCHEMA,
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    echo=False,
)

# Database session.
DBSession = sessionmaker(bind=engine)
