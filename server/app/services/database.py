from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from app.core.config import settings
from sqlalchemy import create_engine, text, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from logs.loggers.logger import logger_config
logger = logger_config(__name__)

# Database connection strings
SQLALCHEMY_DATABASE_URL_NO_DB = f"mysql+mysqlconnector://{settings.MYSQL_DB_USER}:{settings.MYSQL_DB_PASSWORD}@{settings.MYSQL_DB_HOST}:{settings.MYSQL_DB_PORT}"
SQLALCHEMY_DATABASE_URL_WITH_DB = f"mysql+mysqlconnector://{settings.MYSQL_DB_USER}:{settings.MYSQL_DB_PASSWORD}@{settings.MYSQL_DB_HOST}:{settings.MYSQL_DB_PORT}/{settings.MYSQL_DB}"

def create_database_if_not_exists():
    engine_no_db = create_engine(SQLALCHEMY_DATABASE_URL_NO_DB)
    with engine_no_db.connect() as connection:
        try:
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {settings.MYSQL_DB}"))
            logger.debug(f"Database '{settings.MYSQL_DB}' checked/created.")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database: {e}")
            raise

def create_tables_if_not_exist():
    engine_with_db = create_engine(SQLALCHEMY_DATABASE_URL_WITH_DB)
    Base.metadata.create_all(bind=engine_with_db)
    logger.debug("Tables checked/created.")