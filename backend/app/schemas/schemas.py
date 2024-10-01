from typing import Optional
from uuid import UUID
import mysql.connector
from app.core.config import settings
from logs.loggers.logger import logger_config
logger = logger_config(__name__)
import os


class SCHEMAS:
    @staticmethod
    def execute_sql_from_file(connection, table_name):
        """
        Reads the SQL file corresponding to the table name and executes it.
        
        Args:
            connection (mysql.connector.connection): The MySQL connection object.
            table_name (str): The name of the table, which corresponds to the SQL file name.
        """
        # Construct the filename based on the table name
        filename = f"{settings.SQL_PATH}/{table_name}.sql"

        # Check if the file exists
        if not os.path.isfile(filename):
            logger.error(f"Error: {filename} does not exist.")
            return

        # Read the SQL file
        with open(filename, 'r') as sql_file:
            sql_script = sql_file.read()

        # Create a cursor object to execute the SQL
        cursor = connection.cursor()
        
        try:
            # Execute the SQL script
            cursor.execute(sql_script)
            logger.info(f"Table {table_name} created successfully from {filename}.")
            
            # Commit the changes
            connection.commit()
        except mysql.connector.Error as err:
            logger.error(f"Error: {err}")
        finally:
            cursor.close()
        
        
    @staticmethod      
    def read_sql_file(sql_name: str,
                      table_name: Optional[str] = None,
                      format: Optional[bool] = False) -> str:
        """
        Reads the SQL file corresponding to the table name and returns its content.
        
        Args:
            table_name (str): The name of the table, which corresponds to the SQL file name.
        
        Returns:
            str: The SQL script read from the file.
        """
        
        # Construct the filename based on the table name
        filename = f"{settings.SQL_PATH}/{sql_name}.sql"

        # Check if the file exists
        if not os.path.isfile(filename):
            logger.error(f"Error: {filename} does not exist.")
            raise FileNotFoundError(f"Error: {filename} does not exist.")
        
        # Read the SQL file and return its content
        with open(filename, 'r') as sql_file:
            sql_script = sql_file.read()
        
        if format:
            return sql_script.format(table_name=table_name)

        return sql_script
