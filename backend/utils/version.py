from datetime import datetime
from logs.loggers.logger import logger_config
logger = logger_config(__name__)
import os


def get_version_and_build(version_file='version') -> tuple:
    x, y, z = 0, 0, 0  # Default starting version if file doesn't exist
    build_time = ""

    # Check if the file exists and read the current version and build time
    if os.path.exists(version_file):
        with open(version_file, 'r') as file:
            lines = file.readlines()
            if len(lines) >= 2:
                x, y, z = map(int, lines[0].strip().split('.'))
                build_time = lines[1].strip()

                # Convert the build time from 'YYYYMMDDHHMMSS' to a human-readable format
                try:
                    build_datetime = datetime.strptime(build_time, '%Y%m%d%H%M%S')
                    build_time = build_datetime.strftime('%B %d, %Y %I:%M:%S %p')
                    logger.info(f"Version: {x}.{y}.{z}")
                    logger.info(f"Build: {build_time}")
                except ValueError:
                    # Handle case where build_time format is incorrect
                    build_time = "Invalid build time format"
                    logger.error(f"Invalid build time format: {build_time}")

    return f'{x}.{y}.{z}', build_time