from datetime import datetime
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
                except ValueError:
                    # Handle case where build_time format is incorrect
                    build_time = "Invalid build time format"

    return f'{x}.{y}.{z}', build_time