import datetime
import json
import math
import os
import time
import psutil
import yaml
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from app.core.config import settings
from logs.loggers.logger import logger_config
logger = logger_config(__name__)


def imitate_get_users_api(resp_fn: str, time_param: str):
    valid_time_params = ["daily", "weekly", "monthly", "total"]
    
    # Check if the provided time_param is one of the valid options
    if time_param not in valid_time_params:
        # Raise HTTP 404 error with a custom message for invalid time_param
        logger.error(f"HTTP 404 error, invalid time_param: {time_param}")
        raise HTTPException(status_code=404, detail=f"Invalid 'time_param': {time_param}")
    
    # Construct the file path where the data is expected to be located
    resp_fp = os.path.join("testing_data", "get_users", time_param, resp_fn)
    
    # Check if the constructed file path exists
    if not os.path.exists(resp_fp):
        # Raise HTTP 404 error with a custom message for file not found
        logger.error(f"HTTP 404 error, Filepath does not exist: {resp_fp}")
        raise HTTPException(status_code=404, detail=f"Filepath does not exist: {resp_fp}")
    
    # Open the file at the constructed file path in read mode
    with open(resp_fp, "r") as f:
        # Load and parse the JSON data from the file
        data = json.load(f)
    
    # Return the parsed data as a JSON response
    return JSONResponse(content=data)


def imitate_get_positions_by_trader_id(resp_fn: str, trader_id: str):
    resp_fp = os.path.join("testing_data", "get_positions_by_trader_id", trader_id, resp_fn)
    if not os.path.exists(resp_fp):
        logger.error(f"HTTP 404 error, Filepath does not exist: {resp_fp}")
        raise HTTPException(status_code=404, detail=f"Filepath does not exist: {resp_fp}")
    
    with open(resp_fp, "r") as f:
        data = json.load(f)
        
    return JSONResponse(content=data)


def check_key_in_list_of_dicts(key, list_of_dicts):
    for dictionary in list_of_dicts:
        if key in dictionary:
            return True
    return False


def round_to_same_format(from_num, to_num):
    if isinstance(to_num, int):
        rounded_from_num = round(from_num)
    else:
        decimal_places = len(str(to_num).split('.')[-1])
        rounded_from_num = float(round(from_num, decimal_places))

    return rounded_from_num


def calc_timestamp_diff_in_s(timestamp: int):
    current_timestamp = int(str(time.time()).replace(".", "")[:13])
    timestamp_diff = current_timestamp - timestamp
    timestamp_diff_in_s = timestamp_diff // 1000
    
    return timestamp_diff_in_s


def convert_amount(user_amount, min_qty: float, step_size: float, entry_price: float):  
    # Calculate the ceiling and floor values
    ceil_value = math.ceil(user_amount / step_size) * step_size
    floor_value = math.floor(user_amount / step_size) * step_size
    
    # Choose the closest value to the user amount
    if abs(user_amount - ceil_value) < abs(user_amount - floor_value):
        corrected_amount = ceil_value
    else:
        corrected_amount = floor_value
    
    # Ensure the corrected amount is not less than the minimum quantity
    if corrected_amount < min_qty:
        corrected_amount = min_qty

    if (corrected_amount * entry_price) < 5:
        corrected_amount = ceil_value
    
    return corrected_amount


class DotDict(dict):
    """Custom dictionary that supports both dot and bracket notation."""
    
    def __getattr__(self, attr):
        value = self.get(attr)
        if isinstance(value, dict):
            return DotDict(value)
        return value

    def __setattr__(self, key, value):
        if isinstance(value, dict):
            value = DotDict(value)
        self[key] = value

    def __delattr__(self, key):
        del self[key]


def load_config_from_yaml():
    """
    Load configuration data from a YAML file.

    Returns:
        DotDict: The loaded configuration data as a DotDict object.
                 If the file does not exist or is empty, an empty DotDict will be returned.
    """
    file_path = settings.CONFIG_FILE
    try:
        with open(file_path, "r") as file:
            logger.info("Loading configs...")
            config_data = yaml.safe_load(file)
            if config_data is None:
                config_data = {}
            # Convert to DotDict for dot notation access
            config_data = DotDict(config_data)
    except FileNotFoundError as e:
        config_data = DotDict()
        logger.error(f"Error occurred while loading the YAML file: {e}")
    except Exception as e:
        config_data = DotDict()
        logger.error(f"Error occurred while loading the YAML file: {e}")
        
    return config_data


def is_command_running(command_to_find: str):
    for q in psutil.process_iter():
        if q.name().startswith("python"):
            full_script_command = " ".join(q.cmdline())
            if full_script_command == command_to_find:
                return True
    return False


def is_valid_time_to_update_top_traders():
    # Define your time window boundaries in UTC
    start_time = datetime.time(0, 5)  # 00:05 UTC
    end_time = datetime.time(0, 35)   # 00:35 UTC

    # Get the current time in UTC
    current_time_utc = datetime.datetime.utcnow().time()

    # Check if the current time is within the time window (30 minutes in this case)
    if start_time <= current_time_utc <= end_time:
        # Call your function here
        # Example: your_function_name()
        return True
    else:
        return False
    
    
def calc_perc_diff_between_x_y(x: float, y: float):
    abs_diff = abs(x - y)
    abs_diff_in_perc = (abs_diff / max([x, y])) * 100
    return abs_diff_in_perc
