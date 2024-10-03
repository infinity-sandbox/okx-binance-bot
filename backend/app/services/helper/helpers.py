import datetime
import json
import math
import os
import time
import psutil
import yaml
from loguru import logger
from telethon.sync import TelegramClient


def imitate_get_users_api(resp_fn: str, time_param: str):
    valid_time_params = ["daily", "weekly", "monthly", "total"]
    if time_param not in valid_time_params:
        return {"status": 404, "message": f"Invalid 'time_param': {time_param}"}
    
    resp_fp = os.path.join("testing_data", "get_users", time_param, resp_fn)
    if not os.path.exists(resp_fp):
        return {"status": 404, "message": f"Filepath does not exist: {resp_fp}"}
    
    with open(resp_fp, "r") as f:
        data = json.load(f)
    return data

def imitate_get_positions_by_trader_id(resp_fn: str, trader_id: str):
    resp_fp = os.path.join("testing_data", "get_positions_by_trader_id", trader_id, resp_fn)
    if not os.path.exists(resp_fp):
        return {"status": 404, "message": f"Filepath does not exist: {resp_fp}"}
    
    with open(resp_fp, "r") as f:
        data = json.load(f)
    return data

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

def load_config_from_yaml():
    """
    Load configuration data from a YAML file.

    Returns:
        dict: The loaded configuration data as a dictionary.
              If the file does not exist or is empty, an empty dictionary will be returned.
    """
    file_path = "config.yml"
    try:
        with open(file_path, "r") as file:
            config_data = yaml.safe_load(file)
    except FileNotFoundError:
        config_data = {}
    except Exception as e:
        logger.error(f"Error occurred while loading the YAML file: {e}")
        config_data = {}

    return config_data

def is_command_running(command_to_find: str):
    for q in psutil.process_iter():
        if q.name().startswith("python"):
            full_script_command = " ".join(q.cmdline())
            if full_script_command == command_to_find:
                return True
    return False


def is_valid_time_to_update_top_traders():
    # Get the current time in UTC
    current_time_utc = datetime.datetime.utcnow().time()
    
    # Set your desired time window boundaries (e.g., 30 minutes window)
    start_time = (datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).time()  # 1 minutes ago
    end_time = (datetime.datetime.utcnow() + datetime.timedelta(minutes=1)).time()   # 1 minutes in the future

    # Check if the current time is within the time window
    if start_time <= current_time_utc <= end_time:
        return True
    else:
        return False

    
def calc_perc_diff_between_x_y(x: float, y: float):
    abs_diff = abs(x - y)
    abs_diff_in_perc = (abs_diff / max([x, y])) * 100
    return abs_diff_in_perc