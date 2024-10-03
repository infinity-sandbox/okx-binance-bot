import asyncio
import json
import os
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional
import requests
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from loguru import logger
from helper import helpers
import helper.telegram_bot as telegram_bot
from databases.db_manager import DatabaseManager
import mysql.connector

config = helpers.load_config_from_yaml()
db_host = config["db_host"]
db_user = config["db_user"]
db_password = config["db_password"]
database = config["database"]
rapid_api_key = config["rapidapi_api_key"]

class LeaderboardScraper:
    def __init__(self, db):
        self.db = db
        self.base_url = "https://okx-copy-trading1.p.rapidapi.com"
        self.headers = {
            "X-RapidAPI-Key": rapid_api_key,
            "X-RapidAPI-Host": "okx-copy-trading1.p.rapidapi.com"
        }
        self.limiter = AsyncLimiter(10, 1)  # Adjust based on your rate limit requirements
        logger.debug('LeaderboardScraper initialized with base URL and headers.')

    async def bound_fetch(self, url, session, trader_id: str):
        try:
            async with self.limiter:
                async with session.get(url, headers=self.headers) as response:
                    try:
                        if 'application/json' in response.headers.get('content-type', '').lower():
                            response_data = await response.json()
                        else:
                            response_data = await response.text()
                            logger.warning(f"RapidAPI response is not a JSON: {response_data}")
                    except json.JSONDecodeError:
                        logger.error(f"JSON decoding failed for URL: {url}")
                        response_data = "Invalid JSON response"
                    
                    return {"response": response_data, "trader_id": trader_id}
        except asyncio.TimeoutError:
            logger.error(f"Timeout occurred for trader ID: {trader_id}")
            return {"response": "Timeout occurred", "trader_id": trader_id}

    async def fetch_api_urls(self, urls_and_trader_ids):
        tasks = []
        async with ClientSession() as session:
            for dict_i in urls_and_trader_ids:
                url = dict_i["url"]
                trader_id = dict_i["trader_id"]
                task = asyncio.create_task(self.bound_fetch(url, session, trader_id))
                tasks.append(task)
        
            responses = await asyncio.gather(*tasks)
            return responses

    def generate_positions_api_endpoints(self, ignore_trader_ids: list = None, include_observed: bool = False):
        if include_observed:
            observed_trader_ids = self.db.fetch_observed_trader_ids()
        else:
            observed_trader_ids = []
        followed_trader_ids = self.db.fetch_all_followed_trader_ids()
        all_trader_ids = list(set(observed_trader_ids + followed_trader_ids))
        trader_ids_filtered = [trader_id for trader_id in all_trader_ids if trader_id not in ignore_trader_ids]
        positions_api_endpoint_urls_and_trader_ids = [
            {
                "url": f"{self.base_url}/trader/{trader_id}/positions",
                "trader_id": f"{trader_id}"
            }
            for trader_id in trader_ids_filtered
        ]
        return positions_api_endpoint_urls_and_trader_ids
    
    def generate_historical_positions_api_endpoints(self, ignore_trader_ids: list = None, include_observed: bool = False):
        if include_observed:
            observed_trader_ids = self.db.fetch_observed_trader_ids()
        else:
            observed_trader_ids = []
        followed_trader_ids = self.db.fetch_all_followed_trader_ids()
        all_trader_ids = list(set(observed_trader_ids + followed_trader_ids))
        trader_ids_filtered = [trader_id for trader_id in all_trader_ids if trader_id not in ignore_trader_ids]
        positions_api_endpoint_urls_and_trader_ids = [
            {
                "url": f"{self.base_url}/trader/{trader_id}/positions/history",
                "trader_id": f"{trader_id}"
            }
            for trader_id in trader_ids_filtered
        ]
        return positions_api_endpoint_urls_and_trader_ids
    
    def generate_user_statistics_api_endpoint_urls(self, querystring: dict, trader_ids: list = None, ignore_trader_ids: list = None):
        trader_ids_filtered = [trader_id for trader_id in trader_ids if trader_id not in ignore_trader_ids]
        trader_statistics_api_urls_and_trader_ids = [
            {
                "url": f"{self.base_url}/trader/{trader_id}/trade-stats?" + "&".join(
                    [f"{key}={value}" for key, value in querystring.items()]
                ),
                "trader_id": f"{trader_id}"
            }
            for trader_id in trader_ids_filtered
        ]
        return trader_statistics_api_urls_and_trader_ids
    
    def generate_user_api_endpoint_urls(self, trader_ids: list = None, ignore_trader_ids: list = None):
        trader_ids_filtered = [trader_id for trader_id in trader_ids if trader_id not in ignore_trader_ids]
        trader_api_urls_and_trader_ids = [
            {
                "url": f"{self.base_url}/trader/{trader_id}",
                "trader_id": f"{trader_id}"
            }
            for trader_id in trader_ids_filtered
        ]
        return trader_api_urls_and_trader_ids

    def get_positions_from_api(self, ignore_trader_ids: list = None, include_observed: bool = False):
        """
        Returns:
        {
        "trader_id_1": [{pos_1, pos_2, ...}],
        "trader_id_2": [{pos_1, pos_2, ...}]
        }
        """
        all_urls_and_trader_ids = self.generate_positions_api_endpoints(
            ignore_trader_ids=ignore_trader_ids, include_observed=include_observed
        )
        if not all_urls_and_trader_ids:
            logger.debug("We are not following any traders, so no positions to retrieve.")

        # Remove duplicate items
        unique_urls = []
        all_urls_and_trader_ids_unique = []
        for dict_i in all_urls_and_trader_ids:
            if dict_i["url"] not in unique_urls:
                all_urls_and_trader_ids_unique.append(dict_i)
                unique_urls.append(dict_i["url"])

        logger.info("Getting positions from RapidAPI")
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(all_urls_and_trader_ids_unique))
        results = loop.run_until_complete(future)
        
        trader_ids_w_positions = {}
        for dict_i in results:
            response = dict_i["response"]
            if isinstance(response, str):
                return None
            trader_id = dict_i["trader_id"]
            if not response or response.get("message") != "OK":
                logger.warning(f"Unable to get positions for trader ID: {trader_id}")
                logger.warning(f"{response.get('message', 'No message provided')}")
                return None
            
            positions = response.get("data", [])

            # Used for matching API returned keys to 'position' table column keys
            api_key_to_db_key_matcher = {
                "availSubPos": "avail_sub_pos",
                "ccy": "ccy",
                "instId": "inst_id",
                "instType": "inst_type",
                "last": "last",
                "lever": "lever",
                "margin": "margin",
                "markPx": "mark_px",
                "mgnMode": "mgn_mode",
                "notionalUsd": "notional_usd",
                "openAvgPx": "open_avg_px",
                "openTime": "open_time",
                "pnl": "pnl",
                "pnlRatio": "pnl_ratio",
                "posSide": "pos_side",
                "slTriggerPx": "sl_trigger_px",
                "slTriggerType": "sl_trigger_type",
                "subPos": "sub_pos",
                "tpTriggerPx": "tp_trigger_px",
                "tpTriggerType": "tp_trigger_type",
                "tradeItemId": "trade_item_id",
                "uTime": "u_time",
                "traderId": "trader_id",
            }
            
            # Fix received positions from API
            positions_fixed = []
            for position_dict in positions:
                position_dict_fixed = {
                    api_key_to_db_key_matcher.get(key): val
                    for key, val in position_dict.items()
                    if api_key_to_db_key_matcher.get(key)
                }
                position_dict_fixed = {key: val if val != "" else None for key, val in position_dict_fixed.items()}
                position_dict_fixed["trader_id"] = trader_id  # Assign trader_id
                positions_fixed.append(position_dict_fixed)

            trader_ids_w_positions[trader_id] = positions_fixed
        
        return trader_ids_w_positions
    
    def get_historical_positions_from_api(
        self,
        ignore_trader_ids: list = None,
        include_observed: bool = False,
        max_pos_count_per_trader: int = 40
    ):
        """
        Returns:
        {
        "trader_id_1": [{pos_1, pos_2, ...}],
        "trader_id_2": [{pos_1, pos_2, ...}]
        }
        """
        all_urls_and_trader_ids = self.generate_historical_positions_api_endpoints(
            ignore_trader_ids=ignore_trader_ids, include_observed=include_observed
        )
        if not all_urls_and_trader_ids:
            logger.debug("We are not following any traders, so no historical positions to retrieve.")

        # Remove duplicate items
        unique_urls = []
        all_urls_and_trader_ids_unique = []
        for dict_i in all_urls_and_trader_ids:
            if dict_i["url"] not in unique_urls:
                all_urls_and_trader_ids_unique.append(dict_i)
                unique_urls.append(dict_i["url"])

        logger.info("Getting historical positions from RapidAPI")
        all_historical_positions = []
        for url in unique_urls:
            total_pages_to_check = max_pos_count_per_trader // 20
            params = {}
            hist_pos_counter = 0

            for _ in range(total_pages_to_check + 1):
                has_next_page = False

                # Send a GET request to the API
                resp = requests.get(url=url, headers=self.headers, params=params)
        
                # Check if the response status code is 200 (OK)
                if resp.status_code != 200:
                    logger.warning(f"Response status code != 200. Status code: {resp.status_code}")
                    return None  # Return None to indicate failure          

                # Check if the response message is "OK" to ensure a successful fetch
                if resp.json().get("message") == "OK":
                    historical_positions = resp.json().get("data")
                    if len(historical_positions) == 20:
                        has_next_page = True
                else:
                    logger.warning(f"Failed to fetch historical positions from RapidAPI. Error: {resp.json().get('message')}")
                    return None  # Return None to indicate failure
                
                all_historical_positions.extend(historical_positions)
                hist_pos_counter += len(historical_positions)

                if hist_pos_counter >= max_pos_count_per_trader:
                    break
                
                if has_next_page:
                    params = {"after": historical_positions[-1]["tradeItemId"]}
                else:
                    break

        return all_historical_positions
    
    def get_user_statistics(self, trader_ids: list = None, ignore_trader_ids: list = None):
        """
        Returns:
        {
        "trader_id_1": {
            "trader_id": x,
            "date_range": x,
            "follower_num": x,
            "current_follow_pnl": x,
            "aum": x,
            "avg_position_value": x,
            "cost_val": x,
            "win_ratio": x,
            "loss_days": x,
            "profit_days": x,
            "yield_ratio": x,
        },
        {
            ...
        }
        """
        get_trade_stats_config = config["get_trade_stats"]
        multiple_date_ranges = get_trade_stats_config["date_ranges"]
        traders_stats = []
        for date_range in multiple_date_ranges:
            urls = self.generate_user_statistics_api_endpoint_urls(
                querystring={"dateRange": str(date_range)}, trader_ids=trader_ids, ignore_trader_ids=ignore_trader_ids
            )
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.fetch_api_urls(urls))
            results = loop.run_until_complete(future)

            # Used for matching API returned keys to 'position' table column keys
            api_key_to_db_key_matcher = {
                "followerNum": "follower_num",
                "currentFollowPnl": "current_follow_pnl",
                "aum": "aum",
                "avgPositionValue": "avg_position_value",
                "costVal": "cost_val",
                "winRatio": "win_ratio",
                "lossDays": "loss_days",
                "profitDays": "profit_days",
                "yieldRatio": "yield_ratio",
            }
            
            for dict_i in results:
                response = dict_i["response"]
                if isinstance(response, str):
                    logger.warning("Response is str.")
                    logger.warning(response)
                trader_id = dict_i["trader_id"]  # Get trader_id directly from response
                if not response or response.get("message") != "OK":
                    logger.warning(f"Unable to get getTradeStatsOfTraderById for trader ID: {trader_id}")
                    logger.warning(f"{response.get('message', 'No message provided')}")
                else:
                    data = dict_i["response"].get("data", {})
                    trader_stats_fixed = {
                        api_key_to_db_key_matcher.get(key): val
                        for key, val in data.items()
                        if api_key_to_db_key_matcher.get(key)
                    }
                    trader_stats_fixed = {key: val if val != "" else None for key, val in trader_stats_fixed.items()}
                    trader_stats_fixed["trader_id"] = trader_id
                    trader_stats_fixed["date_range"] = int(date_range)
                    traders_stats.append(trader_stats_fixed)

        return traders_stats
    
    def get_user_yield_ratio(self, trader_ids: list = None, ignore_trader_ids: list = None):
        """
        Returns:
        {
        "trader_id_1": 0.84,
        {
            ...
        }
        """
        trader_ids_w_pnl = {}
        urls = self.generate_user_api_endpoint_urls(trader_ids=trader_ids, ignore_trader_ids=ignore_trader_ids)
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(urls))
        results = loop.run_until_complete(future)

        # Used for matching API returned keys to 'position' table column keys
        api_key_to_db_key_matcher = {
            "yieldRatio": "yield_ratio"
        }
        
        for dict_i in results:
            response = dict_i["response"]
            if isinstance(response, str):
                logger.warning("Response is str.")
                logger.warning(response)
                return None
            trader_id = dict_i["trader_id"]  # Get trader_id directly from response
            if not response or response.get("message") != "OK":
                logger.warning(f"Unable to get getPNLOfTraderById for trader ID: {trader_id}")
                logger.warning(f"{response.get('message', 'No message provided')}")
            else:
                data = dict_i["response"].get("data", {})
                trader_yield_ratio_fixed = {
                    api_key_to_db_key_matcher.get(key): val
                    for key, val in data.items()
                    if api_key_to_db_key_matcher.get(key)
                }
                trader_yield_ratio_fixed = {
                    key: val if val != "" else None
                    for key, val in trader_yield_ratio_fixed.items()
                }
                trader_yield_ratio_fixed["trader_id"] = trader_id
                trader_ids_w_pnl[trader_id] = trader_yield_ratio_fixed

        return trader_ids_w_pnl

    def get_init_traders(
        self,
        type_param: str,
        max_traders_param: int,
        lead_trader_assets_high_param: Optional[int] = None,
        aum_high: Optional[int] = None,
        time_as_lead_trader: Optional[int] = None,
        has_vacancies: Optional[bool] = None,
        aum_low: Optional[int] = None,
        lead_trader_assets_low: Optional[int] = None,
        win_ratio: Optional[float] = None,
    ) -> Optional[List[Dict]]:
        
        params_to_use = {"type": type_param, "page": 1}  # Start with page 1

        # Validate input parameters
        if (
            not validate_param(type_param, str, "type_param", ["pnl", "aum"]) or
            not validate_param(max_traders_param, int, "max_traders_param")
        ):
            return None
        
        # Add other parameters if they are not None
        if lead_trader_assets_high_param is not None:
            params_to_use["leadTraderAssetsHigh"] = str(lead_trader_assets_high_param)
        if aum_low is not None:
            params_to_use["aumLow"] = str(aum_low)
        if lead_trader_assets_low is not None:
            params_to_use["leadTraderAssetsLow"] = str(lead_trader_assets_low)
        if win_ratio is not None:
            params_to_use["winRatio"] = str(win_ratio)
        if aum_high is not None:
            params_to_use["aumHigh"] = str(aum_high)

        # Handle time_as_lead_trader
        if time_as_lead_trader is True:
            params_to_use["timeAsLeadTrader"] = "true"
        elif time_as_lead_trader is False:
            params_to_use["timeAsLeadTrader"] = "false"
        elif time_as_lead_trader is None:
            params_to_use["timeAsLeadTrader"] = "default"

        # Handle has_vacancies
        if has_vacancies is not None:
            params_to_use["hasVacancies"] = "true" if has_vacancies else "false"

        all_traders = []
        url = f"{self.base_url}/trader/t-performance"  # Updated endpoint to fetch all traders

        while True:
            logger.debug(f"Requesting traders from {url} with params: {params_to_use}")

            resp = requests.get(url=url, headers=self.headers, params=params_to_use)

            # Check if the response status code is 200 (OK)
            if resp.status_code != 200:
                logger.warning(f"Response status code != 200. Status code: {resp.status_code}, Response: {resp.text}")
                return None  # Return None to indicate failure

            # Check if the response message is "OK" to ensure a successful fetch
            if resp.json().get("message") == "OK":
                traders = resp.json().get("data")
                all_traders.extend(traders)
                if len(traders) >= 9 and len(all_traders) < max_traders_param:
                    params_to_use["page"] += 1  # Increment page for the next request
                else:
                    if len(all_traders) > max_traders_param:
                        all_traders = all_traders[:max_traders_param]
                    break
            else:
                logger.warning(f"Failed to fetch traders from RapidAPI. Error: {resp.json().get('message')}, Response: {resp.text}")
                return None  # Return None to indicate failure
        
        return all_traders



def get_and_update_init_traders():
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    scraper = LeaderboardScraper(db=db)

    search_traders_config = config["search_traders_config"]
    init_traders_data = scraper.get_init_traders(**search_traders_config)

    if init_traders_data is None or init_traders_data is False:
        return False  # Indicate failure
    
    logger.debug(f"Got {len(init_traders_data)} init traders.")
    db.upsert_init_traders(data=init_traders_data)
    return True  # Indicate success


def update_trader_rois(retry_count: int = 20, include_observed: bool = True):
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    scraper = LeaderboardScraper(db=db)
    logger.info("Updating yieldRatio of API traders")
    
    init_trader_ids = list(db.fetch_init_traders().keys()) if db.fetch_init_traders() else []
    all_trader_ids = list(set(init_trader_ids))
    if include_observed:
        observed_trader_ids = db.fetch_observed_trader_ids()
        all_trader_ids += observed_trader_ids

    trader_ids_w_yield_ratio_from_api = None
    for retry_num in range(1, retry_count + 1):
        trader_ids_w_yield_ratio_from_api = scraper.get_user_yield_ratio(trader_ids=all_trader_ids)
        if trader_ids_w_yield_ratio_from_api:
            break

        if retry_num == retry_count:  # last retry
            break
        
        delay = retry_num * 5
        logger.debug(f"Delaying {delay} seconds")
        time.sleep(delay)
    
    if trader_ids_w_yield_ratio_from_api:
        for trader_id, rois_dict in trader_ids_w_yield_ratio_from_api.items():
            yield_ratio = rois_dict["yield_ratio"]
            data = {
                "yield_ratio": yield_ratio,
            }
            db.update_data(table="trader", data=data, condition_column="trader_id", condition_value=trader_id)
        return True
    else:
        return False
    

def update_trader_stats(retry_count: int = 20, include_observed: bool = True):
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    scraper = LeaderboardScraper(db=db)
    logger.info("Updating trading stats of init traders")
    
    init_trader_ids = list(db.fetch_init_traders().keys()) if db.fetch_init_traders() else []
    all_trader_ids = list(set(init_trader_ids))
    if include_observed:
        observed_trader_ids = db.fetch_observed_trader_ids()
        all_trader_ids += observed_trader_ids

    traders_stats_from_api = None
    for retry_num in range(1, retry_count + 1):
        traders_stats_from_api = scraper.get_user_statistics(trader_ids=all_trader_ids)
        if traders_stats_from_api:
            break

        if retry_num == retry_count:  # last retry
            break
        
        delay = retry_num * 5
        logger.debug(f"Delaying {delay} seconds")
        time.sleep(delay)
    
    if traders_stats_from_api:
        for stats_dict in traders_stats_from_api:
            db.insert_or_update_data(table="trader_stats", data=stats_dict)
        return True
    else:
        return False


def update_last_pos_datetime_for_all_traders_once():
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    logger.info("Updating last positions datetimes once")

    # Assuming you have a method to update the last position datetime for all traders
    db.update_last_pos_datetime_for_all_traders()


def monitor_positions(retry_count: int = 20, delay: int = 10, include_observed: bool = False):
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    scraper = LeaderboardScraper(db=db)

    first_time_run = True
    top_traders_update_status = {
        "updated_on_day": None
    }

    while True:
        current_day = datetime.today().strftime("%A")
        if top_traders_update_status["updated_on_day"] != current_day or first_time_run:
            is_right_time_to_update = helpers.is_valid_time_to_update_top_traders()
            if is_right_time_to_update:
                init_traders_res = get_and_update_init_traders()
                if not init_traders_res:
                    msg = "Unable to get init traders (rapidapi.py)"
                    logger.error(msg)
                    telegram_bot.send_telegram_message(msg=msg)

                top_traders_roi_res = update_trader_rois()
                if not top_traders_roi_res:
                    msg = "Unable to update top traders' ROIs (rapidapi.py)"
                    logger.error(msg)
                    telegram_bot.send_telegram_message(msg=msg)

                traders_stats = update_trader_stats()
                if not traders_stats:
                    msg = "Unable to update top traders' stats (rapidapi.py)"
                    logger.error(msg)
                    telegram_bot.send_telegram_message(msg=msg)

                trader_ids_to_follow = db.detect_traders_to_follow()
                db.set_traders_to_follow(traders_to_follow=trader_ids_to_follow)

                # Update status to the current day name
                top_traders_update_status["updated_on_day"] = current_day
                first_time_run = False
                logger.success(f"Successfully updated traders to follow for {current_day}.")

        for retry_num in range(1, retry_count + 1):
            api_positions = scraper.get_positions_from_api(include_observed=include_observed)

            if api_positions:
                now_fn = datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".json"
                os.makedirs("rapidapi_positions", exist_ok=True)

                fp = os.path.join("rapidapi_positions", now_fn)
                logger.info(f"Dumped positions data from RapidAPI: {now_fn}")

                with open(fp, "w") as f:
                    json.dump(api_positions, f, indent=4)

                db.insert_temp_positions(traders_ids_and_positions=api_positions)
                break
            if api_positions == {}:
                break

            if retry_num == retry_count:
                logger.error("Unable to retrieve API positions from RapidAPI")
                return

            retry_delay = retry_num * 5
            logger.debug(f"Delaying {retry_delay} seconds")
            time.sleep(retry_delay)

        time.sleep(delay)


def validate_param(param, expected_type, param_name: str, valid_values=None):
    if param is not None:
        if not isinstance(param, expected_type):
            logger.error(f"Invalid '{param_name}': {param}")
            return False
        if valid_values and param not in valid_values:
            logger.error(f"Invalid '{param_name}': {param}")
            return False
    return True


def get_hist_positions(instance: str):
    valid_instances = ["x1", "x2", "x3"]
    if instance not in valid_instances:
        logger.error(f"Invalid instance: {instance}")
        return
    pos_table_name = f"position_{instance}"
    kc_stats_table_name = f"kc_stats_{instance}"

    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    scraper = LeaderboardScraper(db=db)
    hist_positions = scraper.get_historical_positions_from_api()
    inserted_count = 0
    for pos_dict_i in hist_positions:
        # Used for matching API returned keys to 'position' table column keys
        api_key_to_db_key_matcher = {
            "ccy": "ccy",
            "closeAvgPx": "close_avg_px",
            "contractVal": "contract_val",
            "id": "okx_pos_id",
            "instId": "inst_id",
            "instType": "inst_type",
            "lever": "lever",
            "margin": "margin",
            "mgnMode": "mgn_mode",
            "multiplier": "multiplier",
            "openAvgPx": "open_avg_px",
            "openTime": "open_time",
            "pnl": "pnl",
            "pnlRatio": "pnl_ratio",
            "posSide": "pos_side",
            "subPos": "user_sub_pos",
            "tradeItemId": "trade_item_id",
            "traderId": "trader_id",
            "uTime": "u_time",
        }
        
        position_dict_fixed = {
            api_key_to_db_key_matcher[key]: val
            for key, val in pos_dict_i.items()
            if api_key_to_db_key_matcher.get(key)
        }
        position_dict_fixed = {key: val if val != "" else None for key, val in position_dict_fixed.items()}
        position_dict_fixed["is_active"] = 0
        position_dict_fixed["is_ignored"] = 1
        position_dict_fixed["is_ignored_reason"] = "historical"
        try:
            db.insert_data(table=pos_table_name, data=position_dict_fixed)
            inserted_count += 1
        except mysql.connector.errors.IntegrityError:
            pass

        db.insert_or_update_kc(kc_stats_table_name=kc_stats_table_name, top_x_table_name=pos_table_name)
    logger.success(f"Successfully inserted {inserted_count} historical positions.")


def run():
    logging_dir = "./logs/rapidapi/"
    os.makedirs(logging_dir, exist_ok=True)
    logger.add(f"{logging_dir}"+"{time:YYYY-MM-DD}.log", rotation="00:00", retention="1 day")
    
    # Updating all traders' last positions datetimes
    update_last_pos_datetime_for_all_traders_once()

    # Constantly monitoring positions
    try:
        monitor_positions(include_observed=True, delay=30)
    except Exception as e:
        full_error_msg = traceback.format_exc()
        logger.error(full_error_msg)
        msg = f"Positions monitoring failed (rapidapi.py). Error: \n{e}"
        telegram_bot.send_telegram_message(msg=msg)


if __name__ == "__main__":
    get_hist_positions(instance="x1")  # Uncomment for inserting historical positions
    # Uncomment for inserting initial traders into the database
    run()
