import math
import os
import sys
import time
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Union

import mysql.connector
from loguru import logger

import helpers
import telegram_bot
from db_manager import DatabaseManager
from helpers import calc_timestamp_diff_in_s, convert_amount, calc_perc_diff_between_x_y
from rapidapi import LeaderboardScraper
from trading_api import TradingAPI

config = helpers.load_config_from_yaml()

DATABASE_FILE = "leaderboard_db"
MAX_TIME_TO_FILL = config["max_time_to_fill"]

IGNORE_NEG_TOTAL_ROI = config["ignore_neg_total_roi_traders"]
IGNORE_NEG_ALL_TIMEFRAMES_ROI = config["ignore_neg_all_timeframes_roi_traders"]
IGNORE_OBSERVED_TRADERS = config["ignore_observed_traders"]

ALLOCATION_OF_TOTAL_BALANCE_PERC = config["equity_of_total_equity"]
ALLOCATION_PER_SINGLE_POSITION_PERC = config["equity_per_single_pos"]   # X%

INCR_DECR_PERC = config["incr_decr_perc"]   # Y%
MAX_POS_SIZE_PERC = config["max_pos_size_perc"]  # W%
MIN_POS_SIZE_PERC = config["min_pos_size_perc"]  # T%

SL_RATIO = config["sl_ratio"]

COPY_TRADER_BY = config["copy_trader_by"]

COPYING_TYPE = "multi"

db_host = config["db_host"]
db_user = config["db_user"]
db_password = config["db_password"]
database = config["database"]


class Leaderboard:
    replicatable_tables = ["position", "kc_stats"]

    def __init__(self, instance: str, instance_to_replicate: str = None):
        self.instance = instance
        self.instance_to_replicate = instance_to_replicate
        self.position_table_name = f"position_{self.instance}"
        self.kc_stats_table_name = f"kc_stats_{self.instance}"
        self.config = None
        self.db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
        self.scraper = LeaderboardScraper(db=self.db)
        if self.instance_to_replicate:
            self.replicate_instance()

    def replicate_instance(self):
        logger.debug(f"Trying to replicate an instance {self.instance_to_replicate}")
        from_instance = self.instance_to_replicate.split("_")[-1]
        from_tables = [f"{partial_table}_{self.instance_to_replicate}" for partial_table in self.replicatable_tables]
        to_tables = [table.replace(from_instance, self.instance) for table in from_tables]
        
        for from_table, to_table in zip(from_tables, to_tables):
            replication_res = self.db.replicate_existing_table(from_table=from_table, to_table=to_table)
            if not replication_res:
                logger.error("Something went wrong when replicating tables. Stopping.")
                sys.exit(1)

    def check_and_update_filled_db_orders(self, db_positions: dict):  
        logger.info("Checking current DB positions if they got filled")

        db_positions_flattened = [
            position
            for trader_id in db_positions
            for position in db_positions[trader_id]
            if position["bin_pos_id"]
        ]
        db_position_unique_symbols = (
            list(
                set([
                    position["inst_id"].split("-")[0]
                    for position in db_positions_flattened
                ])
            )
        )
        
        # get all filled orders
        all_filled_orders = {}
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"symbols": db_position_unique_symbols}
        filled_orders = trader.get_filled_orders_for_multi_symbols(metadata=metadata)

        for order_dict in filled_orders:
            order_id = order_dict["info"]["orderId"]
            all_filled_orders[order_id] = order_dict

        # Update positions that were filled
        for trader_id in db_positions:
            positions = db_positions[trader_id]
            for position in positions:
                okx_position_id = position["okx_pos_id"]  # primary key
                is_filled_db = position["is_filled"]
                if not is_filled_db:
                    is_order_filled = all_filled_orders.get(position["bin_pos_id"])
                    if is_order_filled:
                        position["is_filled"] = 1
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=okx_position_id
                        )
                        logger.debug(f"Position got filled, okx_pos_id: {okx_position_id}")

    def update_db_positions_pnl_and_roe(self, trader_ids_w_api_positions: dict, trader_ids_w_db_positions: dict):
        logger.info("Updating PNL and ROE of matched DB -> API positions")
               
        for db_trader_id in trader_ids_w_db_positions:
            db_positions = trader_ids_w_db_positions[db_trader_id]
            for db_position in db_positions:
                okx_pos_id = db_position["okx_pos_id"]  # primary key

                if db_trader_id in trader_ids_w_api_positions:
                    api_positions = trader_ids_w_api_positions[db_trader_id]
                    same_position_result = self.try_to_find_same_position(
                        single_position=db_position, versus_positions=api_positions, single_position_type="db"
                    )
                    is_same_position = same_position_result["same_position"]
                    # if it's the same position, then we need to update PNL and ROE of matched position
                    if is_same_position:
                        api_position = same_position_result["position"]
                        position = {
                            "pnl": api_position["pnl"],
                            "roe": api_position["roe"],
                        }
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=okx_pos_id
                        )

    def close_or_cancel_no_longer_valid_db_positions(
        self,
        trader_ids_w_api_positions: dict,
        trader_ids_w_db_positions: dict
    ):
        logger.info("Searching for no longer relevant DB positions to close or cancel.")
        
        # Go over all DB positions and try to identify no longer relevant DB positions and new API positions
        db_positions_to_close = []
        db_positions_to_cancel = []
        
        for db_trader_id in trader_ids_w_db_positions:
            db_positions = trader_ids_w_db_positions[db_trader_id]
            for db_position in db_positions:
                table_position_id = db_position["id"]  # primary key
                db_position_id = db_position["position_id"]
                is_filled = db_position["is_filled"]
                is_canceled = db_position["is_canceled"]
                is_closed = db_position["is_closed"]
                db_position_roe = db_position["roe"]

                # if any([is_canceled, is_closed]):
                #     continue
                
                if db_trader_id in trader_ids_w_api_positions:
                    api_positions = trader_ids_w_api_positions[db_trader_id]
                    same_position_result = self.try_to_find_same_position(
                        single_position=db_position, versus_positions=api_positions, single_position_type="db"
                    )
                    is_same_position = same_position_result["same_position"]

                    # check if it needs to set as ignored if it's still not filled in X amount of time
                    if is_same_position:
                        if not is_filled:
                            insert_timestamp = db_position["insert_timestamp"]
                            timestamp_diff_in_s = calc_timestamp_diff_in_s(timestamp=insert_timestamp)
                            if timestamp_diff_in_s >= MAX_TIME_TO_FILL:
                                db_position["is_ignored"] = 1
                                db_position["is_ignored_reason"] = "expired"
                                if db_position_id:  # it means it was copied (but not filled)
                                    if not is_canceled:
                                        db_positions_to_cancel.append(db_position)
                    else:
                        if is_filled:
                            if not is_closed:
                                db_positions_to_close.append(db_position)
                        else:  # not filled
                            if db_position_id:  # we will need to cancel it
                                if not is_canceled:
                                    db_positions_to_cancel.append(db_position)
                            else:  # no need to cancel or close as this position wasn't even copied
                                position = {
                                    "is_active": 0
                                }
                                logger.debug(f"Deactivating table position ID (x1): {table_position_id}")
                                self.db.update_data(
                                    table=self.position_table_name,
                                    data=position,
                                    condition_column="id",
                                    condition_value=table_position_id
                                )
                                
                                # Update innactive position success result (win or lose) for the trader
                                if db_position_roe > 0:
                                    self.db.insert_or_update_success_stats(
                                        trader_id=db_trader_id,
                                        position_table_name=self.position_table_name,
                                        is_win=True
                                    )
                                elif db_position_roe < 0:
                                    self.db.insert_or_update_success_stats(
                                        trader_id=db_trader_id,
                                        position_table_name=self.position_table_name,
                                        is_win=False
                                    )

                else:  # The position of a trader that doesn't exist anymore inside API traders
                    if any([is_canceled, is_closed]):
                        position = {
                            "is_active": 0
                        }
                        logger.debug(f"Deactivating table position ID (x2): {table_position_id}")
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=table_position_id
                        )

                        # Update innactive position success result (win or lose) for the trader
                        if db_position_roe > 0:
                            self.db.insert_or_update_success_stats(
                                trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=True
                            )
                        elif db_position_roe < 0:
                            self.db.insert_or_update_success_stats(
                                trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=False
                            )

                        continue

                    if is_filled:  # we will need to close it
                        if not is_closed:
                            db_positions_to_close.append(db_position)
                    else:
                        if db_position_id:  # we will need to cancel it
                            if not is_canceled:
                                db_positions_to_cancel.append(db_position)
                        else:  # we won't need to cancel it because this position was never copied
                            position = {
                                "is_active": 0
                            }
                            logger.debug(f"Deactivating table position ID (x3): {table_position_id}")
                            self.db.update_data(
                                table=self.position_table_name,
                                data=position,
                                condition_column="id",
                                condition_value=table_position_id
                            )

                            # Update innactive position success result (win or lose) for the trader
                            if db_position_roe > 0:
                                self.db.insert_or_update_success_stats(
                                    trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=True
                                )
                            elif db_position_roe < 0:
                                self.db.insert_or_update_success_stats(
                                    trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=False
                                )

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": db_positions_to_cancel}
        canceled_orders = trader.cancel_multi_orders_v2(metadata=metadata)
        for canceled_order_res in canceled_orders:
            status, result = canceled_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                position = {
                    "is_active": 0,
                    "is_ignored": result.get("is_ignored"),
                    "is_ignored_reason": result.get("is_ignored_reason"),
                    "is_canceled": 1
                }
                if position["is_ignored_reason"] == "expired":  # it's just expired but still active
                    del position["is_active"]
                logger.debug(f"Successfully canceled table position ID: {position_table_id}")
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )

                if "is_active" in position:  # it means "is_active": 0
                    db_trader_id = result["trader_id"]
                    db_position_roe = result["db_position_roe"]
                    # Update innactive position success result (win or lose) for the trader
                    if db_position_roe > 0:
                        self.db.insert_or_update_success_stats(
                            trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=True
                        )
                    elif db_position_roe < 0:
                        self.db.insert_or_update_success_stats(
                            trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=False
                        )

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": db_positions_to_close}
        closed_orders = trader.close_multi_orders_v2(metadata=metadata)
        for closed_order_res in closed_orders:
            status, result = closed_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                new_amount_user = (
                    float(result.get("amount_user")) - float(result.get("amount"))
                )   # original user amount - amount that was closed
                position = {
                    "is_active": 0,
                    "amount_user": new_amount_user,
                    "is_closed": 1
                }
                # logger.debug(f"Successfully closed table position ID: {position_table_id}")
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )

                db_trader_id = result["trader_id"]
                db_position_roe = result["db_position_roe"]
                # Update innactive position success result (win or lose) for the trader
                if db_position_roe > 0:
                    self.db.insert_or_update_success_stats(
                        trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=True
                    )
                elif db_position_roe < 0:
                    self.db.insert_or_update_success_stats(
                        trader_id=db_trader_id, position_table_name=self.position_table_name, is_win=False
                    )

    def insert_new_api_positions(
        self,
        trader_ids_w_api_positions: dict,
        trader_ids_w_db_positions: dict,
        first_time_run
    ):
        logger.info("Inserting new API positions")

        api_positions_to_insert = {}

        # Go over all API positions and try to identify same DB positions and new API positions
        for api_trader_id in trader_ids_w_api_positions:
            api_positions = trader_ids_w_api_positions[api_trader_id]
            for api_position in api_positions:
                api_position_trader_id = api_position["trader_id"]
                if api_position_trader_id in trader_ids_w_db_positions:
                    same_position_result = self.try_to_find_same_position(
                        single_position=api_position,
                        versus_positions=trader_ids_w_db_positions[api_position_trader_id],
                        single_position_type="api"
                    )
                    is_same_position = same_position_result["same_position"]
                    if is_same_position:
                        continue
                    else:
                        if api_trader_id not in api_positions_to_insert:
                            api_positions_to_insert[api_trader_id] = []
                        # print("x1")
                        api_positions_to_insert[api_trader_id].append(api_position)
                else:
                    if api_trader_id not in api_positions_to_insert:
                        api_positions_to_insert[api_trader_id] = []
                    # print("x2")
                    api_positions_to_insert[api_trader_id].append(api_position)

        if not [pos for api_trader_id in api_positions_to_insert for pos in api_positions_to_insert[api_trader_id]]:
            return True

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)

        all_traders_stats = self.db.get_all_traders_success_stats(position_table_name=self.position_table_name)

        metadata = {
            "allocation_of_total_balance": ALLOCATION_OF_TOTAL_BALANCE_PERC,
            "allocation_per_single_position": ALLOCATION_PER_SINGLE_POSITION_PERC,
        }
        res = trader.calc_balance_availability(metadata=metadata)
        status, result = res[0]
        if not status:
            logger.error(result)
            return None
        
        balance_to_use_for_trading_in_usdt = result["balance_to_use_for_trading_in_usdt"]
        # usdt_amount_to_use_per_single_position = result["usdt_amount_to_use_per_single_position"]  # remove it later

        # Round quantity_to_close by symbol precisions
        all_symbols = []
        for trader_id in api_positions_to_insert:
            positions = api_positions_to_insert[trader_id]
            for position in positions:
                all_symbols.append(position["inst_id"])
        unique_symbols = list(set(all_symbols))
        metadata = {
            "symbols": unique_symbols
        }
        # update user amount
        min_qty_and_step_sizes = trader.get_min_qty_and_step_size_for_symbols(metadata=metadata)
        for trader_id in api_positions_to_insert:
            positions = api_positions_to_insert[trader_id]
            for dict_i in positions:
                entry_price = dict_i["entry_price"]
                pos_symbol = dict_i["symbol"]
                leverage = dict_i["leverage"]

                # the logic of X%, Y%, W%, T%.
                x_perc = ALLOCATION_PER_SINGLE_POSITION_PERC  # X%
                trader_win_lose_count_res = all_traders_stats[trader_id]["win_lose_count_res"]
                new_x_perc_allocation = (
                    x_perc + (trader_win_lose_count_res * INCR_DECR_PERC)
                )   # based by trader's win, lose sum

                if new_x_perc_allocation > MAX_POS_SIZE_PERC:
                    new_x_perc_allocation = MAX_POS_SIZE_PERC
                elif new_x_perc_allocation < MIN_POS_SIZE_PERC:
                    new_x_perc_allocation = MIN_POS_SIZE_PERC

                dict_i["balance_allocation_perc"] = new_x_perc_allocation

                usdt_amount_to_use_per_single_position = (
                    balance_to_use_for_trading_in_usdt * (new_x_perc_allocation / 100)
                )

                if pos_symbol in min_qty_and_step_sizes:
                    min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                    step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                    user_amount = (usdt_amount_to_use_per_single_position / entry_price) * leverage
                    user_amount_fixed = convert_amount(
                        user_amount=user_amount, min_qty=min_qty, step_size=step_size, entry_price=entry_price
                    )
                else:
                    user_amount_fixed = (usdt_amount_to_use_per_single_position / entry_price) * leverage
                    logger.warning(f"Missing min_qty, step_size of {pos_symbol} symbol")

                if COPYING_TYPE == "single":
                    dict_i["amount_user"] = user_amount_fixed
                elif COPYING_TYPE == "multi":
                    dict_i["amount_user"] = None

        unique_trader_ids = list(set([trader_id for trader_id in api_positions_to_insert]))
        # Insert new API positions but set as still not copied
        for api_trader_id in api_positions_to_insert:
            api_positions = api_positions_to_insert[api_trader_id]
            for api_position in api_positions:
                # Ignoring by first time run
                if first_time_run:
                    api_position["is_ignored"] = 1
                    api_position["is_ignored_reason"] = "first time run"
                # Ignoring by win-lose count res and ROIs
                else:
                    # trader_win_lose_count_res = all_traders_stats[api_position["trader_id"]]["win_lose_count_res"]
                    # if trader_win_lose_count_res < 0:  # uncomment later
                    #     api_position["is_ignored"] = 1 # uncomment later
                    #     api_position["is_ignored_reason"] = "negative win-lose count" # uncomment later

                    if not api_position["is_ignored"]:  # if the position is still not ignored - check ROIs
                        trader_ids_w_roi = self.db.fetch_trader_ids_with_roi(trader_ids=unique_trader_ids)  # FIX
                        if trader_ids_w_roi.get(api_position['trader_id']) is None:
                            logger.warning(f"Missing ROIs data of trader ID: {api_position['trader_id']}")
                            continue

                        kc_stats = self.db.get_all_traders_kc_stats(kc_stats_table_name=self.kc_stats_table_name)
                        
                        roi_data_dict = trader_ids_w_roi.get(api_position['trader_id'])
                        daily_roi = roi_data_dict["daily_roi"]
                        weekly_roi = roi_data_dict["weekly_roi"]
                        monthly_roi = roi_data_dict["monthly_roi"]
                        total_roi = roi_data_dict["total_roi"]

                        trader_type = self.db.detect_trader_type(trader_id=api_position['trader_id'])

                        if IGNORE_OBSERVED_TRADERS and trader_type == "observed":
                            api_position["is_ignored"] = 1
                            api_position["is_ignored_reason"] = "ignore observed"

                        # Ignoring by multiple timeframe ROIs
                        if IGNORE_NEG_ALL_TIMEFRAMES_ROI and not api_position["is_ignored"]:
                            is_ignored = 0
                            is_ignored_reason = "negative"

                            if daily_roi and daily_roi <= 0:
                                is_ignored = 1
                                is_ignored_reason += " daily,"
                            if weekly_roi and weekly_roi <= 0:
                                is_ignored = 1
                                is_ignored_reason += " weekly,"
                            if monthly_roi and monthly_roi <= 0:
                                is_ignored = 1
                                is_ignored_reason += " monthly,"
                            if total_roi and total_roi <= 0:
                                is_ignored = 1
                                is_ignored_reason += " total,"
                            
                            if is_ignored:
                                api_position["is_ignored"] = is_ignored
                                is_ignored_reason_final = is_ignored_reason.strip(",") + " ROI"
                                api_position["is_ignored_reason"] = is_ignored_reason_final

                        if IGNORE_NEG_TOTAL_ROI and not api_position["is_ignored"]:  # Ignoring by total ROI
                            if total_roi is None:
                                api_position["is_ignored"] = 1
                                api_position["is_ignored_reason"] = "missing total ROI"
                            elif total_roi <= 0:  # set all positions as ignored if the trader's ROI is negative
                                api_position["is_ignored"] = 1
                                api_position["is_ignored_reason"] = "negative total ROI"
                        
                        if not api_position["is_ignored"]:  # if the position is still not ignored - check trades count
                            traders_trades_counts = self.db.get_all_traders_trades_counts(
                                top_x_table_name=self.position_table_name
                            )
                            min_trades_count = 30
                            if traders_trades_counts.get(api_trader_id, 0) < min_trades_count:
                                api_position["is_ignored"] = 1
                                api_position["is_ignored_reason"] = f"less than {min_trades_count} trades"
                        
                        if not api_position["is_ignored"]:  # if the position is still not ignored - check kc stats
                            pos_trader_kc_stat = kc_stats.get(api_trader_id)
                            if pos_trader_kc_stat and pos_trader_kc_stat <= 0:
                                api_position["is_ignored"] = 1
                                api_position["is_ignored_reason"] = "negative kc"

                        # IGNORING LOGIC FOR DIFFERENT SIDE BUT SAME SIDE SYMBOLS

                api_position["is_copied"] = 0
                try:
                    current_datetime_object = datetime.now()
                    timestamp = int(str(time.time()).replace(".", "")[:13])
                    api_position["insert_timestamp"] = timestamp
                    self.db.insert_position(table=self.position_table_name, data=api_position)
                    self.db.update_last_pos_datetime_for_trader(
                        trader_id=api_trader_id, last_pos_datetime=current_datetime_object
                    )
                except mysql.connector.IntegrityError as e:
                    logger.warning(e)

    def ignore_and_or_close_or_cancel_opposite_and_same_positions(self):
        logger.info("Ignoring and/or canceling or closing opposite, same symbol DB positions.")

        trader_ids_w_db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        unique_trader_ids = list(set([trader_id for trader_id in trader_ids_w_db_positions]))

        trader_ids_w_roi = self.db.fetch_trader_ids_with_roi(trader_ids=unique_trader_ids)

        # Find opposite positions and decide which ones should be ignored
        logger.info("Searching for opposite positions")

        all_traders_stats = self.db.get_all_traders_success_stats(position_table_name=self.position_table_name)
        
        db_positions_w_same_symbols_to_ignore_ids = []  # used to set appropriate ingore reason
        db_positions_to_ignore_because_of_hedged_status = []  # used to set appropriate ingore reason
        db_positions_to_ignore_because_of_lower_win_lose_res = []  # used to set appropriate ingore reason
        db_positions_to_ignore_because_of_lower_roi = []  # used to set appropriate ingore reason

        db_positions_to_ignore_and_cancel_or_close_ids = []  # position table IDs - ignored and canceled or closed
        db_positions_flattened = [
            position
            for trader_id in trader_ids_w_db_positions
            for position in trader_ids_w_db_positions[trader_id]
        ]
        for current_idx, current_pos in enumerate(db_positions_flattened):
            current_pos_table_id = current_pos["id"]
            current_pos_trader_id = current_pos["trader_id"]
            current_pos_is_ignored = current_pos["is_ignored"]
            current_pos_symbol, current_pos_side = current_pos["symbol"], current_pos["side"]
            current_pos_symbol_w_side = f"{current_pos_symbol}_{current_pos_side}"

            if current_idx < (len(db_positions_flattened) - 1):  # if not the last position
                for next_idx, next_pos in enumerate(db_positions_flattened[current_idx + 1:]):
                    next_pos_table_id = next_pos["id"]
                    next_pos_trader_id = next_pos["trader_id"]
                    next_pos_is_ignored = next_pos["is_ignored"]
                    next_pos_symbol = next_pos["symbol"]
                    next_pos_symbol_w_side = f"{next_pos_symbol}_{next_pos['side']}"
                    next_pos_opposite_side = "buy" if next_pos["side"] == "sell" else "sell"
                    next_pos_symbol_w_opposite_side = (
                        f"{next_pos_symbol}_{next_pos_opposite_side}"
                    )   # flip the side to find out if this is an opposite position

                    # if the curent/next position is ignored - we don't need to compare as it doesn't matter
                    if current_pos_is_ignored or next_pos_is_ignored:
                        continue
                    if current_pos_symbol_w_side == next_pos_symbol_w_opposite_side:  # found opposite position
                        if current_pos_trader_id == next_pos_trader_id:  # hedged positions?
                            logger.debug(f"Detected hedged position. {current_pos_table_id}, {next_pos_table_id}")
                            current_pos_update_timestamp = current_pos["update_timestamp"]
                            next_pos_update_timestamp = next_pos["update_timestamp"]
                            # logic for ignoring the earlier position (not the latest)
                            if next_pos_update_timestamp >= current_pos_update_timestamp:
                                db_positions_to_ignore_and_cancel_or_close_ids.append(current_pos_table_id)
                                db_positions_to_ignore_because_of_hedged_status.append(current_pos_table_id)
                            else:
                                db_positions_to_ignore_and_cancel_or_close_ids.append(next_pos_table_id)
                                db_positions_to_ignore_because_of_hedged_status.append(next_pos_table_id)

                        current_pos_trader_win_lose_res = all_traders_stats[current_pos_trader_id]["win_lose_count_res"]
                        next_pos_trader_win_lose_res = all_traders_stats[next_pos_trader_id]["win_lose_count_res"]

                        if current_pos_trader_win_lose_res != next_pos_trader_win_lose_res:
                            compare_by = "win_lose_res"
                        else:
                            compare_by = "roi"

                        if compare_by == "win_lose_res":  # comparing by win lose res
                            if current_pos_trader_win_lose_res > next_pos_trader_win_lose_res:
                                db_positions_to_ignore_and_cancel_or_close_ids.append(next_pos_table_id)
                                db_positions_to_ignore_because_of_lower_win_lose_res.append(next_pos_table_id)
                            else:
                                db_positions_to_ignore_and_cancel_or_close_ids.append(current_pos_table_id)
                                db_positions_to_ignore_because_of_lower_win_lose_res.append(current_pos_table_id)
                        else:  # comparing by total ROI
                            if (
                                trader_ids_w_roi[current_pos['trader_id']]['total_roi']
                                > trader_ids_w_roi[next_pos['trader_id']]['total_roi']
                            ):
                                db_positions_to_ignore_and_cancel_or_close_ids.append(next_pos_table_id)
                                db_positions_to_ignore_because_of_lower_roi.append(next_pos_table_id)
                            else:  # next position total ROI is better
                                db_positions_to_ignore_and_cancel_or_close_ids.append(current_pos_table_id)
                                db_positions_to_ignore_because_of_lower_roi.append(current_pos_table_id)

                    elif current_pos_symbol_w_side == next_pos_symbol_w_side:  # found same symbol and side position
                        # we leave the first position as active and ignore the next position 
                        db_positions_to_ignore_and_cancel_or_close_ids.append(next_pos_table_id)
                        db_positions_w_same_symbols_to_ignore_ids.append(next_pos_table_id)

        # ignore and cancel/close opposite positions
        opposite_positions_to_cancel = []
        opposite_positions_to_close = []
        for trader_id in trader_ids_w_db_positions:
            for current_pos in trader_ids_w_db_positions[trader_id]:
                current_pos_table_id = current_pos["id"]
                current_pos_is_filled = current_pos["is_filled"]
                current_pos_is_copied = current_pos["is_copied"]
                current_pos_is_ignored = current_pos["is_ignored"]
                current_pos_is_canceled = current_pos["is_canceled"]
                current_pos_is_closed = current_pos["is_closed"]
                if current_pos_is_ignored:
                    continue
                if current_pos_table_id in db_positions_to_ignore_and_cancel_or_close_ids:
                    current_pos["is_ignored"] = 1
                    if not current_pos_is_copied:
                        position = None
                        if current_pos_table_id in db_positions_w_same_symbols_to_ignore_ids:
                            position = {
                                "is_ignored": 1,
                                "is_ignored_reason": 'same symbol and side',
                            }
                        elif current_pos_table_id in db_positions_to_ignore_because_of_hedged_status:
                            position = {
                                "is_ignored": 1,
                                "is_ignored_reason": "hedged",
                            }
                        elif current_pos_table_id in db_positions_to_ignore_because_of_lower_roi:
                            # it means this is recently received API position that needs to be ignored
                            position = {
                                "is_ignored": 1,
                                "is_ignored_reason": "lower roi",
                            }
                        elif current_pos_table_id in db_positions_to_ignore_because_of_lower_win_lose_res:
                            # it means this is recently received API position that needs to be ignored
                            position = {
                                "is_ignored": 1,
                                "is_ignored_reason": "lower win lose res",
                            }

                        if position is not None:
                            self.db.update_data(
                                table=self.position_table_name,
                                data=position,
                                condition_column="id",
                                condition_value=current_pos_table_id
                            )
                    elif current_pos_is_filled:
                        current_pos["position_table_id"] = current_pos_table_id
                        if not current_pos_is_closed:
                            opposite_positions_to_close.append(current_pos)
                    elif not current_pos_is_filled:
                        current_pos["position_table_id"] = current_pos_table_id
                        if not current_pos_is_canceled:
                            opposite_positions_to_cancel.append(current_pos)

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": opposite_positions_to_cancel}
        canceled_orders = trader.cancel_multi_orders_v2(metadata=metadata)
        for canceled_order_res in canceled_orders:
            status, result = canceled_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                if position_table_id in db_positions_w_same_symbols_to_ignore_ids:
                    is_ignored_reason = "same symbol and side"
                elif position_table_id in db_positions_to_ignore_because_of_hedged_status:
                    is_ignored_reason = "hedged"
                elif position_table_id in db_positions_to_ignore_because_of_lower_roi:
                    is_ignored_reason = "lower roi"
                elif position_table_id in db_positions_to_ignore_because_of_lower_win_lose_res:
                    is_ignored_reason = "lower win lose res"
                else:
                    is_ignored_reason = "unknown"
                position = {
                    "is_ignored": 1,
                    "is_ignored_reason": is_ignored_reason,
                    "is_canceled": 1
                }
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": opposite_positions_to_close}
        closed_orders = trader.close_multi_orders_v2(metadata=metadata)
        for closed_order_res in closed_orders:
            status, result = closed_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                if position_table_id in db_positions_w_same_symbols_to_ignore_ids:
                    is_ignored_reason = "same symbol and side"
                elif position_table_id in db_positions_to_ignore_because_of_hedged_status:
                    is_ignored_reason = "hedged"
                elif position_table_id in db_positions_to_ignore_because_of_lower_roi:
                    is_ignored_reason = "lower roi"
                elif position_table_id in db_positions_to_ignore_because_of_lower_win_lose_res:
                    is_ignored_reason = "lower win lose res"
                else:
                    is_ignored_reason = "unknown"
            
                new_amount_user = (
                    float(result.get("amount_user")) - float(result.get("amount"))
                )   # original user amount - amount that was closed
                position = {
                    "is_ignored": 1,
                    "is_ignored_reason": is_ignored_reason,
                    "amount_user": new_amount_user,
                    "is_closed": 1
                }
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )

    def update_db_positions_amounts(self, trader_ids_w_db_positions: dict, trader_ids_w_api_positions: dict):
        # update previous DB positions (if they are not ignored)
        logger.info("Partially closing DB positions")
        db_positions_to_partially_close = []

        for db_trader_id in trader_ids_w_db_positions:
            db_positions = trader_ids_w_db_positions[db_trader_id]
            for db_position in db_positions:
                table_position_id = db_position["id"]  # primary key
                is_filled = db_position["is_filled"]
                is_ignored = db_position["is_ignored"]
                api_positions = (
                    trader_ids_w_api_positions[db_trader_id]
                    if trader_ids_w_api_positions.get(db_trader_id)
                    else []
                )   # position_temp might not have any records for the trader_id that exists inside position_top_x table
                same_position_result = self.try_to_find_same_position(
                    single_position=db_position,
                    versus_positions=api_positions,
                    single_position_type="db"
                )
                is_same_position = same_position_result["same_position"]
                is_need_to_update_amount = same_position_result["need_to_update_amount"]
                api_position = same_position_result["position"]
                if is_same_position:
                    if is_need_to_update_amount:
                        current_pos_amount = db_position["amount"]  # ex.: 100
                        api_position_amount = api_position["amount"]  # ex.: 60 
                        amount_diff_ratio = api_position_amount / current_pos_amount  # ex.: 60 / 100 = 0.6

                        new_amount_user = db_position["amount_user"] * amount_diff_ratio
                        quantity_to_close = db_position["amount_user"] - new_amount_user

                        db_position["amount"] = api_position_amount
                        db_position["quantity_to_close"] = quantity_to_close
                        
                        if not is_ignored and is_filled:
                            db_positions_to_partially_close.append(db_position)

                        # data = {
                        #     "amount": api_position["amount"],
                        #     "update_timestamp": api_position["update_timestamp"]
                        # }
                        # self.db.update_data(
                        #     table="position",
                        #     data=data,
                        #     condition_column="id",
                        #     condition_value=table_position_id
                        # )
                    else:
                        # trader probably increased his position but we won't
                        if db_position["amount"] < api_position["amount"]:
                            data = {"amount": api_position["amount"]}
                            self.db.update_data(
                                table=self.position_table_name,
                                data=data,
                                condition_column="id",
                                condition_value=table_position_id
                            )
        
        if not db_positions_to_partially_close:
            return True

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)

        # Round quantity_to_close by symbol precisions
        unique_symbols = list(set([i["symbol"] for i in db_positions_to_partially_close]))
        metadata = {
            "symbols": unique_symbols
        }
        min_qty_and_step_sizes = trader.get_min_qty_and_step_size_for_symbols(metadata=metadata)
        for dict_i in db_positions_to_partially_close:
            pos_symbol = dict_i["symbol"]
            if pos_symbol in min_qty_and_step_sizes:
                # min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                quantity_to_close = dict_i["quantity_to_close"]
                quantity_to_close_fixed = math.floor(quantity_to_close / step_size) * step_size
                if quantity_to_close_fixed > dict_i["amount_user"]:
                    quantity_to_close_fixed = dict_i["amount_user"]
                dict_i["quantity_to_close"] = quantity_to_close_fixed
                logger.debug("I will try to setup this order to partially close:")
                logger.debug(dict_i)
            else:
                logger.warning(f"Missing min_qty, step_size of {pos_symbol} symbol")

        metadata = {"orders": db_positions_to_partially_close}
        partialy_closed_orders = trader.partially_close_multi_orders_v2(metadata=metadata)
        for partially_closed_order_res in partialy_closed_orders:
            status, result = partially_closed_order_res  # result == order
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                new_amount_user = (
                    result.get("amount_user") - result.get("amount")
                )   # original user amount - amount that was reduced
                position = {
                    # we need to update amount (API position (which coresponds db position) had decreased amount)
                    "amount": result.get("amount_original"),
                    "amount_user": new_amount_user,  # update amount_user of partially closed
                }
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )
    
    def copy_new_positions(self):
        logger.info("Copying new API positions")

        # check all DB positions thats still not copied (basically open positions for API positions that were inserted)
        positions_to_open = []
        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for trader_id in db_positions:
            for current_pos in db_positions[trader_id]:
                current_pos_is_ignored = current_pos["is_ignored"]
                current_pos_is_copied = current_pos["is_copied"]
                if current_pos_is_ignored:
                    continue
                if not current_pos_is_copied:
                    positions_to_open.append(current_pos)

        if not positions_to_open:
            return True

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)

        metadata = {
            "allocation_of_total_balance": ALLOCATION_OF_TOTAL_BALANCE_PERC,
            "allocation_per_single_position": ALLOCATION_PER_SINGLE_POSITION_PERC,
        }
        res = trader.calc_balance_availability(metadata=metadata)
        status, result = res[0]
        if not status:
            logger.error(result)
            return None
        
        free_balance_to_use_for_trading_in_usdt = result["free_balance_to_use_for_trading_in_usdt"]

        unique_symbols = list(set([i["symbol"] for i in positions_to_open]))
        metadata = {
            "symbols": unique_symbols
        }

        # update entry prices
        last_prices = trader.get_last_prices_for_symbols(metadata=metadata)  # {"BTCUSDT": 26157, "BNBUSDT": 258, ...}
        for dict_i in positions_to_open:
            dict_i["entry_price"] = (
                last_prices[dict_i["symbol"]]
                if last_prices.get(dict_i["symbol"])
                else dict_i["entry_price"]
            )

        # Filter positions by MAX_TIME_TO_FILL
        ignore_pos_idxs = []
        for idx, position in enumerate(positions_to_open):
            current_pos_table_id = position["id"]
            current_pos_insert_timestamp = position["insert_timestamp"]
            timestamp_diff_in_s = calc_timestamp_diff_in_s(timestamp=current_pos_insert_timestamp)
            if timestamp_diff_in_s >= MAX_TIME_TO_FILL:
                position = {
                    "is_ignored": 1,
                    "is_ignored_reason": "expired",
                }
                logger.debug(f"Ignoring position ID ({current_pos_table_id}) because of MAX_TIME_TO_FILL.")
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=current_pos_table_id
                )
                ignore_pos_idxs.append(idx)

        positions_to_open = [pos for idx, pos in enumerate(positions_to_open) if idx not in ignore_pos_idxs]

        # Filter positions by free balance in USDT
        ignore_pos_idxs = []
        for idx, position in enumerate(positions_to_open):
            current_pos_table_id = position["id"]
            position_value = (
                (position["entry_price"] * position["amount_user"]) / position["leverage"]
            )   # basically margin requirement value
            free_balance_to_use_for_trading_in_usdt -= position_value
            if free_balance_to_use_for_trading_in_usdt < 0:
                position = {
                    "is_ignored": 1,
                    "is_ignored_reason": "insufficient funds",
                }
                logger.debug(f"Ignoring position ID ({current_pos_table_id}) because of insufficient funds.")
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=current_pos_table_id
                )
                ignore_pos_idxs.append(idx)

        positions_to_open = [pos for idx, pos in enumerate(positions_to_open) if idx not in ignore_pos_idxs]

        metadata = {"orders": positions_to_open}
        opened_orders = trader.open_multi_orders(metadata=metadata)
        for opened_order_res in opened_orders:
            status, result = opened_order_res
            if not status:
                logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                position = {
                    "is_copied": 1,
                    "position_id": result["info"]["orderId"],
                    # TO-DO: you might also need to update user_amount
                }
                self.db.update_data(
                    table=self.position_table_name,
                    data=position,
                    condition_column="id",
                    condition_value=position_table_id
                )
                # logger.success(f"Successfully placed limit order. Position table ID: {position_table_id}")

    def find_largest_kc_trader_id(self) -> Union[str, None]:
        possible_positions_to_open = []
        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for trader_id in db_positions:
            for current_pos in db_positions[trader_id]:
                current_pos_is_ignored = current_pos["is_ignored"]
                current_pos_is_closed = current_pos["is_closed"]  # position might be closed because of SL but active
                if not current_pos_is_ignored and not current_pos_is_closed:
                    possible_positions_to_open.append(current_pos)

        # find a trader with the largest Kelly Criteria
        largest_kc_value = 0
        largest_kc_trader_id = None
        kc_stats = self.db.get_all_traders_kc_stats(kc_stats_table_name=self.kc_stats_table_name)
        for pos in possible_positions_to_open:
            pos_trader_id = pos["trader_id"]
            pos_trader_kc_stat = kc_stats[pos_trader_id]
            if pos_trader_kc_stat and pos_trader_kc_stat > largest_kc_value:
                largest_kc_value = pos_trader_kc_stat
                largest_kc_trader_id = pos_trader_id

        return largest_kc_trader_id
    
    def find_largest_tc_trader_id(self) -> Union[str, None]:
        possible_positions_to_open = []
        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for trader_id in db_positions:
            for current_pos in db_positions[trader_id]:
                current_pos_is_ignored = current_pos["is_ignored"]
                current_pos_is_closed = current_pos["is_closed"]  # position might be closed because of SL but active
                if not current_pos_is_ignored and not current_pos_is_closed:
                    possible_positions_to_open.append(current_pos)

        # find a trader with the largest trades count
        largest_tc_value = 0
        largest_tc_trader_id = None
        tc_stats = self.db.get_all_traders_tc_stats(kc_stats_table_name=self.kc_stats_table_name)
        for pos in possible_positions_to_open:
            pos_trader_id = pos["trader_id"]
            pos_trader_tc_stat = tc_stats[pos_trader_id]
            if pos_trader_tc_stat and pos_trader_tc_stat > largest_tc_value:
                largest_tc_value = pos_trader_tc_stat
                largest_tc_trader_id = pos_trader_id

        return largest_tc_trader_id

    def find_currently_copied_trader_id(self):
        currently_copied_traders = []
        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for trader_id in db_positions:
            for current_pos in db_positions[trader_id]:
                current_pos_is_copied = current_pos["is_copied"]
                current_pos_is_ignored = current_pos["is_ignored"]
                current_pos_is_closed = current_pos["is_closed"]
                if not current_pos_is_ignored and current_pos_is_copied and not current_pos_is_closed:
                    currently_copied_traders.append(current_pos["trader_id"])

        if not currently_copied_traders:
            return None
        else:
            currently_copied_traders_unique = list(set(currently_copied_traders))
            if len(currently_copied_traders_unique) == 1:
                return currently_copied_traders_unique[0]
            else:
                msg = 'Found more than 1 currently copied trader:'
                logger.warning(msg)
                logger.warning(currently_copied_traders_unique)
                telegram_bot.send_telegram_message(msg=msg)
                return False
            
    def ignore_all_traders_except_these(self, except_trader_ids: list):
        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for trader_id in db_positions:
            if trader_id in except_trader_ids:
                continue
            for current_pos in db_positions[trader_id]:
                table_position_id = current_pos["id"]  # primary key
                current_pos_is_ignored = current_pos["is_ignored"]
                if not current_pos_is_ignored:
                    logger.debug(f"Ignoring trader ID's {trader_id} position table ID {table_position_id}")
                    data = {"is_ignored": 1, "is_ignored_reason": "lower kc"}
                    self.db.update_data(
                        table=self.position_table_name,
                        data=data,
                        condition_column="id",
                        condition_value=table_position_id
                    )

    def close_cancel_ignore_trader_id(self, trader_id: str):
        db_positions_to_close = []
        db_positions_to_cancel = []
        db_positions_to_ignore = []

        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for current_pos in db_positions[trader_id]:
            current_pos_id = current_pos["position_id"]
            current_pos_is_filled = current_pos["is_filled"]

            current_pos["is_ignored"] = 1
            current_pos["is_ignored_reason"] = "lower kc"

            if current_pos_is_filled:
                db_positions_to_close.append(current_pos)
            elif current_pos_id and not current_pos_is_filled:
                db_positions_to_cancel.append(current_pos)
            else:
                db_positions_to_ignore.append(current_pos)

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": db_positions_to_cancel}
        canceled_orders = trader.cancel_multi_orders_v2(metadata=metadata)
        for canceled_order_res in canceled_orders:
            status, result = canceled_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                position = {
                    "is_ignored": result.get("is_ignored"),
                    "is_ignored_reason": result.get("is_ignored_reason"),
                    "is_canceled": 1
                }
                logger.debug(f"Successfully canceled table position ID: {position_table_id}")
                self.db.update_data(
                    table=self.position_table_name, 
                    data=position, 
                    condition_column="id", 
                    condition_value=position_table_id
                )

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"orders": db_positions_to_close}
        closed_orders = trader.close_multi_orders_v2(metadata=metadata)
        for closed_order_res in closed_orders:
            status, result = closed_order_res
            if not status:
                # logger.error(f"Failed order. {result}")
                continue
            position_table_id = result.get("position_table_id")
            if position_table_id:
                new_amount_user = (
                    float(result.get("amount_user")) - float(result.get("amount"))
                )   # original user amount - amount that was closed
                position = {
                    "is_ignored": result.get("is_ignored"),
                    "is_ignored_reason": result.get("is_ignored_reason"),
                    "amount_user": new_amount_user,
                    "is_closed": 1
                }
                # logger.debug(f"Successfully closed table position ID: {position_table_id}")
                self.db.update_data(
                    table=self.position_table_name, 
                    data=position, 
                    condition_column="id", 
                    condition_value=position_table_id
                )

        for position in db_positions_to_ignore:
            position_table_id = position["id"]
            self.db.update_data(
                table=self.position_table_name,
                data=position,
                condition_column="id",
                condition_value=position_table_id
            )

    def copy_trader_id(self, trader_id: str):
        kc_stats = self.db.get_all_traders_kc_stats(kc_stats_table_name=self.kc_stats_table_name)

        already_existing_positions = []
        positions_to_open = []

        db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for current_pos in db_positions[trader_id]:
            current_pos_is_canceled = current_pos["is_canceled"]
            current_pos_is_closed = current_pos["is_closed"]
            current_pos_id = current_pos["position_id"]
            current_pos_is_ignored = current_pos["is_ignored"]

            # it means this position is still not copied and not ignored
            if not current_pos_id and not current_pos_is_ignored:
                positions_to_open.append(current_pos)

            # it means this position is already copied
            elif current_pos_id and not current_pos_is_canceled and not current_pos_is_closed:
                already_existing_positions.append(current_pos)

        if not positions_to_open:
            logger.debug("No positions to open.")
            return True

        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)

        metadata = {
            "allocation_of_total_balance": ALLOCATION_OF_TOTAL_BALANCE_PERC,
            "allocation_per_single_position": ALLOCATION_PER_SINGLE_POSITION_PERC,
        }
        res = trader.calc_balance_availability(metadata=metadata)
        status, result = res[0]
        if not status:
            logger.error(result)
            return None
        
        penalties = self.db.get_all_traders_penalties(top_x_table_name=self.position_table_name)
        penalty_value = penalties.get(trader_id)

        trader_kc_val = kc_stats[trader_id] if kc_stats[trader_id] <= 1 else 1
        if penalty_value:  # apply penalty to trader KC value
            trader_kc_val = trader_kc_val / penalty_value

        # trader_kc_val = kc_stats[trader_id] if kc_stats[trader_id] <= 1 else 1 # FIX THIS SHIT
        balance_to_use_for_trading_in_usdt_kc = int(result["balance_to_use_for_trading_in_usdt"] * float(trader_kc_val))
        # free_balance_to_use_for_trading_in_usdt = result["free_balance_to_use_for_trading_in_usdt"]
        # if balance_to_use_for_trading_in_usdt_kc > free_balance_to_use_for_trading_in_usdt:
        #     logger.error(
        #         f"Balance to use is more than free balance. "
        #         f"{balance_to_use_for_trading_in_usdt_kc} > {free_balance_to_use_for_trading_in_usdt}"
        #     )
        #     return None

        usdt_per_single_position = (
                balance_to_use_for_trading_in_usdt_kc // (len(positions_to_open) + len(already_existing_positions))
        )

        unique_symbols = list(set([i["symbol"] for i in positions_to_open + already_existing_positions]))
        metadata = {
            "symbols": unique_symbols
        }

        last_prices = trader.get_last_prices_for_symbols(metadata=metadata)  # {"BTCUSDT": 26157, "BNBUSDT": 258, ...}
        min_qty_and_step_sizes = trader.get_min_qty_and_step_size_for_symbols(metadata=metadata)

        # Rebalancing existing positions

        # then it means we need to rebalance (cancel and reopen or/and partially close existing positions)
        if already_existing_positions and positions_to_open:
            db_positions_to_cancel_and_reopen = []
            db_positions_to_partially_close = []
            for pos in already_existing_positions:
                pos_symbol = pos["symbol"]
                pos_is_filled = pos["is_filled"]
                if pos_is_filled:
                    pos_value_in_usdt = (pos["amount_user"] * last_prices[pos["symbol"]]) / pos["leverage"]
                    usdt_value_to_close = pos_value_in_usdt - usdt_per_single_position
                    if usdt_value_to_close > 0:
                        pos["quantity_to_close"] = (usdt_value_to_close * pos["leverage"]) / last_prices[pos["symbol"]]
                        step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                        quantity_to_close = pos["quantity_to_close"]
                        quantity_to_close_fixed = math.floor(quantity_to_close / step_size) * step_size
                        if quantity_to_close_fixed > pos["amount_user"]:
                            quantity_to_close_fixed = pos["amount_user"]
                        pos["quantity_to_close"] = quantity_to_close_fixed
                        db_positions_to_partially_close.append(pos)

                else:  # not filled

                    # using old price because psition is still not filled and get original usdt value of the position
                    pos_value_in_usdt = pos["amount_user"] * pos["entry_price"]

                    # then we need to close the current position and open with a lower amount
                    if pos_value_in_usdt > usdt_per_single_position:
                        pos_symbol = pos["symbol"]
                        amount_user = usdt_per_single_position / pos["entry_price"]
                        min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                        step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                        amount_user_fixed = convert_amount(
                            user_amount=amount_user,
                            min_qty=min_qty,
                            step_size=step_size,
                            entry_price=pos["entry_price"]
                        )
                        pos["amount_user"] = amount_user_fixed
                        db_positions_to_cancel_and_reopen.append(pos)
            
            # Partially closing positions ### HERE
            metadata = {"orders": db_positions_to_partially_close}
            logger.warning("XXX TESTING")  # Remove later
            for pos in db_positions_to_partially_close:  # Remove later
                logger.debug(f"This position will be partially closed: {pos}")  # Remove later
            logger.warning("XXX TESTING")  # Remove later
            partialy_closed_orders = trader.partially_close_multi_orders_v2(metadata=metadata)
            for partially_closed_order_res in partialy_closed_orders:
                status, result = partially_closed_order_res  # result == order
                if not status:
                    # logger.error(f"Failed order. {result}")
                    continue
                position_table_id = result.get("position_table_id")
                if position_table_id:
                    # original user amount - amount that was reduced
                    new_amount_user = result.get("amount_user") - result.get("amount")

                    position = {
                        "amount_user": new_amount_user,  # update amount_user of partially closed
                    }
                    self.db.update_data(
                        table=self.position_table_name,
                        data=position,
                        condition_column="id",
                        condition_value=position_table_id
                    )

            # Canceling still not filled positions and reopening again with adjusted quantity
            metadata = {"orders": db_positions_to_cancel_and_reopen}
            canceled_orders = trader.cancel_multi_orders_v2(metadata=metadata)
            for canceled_order_res in canceled_orders:
                status, result = canceled_order_res
                if not status:
                    # logger.error(f"Failed order. {result}")
                    continue
                position_table_id = result.get("position_table_id")
                if position_table_id:
                    position = {
                        "is_canceled": 1
                    }
                    logger.debug(
                        f"Successfully canceled (and soon will try to reopen) table position ID: {position_table_id}"
                    )
                    self.db.update_data(
                        table=self.position_table_name,
                        data=position,
                        condition_column="id",
                        condition_value=position_table_id
                    )
            
            # After canceling still not filled positions - we try to reopen it with adjusted quantity
            metadata = {"orders": db_positions_to_cancel_and_reopen}
            opened_orders = trader.open_multi_orders(metadata=metadata)
            for opened_order_res in opened_orders:
                status, result = opened_order_res
                if not status:
                    logger.error(f"Failed order. {result}")
                    continue
                position_table_id = result.get("position_table_id")
                if position_table_id:
                    position = {
                        "is_copied": 1,
                        "is_canceled": 0,
                        "position_id": result["info"]["orderId"],  # update previous position with the new position ID
                    }
                    self.db.update_data(
                        table=self.position_table_name,
                        data=position,
                        condition_column="id",
                        condition_value=position_table_id
                    )
        
        # Opening new positions
        if positions_to_open:
            # update entry prices
            for dict_i in positions_to_open:
                dict_i["entry_price"] = (
                    last_prices[dict_i["symbol"]]
                    if last_prices.get(dict_i["symbol"])
                    else dict_i["entry_price"]
                )

            # update amount_user
            for dict_i in positions_to_open:
                position_table_id = dict_i["id"]
                if min_qty_and_step_sizes:
                    pos_symbol = dict_i["symbol"]
                    amount_user = (usdt_per_single_position / dict_i["entry_price"]) * dict_i["leverage"]
                    min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                    step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                    amount_user_fixed = convert_amount(
                        user_amount=amount_user,
                        min_qty=min_qty,
                        step_size=step_size,
                        entry_price=dict_i["entry_price"]
                    )
                else:
                    amount_user_fixed = (usdt_per_single_position / dict_i["entry_price"]) * dict_i["leverage"]
                dict_i["amount_user"] = amount_user_fixed
                self.db.update_data(
                    table=self.position_table_name,
                    data=dict_i,
                    condition_column="id",
                    condition_value=position_table_id
                )
                
            metadata = {"orders": positions_to_open}
            opened_orders = trader.open_multi_orders(metadata=metadata)
            for opened_order_res in opened_orders:
                status, result = opened_order_res
                if not status:
                    logger.error(f"Failed order. {result}")
                    continue
                position_table_id = result.get("position_table_id")
                if position_table_id:
                    position = {
                        "is_copied": 1,
                        "position_id": result["info"]["orderId"],
                        # TO-DO: you might also need to update user_amount
                    }
                    self.db.update_data(
                        table=self.position_table_name,
                        data=position,
                        condition_column="id",
                        condition_value=position_table_id
                    )

    def update_liquidation_prices(self):  
        logger.debug("Updating liquidation prices")

        symbols = []
        all_db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for db_trader_id in all_db_positions:
            db_positions = all_db_positions[db_trader_id]
            for db_position in db_positions:
                is_filled = db_position["is_filled"]
                if is_filled:
                    symbols.append(db_position["inst_id"].split("-")[0])

        symbols = list(set(symbols))
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)

        metadata = {"symbols": symbols}
        res = trader.get_liquidation_prices(metadata=metadata)
        status, result = res
        if not status:
            logger.error(result)
            return None
        
        for db_trader_id in all_db_positions:
            db_positions = all_db_positions[db_trader_id]
            for db_position in db_positions:
                okx_pos_table_id = db_position["okx_pos_id"]
                bin_pos_id = db_position["bin_pos_id"]
                symbol = db_position["inst_id"].split("-")[0]
                is_filled = db_position["is_filled"]
                pos_liquidation_price = db_position["user_liquidation_price"]
                if not is_filled:
                    continue
                current_liquidation_price = result.get(symbol)
                if pos_liquidation_price == current_liquidation_price:  # then no need to update
                    continue
                logger.debug(
                    f"Updating liquidation price for position ID: {bin_pos_id}. "
                    f"From: {pos_liquidation_price}, "
                    f"to: {current_liquidation_price}"
                )
                data_to_update = {"user_liquidation_price": current_liquidation_price}
                self.db.update_data(
                    table=self.position_table_name,
                    data=data_to_update,
                    condition_column="okx_pos_id",
                    condition_value=okx_pos_table_id
                )

    def insert_or_update_stop_losses(self):  
        logger.debug("Inserting/updating stop losses")

        positions_that_needs_sl = {}
        all_db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for db_trader_id in all_db_positions:
            db_positions = all_db_positions[db_trader_id]
            for db_position in db_positions:
                bin_pos_id = db_position["bin_pos_id"]
                is_closed = db_position["is_closed"]
                is_filled = db_position["is_filled"]
                liquidation_price = db_position["user_liquidation_price"]
                if is_filled and liquidation_price and not is_closed:
                    positions_that_needs_sl[bin_pos_id] = db_position

        # retrieve all active SL positions
        all_active_stop_losses = self.db.get_all_active_stop_losses(position_table=self.position_table_name)
        
        sls_to_cancel = []
        for orig_position_id in all_active_stop_losses:
            stop_loss_pos = all_active_stop_losses[orig_position_id]
            orig_position_id = stop_loss_pos["orig_position_id"]
            if orig_position_id not in positions_that_needs_sl:
                sls_to_cancel.append(stop_loss_pos)

        # cancel all stop-losses that are not relevant anymore
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"positions": sls_to_cancel}
        canceled_sls = trader.cancel_sls(metadata=metadata)

        for canceled_sl_res in canceled_sls:
            status, result = canceled_sl_res
            if not status:
                continue
            if result.get("status") == "canceled" or result.get("status") == "OrderNotFound":
                sl_table_id = result.get("sl_table_id")
                position = {
                    "is_active": 0,
                }
                self.db.update_data(
                    table="stop_losses", data=position, condition_column="id", condition_value=sl_table_id
                )
                logger.debug(f"Successfully canceled (not relevant anymore) stop-loss ID: {sl_table_id}")

        # retrieve all active SL positions

        # REMOVE LATER
        # all_active_stop_losses = self.db.get_all_active_stop_losses(position_table=self.position_table_name)

        # retrieve all active positions SL positions
        all_active_pos_stop_losses = self.db.get_all_active_pos_stop_losses(position_table=self.position_table_name)
        
        sls_to_cancel = []
        sls_pos_to_update = []
        sls_pos_to_open = []
        for bin_pos_id in positions_that_needs_sl:
            position = positions_that_needs_sl[bin_pos_id]  # entry position (not SL position)
            pos_side = position["pos_side"]
            if pos_side == "long":
                current_pos_sl_price = (
                    position["open_avg_px"] - ((position["open_avg_px"] - position["liquidation_price"]) * SL_RATIO)
                )
            else:
                current_pos_sl_price = (
                    position["open_avg_px"] + ((position["liquidation_price"] - position["open_avg_px"]) * SL_RATIO)
                )
            current_pos_amount = position["user_amount"]

            # SL exists for 'bin_pos_id' but we still don't know if it's active or not or even needs to be updated
            if bin_pos_id in all_active_pos_stop_losses:
                stop_loss_pos = all_active_pos_stop_losses[bin_pos_id]
                is_sl_active = all_active_pos_stop_losses[bin_pos_id]["is_active"]
                prev_sl_price = all_active_pos_stop_losses[bin_pos_id]["price"]

                if isinstance(prev_sl_price, float):
                    decimal_places_of_prev_sl_price = len(str(prev_sl_price).split('.')[1])
                else:
                    decimal_places_of_prev_sl_price = 0

                current_pos_sl_price = round(current_pos_sl_price, decimal_places_of_prev_sl_price)

                prev_sl_amount = all_active_pos_stop_losses[bin_pos_id]["amount"]
                if is_sl_active:
                    if prev_sl_price != current_pos_sl_price or prev_sl_amount != current_pos_amount:
                        max_perc_diff = 1
                        sl_prices_perc_diff = calc_perc_diff_between_x_y(x=prev_sl_price, y=current_pos_sl_price)
                        sl_amount_perc_diff = calc_perc_diff_between_x_y(x=prev_sl_amount, y=current_pos_amount)
                        if sl_prices_perc_diff > max_perc_diff or sl_amount_perc_diff > max_perc_diff:
                            # we will need to cancel current SL and open again
                            sls_to_cancel.append(stop_loss_pos)
                            sls_pos_to_update.append(position)
                else:
                    # we will need to open again (no need to cancel because it's already currently not active)
                    sls_pos_to_update.append(position)
            else:
                sls_pos_to_open.append(position)
        
        # cancel all stop-losses that needs to be updated (reopened)
        metadata = {"positions": sls_to_cancel}
        canceled_sls = trader.cancel_sls(metadata=metadata)
        for canceled_sl_res in canceled_sls:
            status, result = canceled_sl_res
            if not status:
                continue
            sl_table_id = result.get("sl_table_id")
            if sl_table_id:
                position = {
                    "is_active": 0,
                }
                self.db.update_data(
                    table="stop_losses", data=position, condition_column="id", condition_value=sl_table_id
                )
                logger.debug(f"Successfully canceled stop-loss ID: {sl_table_id}")

        # reopen (update) previously canceled (or innactive) stop-losses
        metadata = {"positions": sls_pos_to_update, "sl_ratio": SL_RATIO}
        created_sls = trader.create_sls(metadata=metadata)
        for created_sl_res in created_sls:
            status, result = created_sl_res
            if not status:
                continue
            orig_position_id = result.get("orig_position_id")
            if orig_position_id:  # then it means we need to update existing stop-loss
                sl_position = {
                    "position_id": result["id"],  # SL position ID
                    "is_active": 1,

                    # using 'amount' because the 'stop_losses' table uses 'amount' column instead of 'amount_user'
                    "amount": result["amount_user"],
                    "price": result["stopPrice"],
                }
                sl_table_id = all_active_pos_stop_losses[orig_position_id]["id"]
                self.db.update_data(
                    table="stop_losses", data=sl_position, condition_column="id", condition_value=sl_table_id
                )
                logger.debug(f"Successfully created (reopened) stop-loss ID: {sl_table_id}")
        
        # insert new stop-losses
        metadata = {"positions": sls_pos_to_open, "sl_ratio": SL_RATIO}
        created_sls = trader.create_sls(metadata=metadata)
        for created_sl_res in created_sls:
            status, result = created_sl_res
            if not status:
                continue
            orig_position_id = result.get("orig_position_id")
            if orig_position_id:  # then it means we need to insert a new stop-loss
                sl_position = {
                    "position_table": self.position_table_name,
                    "orig_position_id": orig_position_id,
                    "position_id": result["id"],
                    "symbol": result["symbol"],
                    "position_type": result["position_type"],
                    "side": result["side"],
                    "price": result["stopPrice"],
                    "amount": result["amount"],
                }
                sl_table_id = self.db.insert_data(table="stop_losses", data=sl_position)
                if sl_table_id:
                    logger.debug(f"Successfully created a new stop-loss ID: {sl_table_id}")

    def check_and_update_filled_sls(self):  
        logger.debug("Checking if any stop-losses got triggered")

        # retrieve all active SL positions
        all_active_pos_stop_losses = self.db.get_all_active_pos_stop_losses(position_table=self.position_table_name)
        symbols = [all_active_pos_stop_losses[orig_pos_id]["symbol"] for orig_pos_id in all_active_pos_stop_losses]
        sls_ids = [all_active_pos_stop_losses[orig_pos_id]["position_id"] for orig_pos_id in all_active_pos_stop_losses]

        metadata = {"symbols": symbols, "sls_ids": sls_ids}
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        triggered_sls_ids = trader.get_triggered_sls_for_multi_symbols(metadata=metadata)

        for sl_pos_id in triggered_sls_ids:
            sl_position = {
                "is_active": 0,
                "is_filled": 1,
            }
            logger.debug(f"Trying to update SL (set as filled) for SL position ID: {sl_pos_id}")
            self.db.update_data(
                table="stop_losses", data=sl_position, condition_column="position_id", condition_value=sl_pos_id
            )
            
            # you also need to update position and set as closed (because SL got triggered)
            bin_pos_id = None
            for orig_pos_id in all_active_pos_stop_losses:
                sl_pos_dict_i = all_active_pos_stop_losses[orig_pos_id]
                if sl_pos_dict_i["position_id"] == sl_pos_id:
                    bin_pos_id = sl_pos_dict_i["orig_position_id"]
                    break

            trading_position = {
                "is_closed": 1,
                "amount_user": 0 
            }
            self.db.update_data(
                table=self.position_table_name,
                data=trading_position,
                condition_column="bin_pos_id",
                condition_value=bin_pos_id
            )

            trader_id = self.db.get_trader_id_by_position_id(
                position_table=self.position_table_name, position_id=bin_pos_id
            )
            if trader_id:
                logger.debug(f"Trying to insert/update SL penalty for 'trader_id': {trader_id}")
                self.db.insert_or_update_penalty(top_x_table_name=self.position_table_name, trader_id=trader_id)
            else:
                logger.error(f"Unable to retrieve 'trader_id' for Binance trading position ID: ({bin_pos_id})")

    def check_and_update_filled_tps(self):  
        logger.debug("Checking if any take-profits got triggered")

        # retrieve all active TP positions
        all_active_pos_take_profits = self.db.get_all_active_pos_take_profits(position_table=self.position_table_name)
        symbols = [
            all_active_pos_take_profits[orig_pos_id]["symbol"]
            for orig_pos_id in all_active_pos_take_profits
        ]
        tps_ids = [
            all_active_pos_take_profits[orig_pos_id]["position_id"]
            for orig_pos_id in all_active_pos_take_profits
        ]

        metadata = {"symbols": symbols, "tps_ids": tps_ids}
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        triggered_tps_ids = trader.get_triggered_tps_for_multi_symbols(metadata=metadata)

        for tp_pos_id in triggered_tps_ids:
            tp_position = {
                "is_active": 0,
                "is_filled": 1,
            }
            logger.debug(f"Trying to update TP (set as filled) for TP position ID: {tp_pos_id}")
            # originaly it was condition_column="orig_position_id":
            self.db.update_data(
                table="take_profits", data=tp_position, condition_column="position_id", condition_value=tp_pos_id
            )
            
            # you also need to update position and set as closed (because TP got triggered)
            bin_pos_id = None
            for orig_pos_id in all_active_pos_take_profits:
                tp_pos_dict_i = all_active_pos_take_profits[orig_pos_id]
                if tp_pos_dict_i["position_id"] == tp_pos_id:
                    bin_pos_id = tp_pos_dict_i["orig_position_id"]
                    break

            trading_position = {
                "is_closed": 1,
                "amount_user": 0 
            }
            self.db.update_data(
                table=self.position_table_name,
                data=trading_position,
                condition_column="bin_pos_id",
                condition_value=bin_pos_id
            )

    def insert_or_update_take_profits(self):  
        logger.debug("Inserting/updating take profits")

        positions_that_needs_tp = {}
        all_db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)
        for db_trader_id in all_db_positions:
            db_positions = all_db_positions[db_trader_id]
            for db_position in db_positions:
                bin_pos_id = db_position["bin_pos_id"]
                is_closed = db_position["is_closed"]
                is_filled = db_position["is_filled"]
                if is_filled and not is_closed:
                    positions_that_needs_tp[bin_pos_id] = db_position

        # retrieve all active TP positions
        all_active_take_profits = self.db.get_all_active_take_profits(position_table=self.position_table_name)
        
        tps_to_cancel = []
        for orig_position_id in all_active_take_profits:
            take_profit_pos = all_active_take_profits[orig_position_id]
            orig_position_id = take_profit_pos["orig_position_id"]
            if orig_position_id not in positions_that_needs_tp:
                tps_to_cancel.append(take_profit_pos)

        # cancel all take-profits that are not relevant anymore
        trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
        metadata = {"positions": tps_to_cancel}
        canceled_tps = trader.cancel_tps(metadata=metadata)

        for canceled_tp_res in canceled_tps:
            status, result = canceled_tp_res
            if not status:
                continue
            if result.get("status") == "canceled" or result.get("status") == "OrderNotFound":
                tp_table_id = result.get("tp_table_id")
                position = {
                    "is_active": 0,
                }
                self.db.update_data(
                    table="take_profits", data=position, condition_column="id", condition_value=tp_table_id
                )
                logger.debug(f"Successfully canceled (not relevant anymore) take-profit ID: {tp_table_id}")

        # retrieve all active TP positions
        all_active_take_profits = self.db.get_all_active_take_profits(position_table=self.position_table_name)
        
        tps_to_cancel = []
        tps_pos_to_update = []
        tps_pos_to_open = []
        for position_id in positions_that_needs_tp:
            position = positions_that_needs_tp[position_id]  # entry position (not TP position)
            pos_leverage = position["lever"]
            pos_side = position["pos_side"]
            # CALCULATE TP PRICE USING STANDARD DEVIATION + AVG ROI
            trader_kc_data = self.db.get_trader_kc_table_data(
                kc_stats_table_name=self.kc_stats_table_name, trader_id=position["trader_id"]
            )
            if not trader_kc_data:
                logger.warning(f"Unable to retrieve 'trader_kc_data' for the trader ID: {position['trader_id']}")
                continue
            trader_std_dev = trader_kc_data.get("roe_std_dev")
            trader_avg_roe = trader_kc_data.get("avg_roe")
            tp_perc = (float((trader_avg_roe * 100) + (trader_std_dev * 100))) / pos_leverage
            if pos_side == "long":
                current_pos_tp_price = position["open_avg_px"] + ((position["open_avg_px"] / 100) * tp_perc)
            else:
                current_pos_tp_price = position["open_avg_px"] - ((position["open_avg_px"] / 100) * tp_perc)
                if current_pos_tp_price < 0:  # TP price can't be nagative
                    current_pos_tp_price = 0
            current_pos_amount = position["user_amount"]

            if isinstance(position["open_avg_px"], float):
                decimal_places_of_tp_price = len(str(position["open_avg_px"]).split('.')[1])
            else:
                decimal_places_of_tp_price = 0
            current_pos_tp_price = round(current_pos_tp_price, decimal_places_of_tp_price)

            position["tp_price"] = current_pos_tp_price

            if position_id in all_active_take_profits:
                take_profit_pos = all_active_take_profits[position_id]
                prev_tp_price = all_active_take_profits[position_id]["price"]
                prev_tp_amount = all_active_take_profits[position_id]["amount"]
                if prev_tp_price != current_pos_tp_price or prev_tp_amount != current_pos_amount:
                    tps_to_cancel.append(take_profit_pos)
                    tps_pos_to_update.append(position)
            else:
                tps_pos_to_open.append(position)
        
        # cancel all take-profits that needs to be updated (reopened)
        metadata = {"positions": tps_to_cancel}
        canceled_tps = trader.cancel_tps(metadata=metadata)
        for canceled_tp_res in canceled_tps:
            status, result = canceled_tp_res
            if not status:
                continue
            tp_table_id = result.get("tp_table_id")
            if tp_table_id:
                position = {
                    "is_active": 0,
                }
                self.db.update_data(
                    table="take_profits", data=position, condition_column="id", condition_value=tp_table_id
                )
                logger.debug(f"Successfully canceled take-profit ID: {tp_table_id}")

        # reopen (update) previously canceled take-profits
        metadata = {"positions": tps_pos_to_update}
        created_tps = trader.create_tps(metadata=metadata)
        for created_tp_res in created_tps:
            status, result = created_tp_res
            if not status:
                continue
            orig_position_id = result.get("orig_position_id")
            if orig_position_id:  # then it means we need to update existing take-profit
                tp_position = {
                    "position_id": result["id"],  # TP position ID
                    "is_active": 1,

                    # using 'amount' because the 'take_profit' table is using 'amount' column instead of 'user_amount'
                    "amount": result["user_amount"],
                    "price": result["stopPrice"],
                }
                tp_table_id = all_active_take_profits[orig_position_id]["id"]
                self.db.update_data(
                    table="take_profits", data=tp_position, condition_column="id", condition_value=tp_table_id
                )
                logger.debug(f"Successfully created (reopened) take-profit ID: {tp_table_id}")
        
        # insert new take-profits
        metadata = {"positions": tps_pos_to_open}
        created_tps = trader.create_tps(metadata=metadata)
        for created_tp_res in created_tps:
            status, result = created_tp_res
            if not status:
                continue
            orig_position_id = result.get("orig_position_id")
            if orig_position_id:  # then it means we need to insert a new take-profit
                tp_position = {
                    "position_table": self.position_table_name,
                    "orig_position_id": orig_position_id,
                    "position_id": result["id"],
                    "symbol": result["symbol"],
                    "position_type": result["position_type"],
                    "side": result["side"],
                    "price": result["stopPrice"],
                    "amount": result["amount"],
                }
                tp_table_id = self.db.insert_data(table="take_profits", data=tp_position)
                if tp_table_id:
                    logger.debug(f"Successfully created a new take-profit ID: {tp_table_id}")
    
    def try_to_find_same_position(self, single_position: dict, versus_positions: list, single_position_type: str):
        valid_single_position_types = ["api", "db"]
        if single_position_type not in valid_single_position_types:
            raise Exception(f"Invalid single_position_type: {single_position_type}")
        
        result = {
            "same_position": False,
            "need_to_update_amount": False,
            "position": None,
        }

        for versus_position in versus_positions:
            # get API position data
            single_position_trader_id = single_position["trader_id"]
            single_position_symbol = single_position["symbol"]
            single_position_side = single_position["side"]
            single_position_leverage = single_position["leverage"]
            single_position_entry_price = single_position["entry_price"]
            single_position_amount = single_position["amount"]
            single_position_update_timestamp = single_position["update_timestamp"]
            # get DB position date
            versus_position_trader_id = versus_position["trader_id"]
            versus_position_symbol = versus_position["symbol"]
            versus_position_side = versus_position["side"]
            versus_position_leverage = versus_position["leverage"]
            versus_position_entry_price = versus_position["entry_price"]
            versus_position_amount = versus_position["amount"]
            versus_position_update_timestamp = versus_position["update_timestamp"]

            if single_position_type == "api":
                amount_comparing_logic = single_position_amount < versus_position_amount
            else:  # db
                amount_comparing_logic = single_position_amount > versus_position_amount

            if (
                single_position_trader_id == versus_position_trader_id
                and single_position_symbol == versus_position_symbol
                and single_position_side == versus_position_side
                # and single_position_leverage == versus_position_leverage # leverage can be changed
                and single_position_entry_price == versus_position_entry_price
                and amount_comparing_logic
            ):  # It means the trader partially closed his position
                result["same_position"] = True
                result["need_to_update_amount"] = True  # decrese quantity
                result["position"] = versus_position
                return result
            elif (
                single_position_trader_id == versus_position_trader_id
                and single_position_symbol == versus_position_symbol
                and single_position_side == versus_position_side
                # and single_position_leverage == versus_position_leverage # leverage can be changed
                and single_position_entry_price == versus_position_entry_price
                and single_position_amount == versus_position_amount
                and single_position_update_timestamp == versus_position_update_timestamp
            ):  # It means nothing has happened
                result["same_position"] = True
                result["position"] = versus_position
                return result
            elif (
                single_position_trader_id == versus_position_trader_id
                and single_position_symbol == versus_position_symbol
                and single_position_side == versus_position_side
                and single_position_leverage == versus_position_leverage
                # and single_position_entry_price != versus_position_entry_price # different entry price

                # different amount (probably the trader increased his position)
                # and single_position_amount != versus_position_amount
                # and single_position_update_timestamp != versus_position_update_timestamp # different entry price
            ):  # Something has happened but we asume it's the same position
                result["same_position"] = True
                result["position"] = versus_position
                return result
            elif (
                single_position_trader_id == versus_position_trader_id
                and single_position_symbol == versus_position_symbol
                and single_position_side == versus_position_side
                # and single_position_leverage == versus_position_leverage
                # and single_position_entry_price != versus_position_entry_price # different entry price

                # different amount (probably the trader increased his position)
                # and single_position_amount != versus_position_amount
                and single_position_update_timestamp == versus_position_update_timestamp
            ):  # Something has happened but we asume it's the same position
                result["same_position"] = True
                result["position"] = versus_position
                return result
        
        # not the same position
        return result
    
    def handle_copy_multi_trader_positions(self):
        if self.should_copy_positions:
            # get all traders you might be start copying
            trader_ids_that_will_be_copied = self.db.fetch_active_non_ignored_trader_ids_to_copy(
                table=self.position_table_name
            )

            # calculate total kelly criteria for those traders
            total_kc = self.db.calculate_total_kc(
                top_x_table_name=self.position_table_name, trader_ids=trader_ids_that_will_be_copied
            )
            total_kc = total_kc if total_kc <= 1 else 1

            # allocate balance that will be used (100 KC = 100% of total balance)
            trader = TradingAPI(api_key=binance_api_key, api_secret=binance_api_secret)
            metadata = {
                "allocation_of_total_balance": ALLOCATION_OF_TOTAL_BALANCE_PERC,
                "allocation_per_single_position": ALLOCATION_PER_SINGLE_POSITION_PERC,
            }
            res = trader.calc_balance_availability(metadata=metadata)
            status, result = res[0]
            if not status:
                logger.error(result)
                return None
            
            balance_to_use_for_trading_in_usdt_kc = int(result["balance_to_use_for_trading_in_usdt"] * float(total_kc))

            # get every traders' (which we will be copying) KC
            kc_stats = self.db.get_all_traders_kc_stats(kc_stats_table_name=self.kc_stats_table_name)
            # kc_stats = {trader_id: kc_val if kc_val <= 1 else 1 for trader_id, kc_val in kc_stats.items()}

            # allocate balance by every trader KC value
            # (if the sum of all traders' KC value exceeds the total KC - use proportions)
            all_traders_kc_sum = sum([kc_stats[trader_id] for trader_id in trader_ids_that_will_be_copied])
            weighted_allocation = False
            if all_traders_kc_sum > 1:
                weighted_allocation = True

            for trader_id in trader_ids_that_will_be_copied:
                trader_kc_val = kc_stats[trader_id]
                if weighted_allocation:
                    logger.debug("Using weighted balance allocation")
                    trader_weighted_kc_val = trader_kc_val / all_traders_kc_sum
                    kc_stats[trader_id] = trader_weighted_kc_val

            # divide individual KCs by positions counts
            db_positions = self.db.fetch_active_non_ignored_positions(table=self.position_table_name)  # FIX THAT

            already_existing_positions = []
            positions_to_open = []

            for current_pos in db_positions[trader_id]:
                current_pos_is_canceled = current_pos["is_canceled"]
                current_pos_is_closed = current_pos["is_closed"]
                current_pos_id = current_pos["position_id"]
                current_pos_is_ignored = current_pos["is_ignored"]

                # it means this position is still not copied and not ignored
                if not current_pos_id and not current_pos_is_ignored:
                    positions_to_open.append(current_pos)

                # it means this position is already copied
                elif current_pos_id and not current_pos_is_canceled and not current_pos_is_closed:
                    already_existing_positions.append(current_pos)

            unique_symbols = list(set([i["symbol"] for i in positions_to_open + already_existing_positions]))
            metadata = {
                "symbols": unique_symbols
            }

            # {"BTCUSDT": 26157, "BNBUSDT": 258, ...}
            last_prices = trader.get_last_prices_for_symbols(metadata=metadata)

            min_qty_and_step_sizes = trader.get_min_qty_and_step_size_for_symbols(metadata=metadata)

            # Rebalancing existing positions

            # then it means we need to rebalance (cancel and reopen or/and partially close existing positions)
            if already_existing_positions and positions_to_open:
                db_positions_to_cancel_and_reopen = []
                db_positions_to_partially_close = []
                for pos in already_existing_positions:
                    trader_id = pos["trader_id"]
                    trader_kc_val = kc_stats[trader_id]
                    pos_symbol = pos["symbol"]
                    pos_is_filled = pos["is_filled"]
                    if pos_is_filled:
                        pos_count_of_a_trader = len(db_positions[trader_id])
                        max_pos_value_in_usdt = (
                            balance_to_use_for_trading_in_usdt_kc * (trader_kc_val / pos_count_of_a_trader)
                        )
                        current_pos_value_in_usdt = (pos["amount_user"] * last_prices[pos["symbol"]]) / pos["leverage"]
                        usdt_value_to_close = current_pos_value_in_usdt - max_pos_value_in_usdt
                        if usdt_value_to_close > 0:
                            pos["quantity_to_close"] = (
                                (usdt_value_to_close * pos["leverage"]) / last_prices[pos["symbol"]]
                            )
                            step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                            quantity_to_close = pos["quantity_to_close"]
                            quantity_to_close_fixed = math.floor(quantity_to_close / step_size) * step_size
                            if quantity_to_close_fixed > pos["amount_user"]:
                                quantity_to_close_fixed = pos["amount_user"]
                            pos["quantity_to_close"] = quantity_to_close_fixed
                            db_positions_to_partially_close.append(pos)
                    else:  # not filled
                        max_pos_value_in_usdt = balance_to_use_for_trading_in_usdt_kc * trader_kc_val

                        # using old price because position is not filled and get original usdt value of the position
                        pos_value_in_usdt = pos["amount_user"] * pos["entry_price"]

                        # then we need to close the current position and open with a lower amount
                        if pos_value_in_usdt > max_pos_value_in_usdt:
                            pos_symbol = pos["symbol"]
                            amount_user = max_pos_value_in_usdt / pos["entry_price"]
                            min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                            step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                            amount_user_fixed = convert_amount(
                                user_amount=amount_user,
                                min_qty=min_qty,
                                step_size=step_size,
                                entry_price=pos["entry_price"]
                            )
                            pos["amount_user"] = amount_user_fixed
                            db_positions_to_cancel_and_reopen.append(pos)
                
                # Partially closing positions ### HERE
                metadata = {"orders": db_positions_to_partially_close}
                logger.warning("XXX TESTING")  # Remove later
                for pos in db_positions_to_partially_close:  # Remove later
                    logger.debug(f"This position will be partially closed: {pos}")  # Remove later
                logger.warning("XXX TESTING")  # Remove later
                partialy_closed_orders = trader.partially_close_multi_orders_v2(metadata=metadata)
                for partially_closed_order_res in partialy_closed_orders:
                    status, result = partially_closed_order_res  # result == order
                    if not status:
                        # logger.error(f"Failed order. {result}")
                        continue
                    position_table_id = result.get("position_table_id")
                    if position_table_id:
                        # original user amount - amount that was reduced
                        new_amount_user = result.get("amount_user") - result.get("amount")

                        position = {
                            "amount_user": new_amount_user,  # update amount_user of partially closed
                        }
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=position_table_id
                        )

                # Canceling still not filled positions and reopening again with adjusted quantity
                metadata = {"orders": db_positions_to_cancel_and_reopen}
                canceled_orders = trader.cancel_multi_orders_v2(metadata=metadata)
                for canceled_order_res in canceled_orders:
                    status, result = canceled_order_res
                    if not status:
                        # logger.error(f"Failed order. {result}")
                        continue
                    position_table_id = result.get("position_table_id")
                    if position_table_id:
                        position = {
                            "is_canceled": 1
                        }
                        logger.debug(
                            f"Successfully canceled (and soon will try to reopen) table position ID: "
                            f"{position_table_id}"
                        )
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=position_table_id
                        )
                
                # After canceling still not filled positions - we try to reopen it with adjusted quantity
                metadata = {"orders": db_positions_to_cancel_and_reopen}
                opened_orders = trader.open_multi_orders(metadata=metadata)
                for opened_order_res in opened_orders:
                    status, result = opened_order_res
                    if not status:
                        logger.error(f"Failed order. {result}")
                        continue
                    position_table_id = result.get("position_table_id")
                    if position_table_id:
                        position = {
                            "is_copied": 1,
                            "is_canceled": 0,

                            # update previous position with the new position ID
                            "position_id": result["info"]["orderId"],
                        }
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=position_table_id
                        )

            # Opening new positions
            if positions_to_open:
                # update entry prices
                for dict_i in positions_to_open:
                    dict_i["entry_price"] = (
                        last_prices[dict_i["symbol"]]
                        if last_prices.get(dict_i["symbol"])
                        else dict_i["entry_price"]
                    )

                # update amount_user
                for dict_i in positions_to_open:
                    trader_id = dict_i["trader_id"]
                    trader_kc_val = kc_stats[trader_id]
                    position_table_id = dict_i["id"]
                    pos_count_of_a_trader = len(db_positions[trader_id])
                    max_pos_value_in_usdt = (
                            balance_to_use_for_trading_in_usdt_kc * (trader_kc_val / pos_count_of_a_trader)
                    )
                    if min_qty_and_step_sizes:
                        pos_symbol = dict_i["symbol"]
                        amount_user = (max_pos_value_in_usdt / dict_i["entry_price"]) * dict_i["leverage"]
                        min_qty = min_qty_and_step_sizes[pos_symbol]["min_qty"]
                        step_size = min_qty_and_step_sizes[pos_symbol]["step_size"]
                        amount_user_fixed = convert_amount(
                            user_amount=amount_user,
                            min_qty=min_qty,
                            step_size=step_size,
                            entry_price=dict_i["entry_price"]
                        )
                    else:
                        amount_user_fixed = (max_pos_value_in_usdt / dict_i["entry_price"]) * dict_i["leverage"]
                    dict_i["amount_user"] = amount_user_fixed
                    self.db.update_data(
                        table=self.position_table_name,
                        data=dict_i,
                        condition_column="id",
                        condition_value=position_table_id
                    )
                    
                metadata = {"orders": positions_to_open}
                opened_orders = trader.open_multi_orders(metadata=metadata)
                for opened_order_res in opened_orders:
                    status, result = opened_order_res
                    if not status:
                        logger.error(f"Failed order. {result}")
                        continue
                    position_table_id = result.get("position_table_id")
                    if position_table_id:
                        position = {
                            "is_copied": 1,
                            "position_id": result["info"]["orderId"],
                            # TO-DO: you might also need to update user_amount
                        }
                        self.db.update_data(
                            table=self.position_table_name,
                            data=position,
                            condition_column="id",
                            condition_value=position_table_id
                        )

            # partially close positions if a new position needs to be opened
            # if a new trader appears - you might need to partially close multiple positions of multiple traders
    
    def handle_copy_positions(self):
        if self.should_copy_positions:
            if COPY_TRADER_BY == "TC":  # TC = 'trades_count'
                largest_comparable_value_trader_id = self.find_largest_tc_trader_id()
            elif COPY_TRADER_BY == "KC":  # KC = 'kelly_criteria'
                largest_comparable_value_trader_id = self.find_largest_kc_trader_id()
            else:
                logger.error(f"Invalid 'COPY_TRADER_BY' setting: {COPY_TRADER_BY}")
                return
            
            currenty_copied_trader_id = self.find_currently_copied_trader_id()

            traders_to_keep = []
            if largest_comparable_value_trader_id:
                logger.debug(f"largest_kc_trader_id: {largest_comparable_value_trader_id}")
                traders_to_keep.append(largest_comparable_value_trader_id)
            if currenty_copied_trader_id:
                logger.debug(f"currenty_copied_trader_id: {currenty_copied_trader_id}")
                traders_to_keep.append(currenty_copied_trader_id)

            if not traders_to_keep:
                logger.debug("No active traders found that would be not ignored")
                return
            
            self.ignore_all_traders_except_these(except_trader_ids=traders_to_keep)
            
            if currenty_copied_trader_id is False:
                logger.error("Something went wrong when finding currently copied trader ID.")
                return
            
            if currenty_copied_trader_id is None:
                self.copy_trader_id(trader_id=largest_comparable_value_trader_id)
                return

            if isinstance(currenty_copied_trader_id, str):
                if currenty_copied_trader_id == largest_comparable_value_trader_id:
                    self.copy_trader_id(trader_id=largest_comparable_value_trader_id)
                else:
                    # the logic to decide which trader to copy
                    if COPY_TRADER_BY == "KC":
                        kc_stats = self.db.get_all_traders_kc_stats(kc_stats_table_name=self.kc_stats_table_name)
                        currenty_copied_trader_kc = kc_stats.get(currenty_copied_trader_id, Decimal(0))
                        largest_kc_trader_kc = kc_stats.get(largest_comparable_value_trader_id, Decimal(0))

                        # Jose said the KC ratio should be at least 20% larger to start copying a new trader
                        if currenty_copied_trader_kc * Decimal(1.2) < largest_kc_trader_kc:
                            self.close_cancel_ignore_trader_id(trader_id=currenty_copied_trader_id)
                            self.copy_trader_id(trader_id=largest_comparable_value_trader_id)
                        else:
                            self.copy_trader_id(trader_id=currenty_copied_trader_id)
                    elif COPY_TRADER_BY == "TC":
                        tc_stats = self.db.get_all_traders_tc_stats(kc_stats_table_name=self.kc_stats_table_name)
                        currenty_copied_trader_tc = tc_stats.get(currenty_copied_trader_id, 0)
                        largest_tc_trader_tc = tc_stats.get(largest_comparable_value_trader_id, 0)
                        
                        if currenty_copied_trader_tc < largest_tc_trader_tc:
                            self.close_cancel_ignore_trader_id(trader_id=currenty_copied_trader_id)
                            self.copy_trader_id(trader_id=largest_comparable_value_trader_id)
                        else:
                            self.copy_trader_id(trader_id=currenty_copied_trader_id)

    def run(self, delay: int = 5):
        max_consec_crash_count = 3
        consec_crash_count = 0
        first_time_run = True
        while True:
            try:
                config = helpers.load_config_from_yaml()
                self.should_copy_positions = config[f"{self.instance}_copy_positions"]  

                db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)  
                self.check_and_update_filled_db_orders(db_positions=db_positions)  

                self.update_liquidation_prices()  

                self.insert_or_update_stop_losses()  
                self.insert_or_update_take_profits()  

                self.check_and_update_filled_sls()  
                self.check_and_update_filled_tps()  

                api_positions = self.db.get_temp_positions_from_db(ignore_observed_traders=IGNORE_OBSERVED_TRADERS)  

                self.db.deactivate_trader_ids_of_success_stats_table(position_table_name=self.position_table_name)  
                self.db.insert_trader_ids_to_success_stats_table(position_table_name=self.position_table_name)  

                db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)  
                self.update_db_positions_pnl_and_roe(
                    trader_ids_w_api_positions=api_positions, trader_ids_w_db_positions=db_positions
                )
                        
                db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)  
                self.close_or_cancel_no_longer_valid_db_positions(
                    trader_ids_w_api_positions=api_positions, trader_ids_w_db_positions=db_positions
                )

                logger.debug("Updating Kelly Criteria stats table")
                self.db.insert_or_update_kc(
                    kc_stats_table_name=self.kc_stats_table_name, top_x_table_name=self.position_table_name
                )

                api_positions_count = (len([pos for trader_id in api_positions for pos in api_positions[trader_id]]))
                logger.debug(f"api_positions_count: {api_positions_count}")

                db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)  
                self.insert_new_api_positions(
                    trader_ids_w_api_positions=api_positions,
                    trader_ids_w_db_positions=db_positions,
                    first_time_run=first_time_run
                )

                self.ignore_and_or_close_or_cancel_opposite_and_same_positions()  
                
                db_positions = self.db.fetch_active_db_positions(table=self.position_table_name)  
                self.update_db_positions_amounts(
                    trader_ids_w_db_positions=db_positions, trader_ids_w_api_positions=api_positions
                )

                self.handle_copy_positions()  

                first_time_run = False
                if consec_crash_count > 0:  # it means the script crashed at some point previously
                    consec_crash_count = 0  # reset consecutive crash count as everything went fine this time
                    msg = (
                        f"Binance leaderboard script '{self.top_type} {self.instance}' "
                        f"has been successfully recovered."
                    )
                    telegram_bot.send_telegram_message(msg=msg)

                print("-"*20)
                print("\n"*2)
                time.sleep(delay)
            except Exception as e:
                consec_crash_count += 1  # increase consecutive crash count as something went wrong
                full_error_msg = traceback.format_exc()
                logger.error(full_error_msg)

                if consec_crash_count < max_consec_crash_count:
                    crash_delay = consec_crash_count * delay * 4
                    msg = (
                        f"Binance leaderboard script crashed '{self.top_type} {self.instance}'. "
                        f"Error:\n{e}\nWill be trying to recover in {crash_delay} seconds."
                    )
                    telegram_bot.send_telegram_message(msg=msg)
                    time.sleep(crash_delay)
                else:
                    msg = (
                        f"Warning! Unable to recover Binance leaderboard script '{self.top_type} {self.instance}' "
                        f"after {consec_crash_count} retry/retries. Stopping."
                    )
                    telegram_bot.send_telegram_message(msg=msg)
                    return
        

if __name__ == "__main__":
    """
    Usage: python leaderboard.py top_all x2 top_all_x1
    (when trying to run top_all x2 instance and replicate some tables from top_all_x1 instance
    the last argument 'top_all_x1' in not required if you don't want to replicate some existing instance)
    """
    replicatable_tables = ["position", "kc_stats"]
    valid_instance_args = ["x1", "x2", "x3"]

    # Check for the correct number of arguments
    if not (1 <= len(sys.argv) <= 3):
        logger.error("Usage: python leaderboard.py <instance> [instance_to_replicate]")
        sys.exit(1)

    instance_arg = sys.argv[1]
    if instance_arg not in valid_instance_args:
        logger.error(f"Invalid instance: {instance_arg}")
        sys.exit(1)

    # Optional third argument
    instance_to_replicate_arg = None
    if len(sys.argv) == 3:
        instance_to_replicate_arg = sys.argv[2]
        valid_instances = [f"position_{instance}" for instance in valid_instance_args]
        if instance_to_replicate_arg not in valid_instances:
            logger.error(f"Invalid instance to replicate arg: {instance_to_replicate_arg}")
            sys.exit(1)

    logging_fp = f"./logs/{instance_arg}/"
    if not os.path.exists(logging_fp):
        os.makedirs(logging_fp)

    binance_api_key = config[f"binance_api_key_{instance_arg}"]
    binance_api_secret = config[f"binance_api_secret_{instance_arg}"]
    if not any([binance_api_key, binance_api_secret]):
        logger.error(f"No Binance api_key and/or api_secret for '{instance_arg}' instance arg inside config.yml")
        sys.exit()

    logger.add(f"{logging_fp}"+"{time:YYYY-MM-DD}.log", rotation="00:00", retention="1 day")
    leaderboard = Leaderboard(instance=instance_arg, instance_to_replicate=instance_to_replicate_arg)
    leaderboard.run()
