import time
from datetime import datetime
from typing import Optional
import mysql.connector
from loguru import logger
from helper import helpers
from pprint import pprint
config = helpers.load_config_from_yaml()

db_host = config.mysql_db.host
db_user = config.mysql_db.user
db_password = config.mysql_db.password
database = config.mysql_db.database


class DatabaseManager:
    def __init__(self, db_host, db_user, db_password, database):
        self.host = db_host
        self.user = db_user
        self.password = db_password
        self.database = database
        self.connection = self._connect()
        self.init_x_inst_pos_table_names()

    def _connect(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )

        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")

        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
    
    def init_x_inst_pos_table_names(self):
        self.x_inst_pos_table_names = [
            "position_x1",
            "position_x2",
            "position_x3",
        ]

    def create_tables(self):
        # Creating initial tables (lb_api_session, trader, position_temp)
        create_init_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS trader (
                trader_id VARCHAR(255) NOT NULL PRIMARY KEY,
                inserted_on DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_init TINYINT,
                is_followed TINYINT,
                is_observed TINYINT,
                is_ignored TINYINT,
                aum FLOAT,
                follow_pnl FLOAT,
                follower_limit INT,
                number_of_followers INT,
                total_number_of_followers INT,
                initial_day INT,
                nickname VARCHAR(255),
                pnl FLOAT,
                symbol VARCHAR(255),
                target_id INT,
                win_ratio FLOAT,
                yield_ratio FLOAT,
                last_pos_datetime DATETIME
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trader_stats (
                trader_id VARCHAR(255) NOT NULL,
                date_range INT,
                inserted_on DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                follower_num INT,
                current_follow_pnl FLOAT,
                aum FLOAT,
                avg_position_value FLOAT,
                cost_val FLOAT,
                win_ratio FLOAT,
                loss_days INT,
                profit_days INT,
                yield_ratio FLOAT,
                UNIQUE (trader_id, date_range)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS position_temp (
                id INT AUTO_INCREMENT PRIMARY KEY,
                trader_id VARCHAR(255),
                avail_sub_pos INT,
                ccy VARCHAR(255),
                inst_id VARCHAR(255),
                inst_type VARCHAR(255),
                last FLOAT,
                lever INT,
                margin FLOAT,
                mark_px FLOAT,
                mgn_mode VARCHAR(255),
                notional_usd FLOAT,
                open_avg_px FLOAT,
                open_time BIGINT,
                pnl FLOAT,
                pnl_ratio FLOAT,
                pos_side VARCHAR(255),
                sl_trigger_px FLOAT,
                sl_trigger_type VARCHAR(255),
                sub_pos INT,
                tp_trigger_px FLOAT,
                tp_trigger_type VARCHAR(255),
                trade_item_id BIGINT,
                u_time BIGINT,
                inserted_on_ts BIGINT,
                FOREIGN KEY (trader_id) REFERENCES trader (trader_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS success_stats (
                trader_id VARCHAR(255) NOT NULL,
                position_table VARCHAR(255) NOT NULL,
                is_active TINYINT DEFAULT 1,
                win_count INT DEFAULT 0,
                lose_count INT DEFAULT 0,
                win_lose_count_res INT AS (win_count - lose_count),
                win_rate FLOAT AS (
                    CASE
                        WHEN (win_count + lose_count) = 0 THEN NULL
                        ELSE win_count / (win_count + lose_count)
                    END
                ),
                created_on DATETIME DEFAULT NOW(),
                updated_on DATETIME ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE (trader_id, position_table)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stop_losses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                position_table VARCHAR(255) NOT NULL,
                orig_position_id VARCHAR(255),
                position_id VARCHAR(255),
                symbol VARCHAR(255) NOT NULL,
                position_type VARCHAR(255),
                side VARCHAR(255),
                is_active TINYINT DEFAULT 1,
                is_filled TINYINT DEFAULT 0,
                price FLOAT NOT NULL,
                amount FLOAT NOT NULL,
                UNIQUE (position_table, orig_position_id, position_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS take_profits (
                id INT AUTO_INCREMENT PRIMARY KEY,
                position_table VARCHAR(255) NOT NULL,
                orig_position_id VARCHAR(255),
                position_id VARCHAR(255),
                symbol VARCHAR(255) NOT NULL,
                position_type VARCHAR(255),
                side VARCHAR(255),
                is_active TINYINT DEFAULT 1,
                is_filled TINYINT DEFAULT 0,
                price FLOAT NOT NULL,
                amount FLOAT NOT NULL,
                UNIQUE (position_table, orig_position_id, position_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS penalties (
                trader_id VARCHAR(255) NOT NULL,
                position_table VARCHAR(255) NOT NULL,
                penalty_type VARCHAR(255) NOT NULL,
                penalty_value INT NOT NULL,
                UNIQUE (trader_id, position_table)
            )
            """,
            
        ]

        for query in create_init_table_queries:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
        
        logger.success(f"Initial tables created!")

        # Creating multiple positions tables of different instances (x1, x2, etc.)

        create_x_inst_pos_table_queries = []

        for x_inst_pos_table_name in self.x_inst_pos_table_names:
            x_inst_pos_table_query = f"""
                CREATE TABLE IF NOT EXISTS {x_inst_pos_table_name} (
                    okx_pos_id BIGINT PRIMARY KEY,
                    bin_pos_id VARCHAR(255),
                    trader_id VARCHAR(255),
                    is_active TINYINT NOT NULL DEFAULT 1,
                    is_copied TINYINT DEFAULT 0,
                    is_filled TINYINT DEFAULT 0,
                    is_ignored TINYINT DEFAULT 0,
                    is_ignored_reason VARCHAR(255),
                    is_canceled TINYINT DEFAULT 0,
                    is_closed TINYINT DEFAULT 0,
                    inserted_on DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    user_sub_pos INT,
                    avail_sub_pos INT,
                    ccy VARCHAR(255),
                    inst_id VARCHAR(255),
                    inst_type VARCHAR(255),
                    last FLOAT,
                    lever INT,
                    margin FLOAT,
                    mark_px FLOAT,
                    mgn_mode VARCHAR(255),
                    notional_usd FLOAT,
                    open_avg_px FLOAT,
                    open_time BIGINT,
                    close_avg_px FLOAT,
                    pnl FLOAT,
                    pnl_ratio FLOAT,
                    pos_side VARCHAR(255),
                    sl_trigger_px FLOAT,
                    sl_trigger_type VARCHAR(255),
                    sub_pos INT,
                    tp_trigger_px FLOAT,
                    tp_trigger_type VARCHAR(255),
                    trade_item_id BIGINT,
                    u_time BIGINT,
                    contract_val FLOAT,
                    multiplier INT,
                    user_amount FLOAT,
                    user_liquidation_price FLOAT,
                    FOREIGN KEY (trader_id) REFERENCES trader (trader_id)
                )
            """
            create_x_inst_pos_table_queries.append(x_inst_pos_table_query)

            for query in create_x_inst_pos_table_queries:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    self.connection.commit()
        logger.success(f"Positions tables created!")

        kc_stats_table_names = [
            "kc_stats_x1",
            "kc_stats_x2",
            "kc_stats_x3",
        ]

        create_summ_stats_table_queries = []

        for table_name in kc_stats_table_names:
            summ_stats_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    trader_id VARCHAR(255) NOT NULL PRIMARY KEY,
                    trades_count INT,
                    roe_sum DECIMAL(10, 4),
                    avg_roe DECIMAL(10, 4),
                    roe_std_dev DECIMAL(10, 4),
                    kelly_criteria DECIMAL(20, 10)
                )
            """
            create_summ_stats_table_queries.append(summ_stats_table_query)

            for query in create_summ_stats_table_queries:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    self.connection.commit()
        logger.success("kc stat tables created!")

    def insert_position(self, table, data):
       with self.connection.cursor() as cursor:
            try:
                # Check if the row exists
                unique_columns = ['trader_id', 'symbol', 'update_timestamp']
                where_clause = ' AND '.join([f"{col} = %s" for col in unique_columns])
                check_query = f"SELECT id FROM {table} WHERE {where_clause}"
                
                check_values = tuple(data[col] for col in unique_columns)
                cursor.execute(check_query, check_values)
                existing_row = cursor.fetchone()

                if not existing_row:
                    logger.success("Inserting new position:")
                    logger.success(data)
                    # Insert the data
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join(['%s'] * len(data))
                    insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                    
                    values = tuple(data.values())
                    cursor.execute(insert_query, values)
                    self.connection.commit()

                    last_insert_id = cursor.lastrowid
                else:
                    last_insert_id = existing_row[0]  # Access the 'id' column using index
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Error in insert_data: {e}")
                last_insert_id = None

            return last_insert_id

    def insert_data(self, table, data):
        with self.connection.cursor() as cursor:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            values = tuple(data.values())
            cursor.execute(query, values)
            self.connection.commit()
            
            last_insert_id = cursor.lastrowid

            return last_insert_id
        
    def insert_or_update_data(self, table, data):
        with self.connection.cursor() as cursor:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            update_statements = ', '.join([f"{key} = VALUES({key})" for key in data.keys()])

            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_statements}"
            
            values = tuple(data.values())
            cursor.execute(query, values)
            self.connection.commit()
            
            last_insert_id = cursor.lastrowid

            return last_insert_id

    def update_data(self, table, data, condition_column, condition_value):
        with self.connection.cursor() as cursor:
            set_values = ', '.join([f"{column} = %s" for column in data.keys()])
            query = f"UPDATE {table} SET {set_values} WHERE {condition_column} = %s"
            
            values = list(data.values()) + [condition_value]
            cursor.execute(query, values)
            self.connection.commit()
   
    def fetch_all_trader_ids(self):   
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT trader_id FROM trader"
            cursor.execute(query)
            results = cursor.fetchall()
            result_as_list = [i["trader_id"] for i in results]
            return result_as_list
        
    def fetch_all_followed_trader_ids(self):   
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT trader_id FROM trader WHERE is_followed = 1"
            cursor.execute(query)
            results = cursor.fetchall()
            result_as_list = [i["trader_id"] for i in results]
            return result_as_list
    
    def is_trader_exist(self, trader_id):
        with self.connection.cursor() as cursor:
            query = "SELECT COUNT(*) FROM trader WHERE trader_id = %s"
            cursor.execute(query, (trader_id,))
            result = cursor.fetchone()
            trader_exist = result[0] > 0
            return trader_exist
       
    def fetch_active_db_positions(self, table):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"SELECT * FROM {table} WHERE is_active = 1 ORDER BY inserted_on ASC"
            cursor.execute(query)
            results = cursor.fetchall()
            
            all_db_positions = {}
            for row in results:
                trader_id = row["trader_id"]
                if trader_id not in all_db_positions:
                    all_db_positions[trader_id] = []
                all_db_positions[trader_id].append(row)
            
            return all_db_positions
        
    def fetch_active_non_ignored_trader_ids_to_copy(self, table: str):
        """
        Return trader IDs that will be copied
        SQL query will ignore all next traders with the same symbol
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT DISTINCT(t.trader_id)
                FROM {table} t 
                INNER JOIN (
                    SELECT MIN(id) AS earliest_id
                    FROM {table}
                    WHERE is_active = 1 AND is_ignored = 0
                    GROUP BY symbol
                ) AS subquery ON t.id = subquery.earliest_id
            """
            cursor.execute(query)
            results = cursor.fetchall()
            trader_ids = [row["trader_id"] for row in results]
            
            return trader_ids
        
    def fetch_active_non_ignored_positions(self, table: str):
        """
        Returns non-ignored trader's positions
        Ex.: {'trader_xyz_id': [pos1, pos2, ...]}
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT t.*
                FROM {table} t 
                INNER JOIN (
                    SELECT MIN(id) AS earliest_id
                    FROM {table}
                    WHERE is_active = 1 AND is_ignored = 0
                    GROUP BY symbol
                ) AS subquery ON t.id = subquery.earliest_id
            """
            cursor.execute(query)
            results = cursor.fetchall()

            db_positions = {}
            for row in results:
                trader_id = row["trader_id"]
                if trader_id not in db_positions:
                    db_positions[trader_id] = []
                db_positions[trader_id].append(row)
        
            return db_positions
       
    def fetch_trader_ids_with_roi(self, trader_ids: list):
        with self.connection.cursor(dictionary=True) as cursor:
            if not trader_ids:
                return {}  # Return an empty dictionary if trader_ids is empty
                
            placeholders = ', '.join('%s' for _ in trader_ids)
            query = f"SELECT trader_id, yield_ratio FROM trader WHERE trader_id IN ({placeholders})"
            cursor.execute(query, trader_ids)
            results = cursor.fetchall()
            as_dict = {row['trader_id']: row['yield_ratio'] for row in results}
            return as_dict
        
    def fetch_trader_ids_with_last_position_date(self, trader_ids: list):
        with self.connection.cursor(dictionary=True) as cursor:
            if not trader_ids:
                return {}  # Return an empty dictionary if trader_ids is empty
                
            placeholders = ', '.join('%s' for _ in trader_ids)
            query = f"SELECT trader_id, last_pos_datetime FROM trader WHERE trader_id IN ({placeholders})"
            cursor.execute(query, trader_ids)
            results = cursor.fetchall()
            as_dict = {
                row['trader_id']: {'last_pos_datetime': row['last_pos_datetime']} for row in results
            }
            return as_dict
    
    def fetch_top_trader_ids(self, top_type: str):
        with self.connection.cursor() as cursor:
            query = f"SELECT trader_id FROM trader WHERE {top_type} = 1"
            cursor.execute(query)
            result = cursor.fetchall()
            result_as_list = [i[0] for i in result]
            return result_as_list
    
    def fetch_observed_trader_ids(self):
        with self.connection.cursor() as cursor:
            query = f"SELECT trader_id FROM trader WHERE is_observed = 1"
            cursor.execute(query)
            result = cursor.fetchall()
            result_as_list = [i[0] for i in result]
            return result_as_list
        
    def fetch_init_traders(self):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT t.trader_id, ts.date_range, ts.win_ratio, ts.yield_ratio, ts.current_follow_pnl, ts.profit_days, ts.loss_days
                FROM trader t
                LEFT JOIN trader_stats ts
                ON t.trader_id = ts.trader_id
                WHERE t.is_init = 1
            """
            cursor.execute(query)
            result = cursor.fetchall()

            results_restructured = {}
            for dict_i in result:
                trader_id = dict_i["trader_id"]
                if trader_id not in results_restructured:
                    results_restructured[trader_id] = []
                results_restructured[trader_id].append(dict_i)

            return results_restructured
    
    def upsert_init_traders(self, data: list):
        now = datetime.now()

        # get all trader IDs from RapidAPI
        trader_ids_from_rapidapi = []
        for trader_data_dict in data:
            trader_id = trader_data_dict["id"]
            trader_ids_from_rapidapi.append(trader_id)

        db_followed_trader_ids = self.fetch_all_followed_trader_ids()
        db_observed_trader_ids = self.fetch_observed_trader_ids()
        db_combined_trader_ids = db_followed_trader_ids + db_observed_trader_ids
        traders_to_stop_following_or_observing = list(set([trader_id for trader_id in db_combined_trader_ids if trader_id not in trader_ids_from_rapidapi]))
        traders_to_stop_following_or_observing_rois = self.fetch_trader_ids_with_roi(trader_ids=traders_to_stop_following_or_observing)
        traders_to_stop_following_or_observing_last_trading_dates = self.fetch_trader_ids_with_last_position_date(trader_ids=traders_to_stop_following_or_observing)
        for trader_id in traders_to_stop_following_or_observing:
            logger.debug(f"Unfollowing trader ID: {trader_id})")
            data_to_update = {}

            total_roi = None
            if traders_to_stop_following_or_observing_rois.get(trader_id):
                total_roi = traders_to_stop_following_or_observing_rois[trader_id]

            last_pos_datetime = None
            if traders_to_stop_following_or_observing_last_trading_dates.get(trader_id):
                last_pos_datetime = traders_to_stop_following_or_observing_last_trading_dates[trader_id]["last_pos_datetime"]

            should_be_observed = False
            if total_roi is not None and last_pos_datetime is not None:
                time_difference = now - last_pos_datetime # difference between current time and the time when the trader placed his most latest position
                days_difference_from_the_last_trade = time_difference.days
                if total_roi > 0 and days_difference_from_the_last_trade <= 30:
                    should_be_observed = True

            if should_be_observed:
                logger.debug(f"Starting to observe trader ID: {trader_id})")
                data_to_update["is_observed"] = 1
            else:
                data_to_update["is_observed"] = 0

            data_to_update["is_init"] = 0
            data_to_update["is_followed"] = 0

            self.update_data(table="trader", data=data_to_update, condition_column="trader_id", condition_value=trader_id)

        # Used for matching API returned keys to 'trader' table column keys
        api_key_to_db_key_matcher = {
            "id": "trader_id",
            "aum": "aum",
            "followPnl": "follow_pnl",
            "followerLimit": "follower_limit",
            "numberOfFollowers": "number_of_followers",
            "totalNumberOfFollowers": "total_number_of_followers",
            "initialDay": "initial_day",
            "nickName": "nickname",
            "pnl": "pnl",
            "symbol": "symbol",
            "targetId": "target_id",
            "winRatio": "win_ratio",
            "yieldRatio": "yield_ratio"
        }

        # Restructure data
        api_traders_data_fixed = []
        for trader_data_dict in data:
            trader_data_dict_fixed = {api_key_to_db_key_matcher[key]: val for key, val in trader_data_dict.items() if api_key_to_db_key_matcher.get(key)}
            api_traders_data_fixed.append(trader_data_dict_fixed)

        # Iterate over all traders and insert or update data
        for trader_data_dict_fixed in api_traders_data_fixed:
            trader_id = trader_data_dict_fixed["trader_id"]
            trader_exist = self.is_trader_exist(trader_id=trader_id)
            trader_data_dict_fixed["is_init"] = 1
            if trader_exist:
                logger.debug(f"Updating existing trader: {trader_id}")
                # trader_data_dict_fixed["is_followed"] = 1 # If we update an existing trader, we set 'following' as 1 in case the trader's following status was set as 0 previously
                # trader_data_dict_fixed["is_observed"] = 0 # If we start following this trader again - then we no need to need to observe it
                self.update_data(table="trader", data=trader_data_dict_fixed, condition_column="trader_id", condition_value=trader_id)
            else:
                logger.debug(f"Inserting new trader: {trader_id}")
                trader_data_dict_fixed["is_followed"] = 0 # When we add this trader - we don't start following him/her as we will decide this later
                self.insert_data(table="trader", data=trader_data_dict_fixed)

    def detect_traders_to_follow(self):
        config = helpers.load_config_from_yaml()
        filter_traders_config = config["filter_traders_config"]
        iterate_over_date_ranges = config["get_trade_stats"]["date_ranges"]

        init_traders = self.fetch_init_traders()

        trader_ids_to_follow = []
        for trader_id in init_traders:
            is_valid_trader = True
            trader_dict_items = init_traders[trader_id]
            for trader_dict_i in trader_dict_items:
                trader_date_range = trader_dict_i["date_range"]

                trader_win_ratio = trader_dict_i["win_ratio"]
                trader_yield_ratio = trader_dict_i["yield_ratio"]
                trader_current_follow_pnl = trader_dict_i["current_follow_pnl"]
                trader_profit_days = trader_dict_i["profit_days"]
                trader_loss_days = trader_dict_i["loss_days"]

                if trader_date_range in iterate_over_date_ranges: # iterate over all date ranges
                    min_win_ratio = filter_traders_config[trader_date_range]["win_ratio"]
                    min_yield_ratio = filter_traders_config[trader_date_range]["yield_ratio"]

                    if min_win_ratio is not None and trader_win_ratio is not None:
                        if trader_win_ratio < min_win_ratio:
                            is_valid_trader = False
                            continue
                    if min_yield_ratio is not None and trader_yield_ratio is not None:
                        if trader_yield_ratio < min_yield_ratio:
                            is_valid_trader = False
                            continue

                # if trader_date_range == 7:
                    min_current_follow_pnl = filter_traders_config[trader_date_range]["current_follow_pnl"]
                    min_profit_days = filter_traders_config[trader_date_range]["profit_days"]
                    max_loss_days = filter_traders_config[trader_date_range]["loss_days"]
                    min_profit_loss_days_diff = filter_traders_config[trader_date_range]["profit_loss_days_diff"]

                    if min_current_follow_pnl is not None and trader_current_follow_pnl is not None:
                        if trader_current_follow_pnl < min_current_follow_pnl:
                            is_valid_trader = False
                            continue
                    if min_profit_days is not None and trader_profit_days is not None:
                        if trader_profit_days < min_profit_days:
                            is_valid_trader = False
                            continue
                    if max_loss_days is not None and trader_loss_days is not None:
                        if trader_loss_days > max_loss_days:
                            is_valid_trader = False
                            continue
                    if min_profit_loss_days_diff is not None and trader_profit_days is not None and trader_loss_days is not None:
                        if (trader_profit_days - trader_loss_days) <= min_profit_loss_days_diff:
                            is_valid_trader = False
                            continue

            if is_valid_trader:
                trader_ids_to_follow.append(trader_id)
        
        return trader_ids_to_follow
    
    def set_traders_to_follow(self, traders_to_follow: list):
        if not traders_to_follow:
            print("XXXXX")
            # Disable  all traders
            with self.connection.cursor() as cursor:
                query = f"""
                    UPDATE trader
                    SET is_followed = 0
                """
                cursor.execute(query)
        else:
            placeholders = ', '.join(['%s'] * len(traders_to_follow))

            # Disable traders that are not inside the 'traders_to_follow' list
            with self.connection.cursor() as cursor:
                query = f"""
                    UPDATE trader
                    SET is_followed = 0
                    WHERE trader_id NOT IN ({placeholders})
                """
                cursor.execute(query, traders_to_follow)

            # Enable traders that are inside the 'traders_to_follow' list
            with self.connection.cursor() as cursor:
                query = f"""
                    UPDATE trader
                    SET is_followed = 1
                    WHERE trader_id IN ({placeholders})
                """
                cursor.execute(query, traders_to_follow)
        
        self.connection.commit()

       
    def insert_temp_positions(self, traders_ids_and_positions: dict):
        with self.connection.cursor() as cursor:
            current_time = int(str(time.time()).replace(".", "")[:13])  # Get the current timestamp (use the same format as in the leaderboard.py)

            for trader_id in traders_ids_and_positions:
                positions = traders_ids_and_positions[trader_id]

                # Insert new positions with timestamps
                for position in positions:
                    # Add 'insert_timestamp' to the position with the current timestamp
                    position['inserted_on_ts'] = current_time
                    columns = ', '.join(position.keys())
                    placeholders = ', '.join(['%s'] * len(position))
                    query = f"INSERT INTO position_temp ({columns}) VALUES ({placeholders})"
                    values = tuple(position.values())
                    cursor.execute(query, values)

            # Delete old positions not received from the API
            query = f"DELETE FROM position_temp WHERE inserted_on_ts < %s"
            cursor.execute(query, (current_time,))

            self.connection.commit()

    def get_temp_positions_from_db(self, ignore_observed_traders: bool = True):
        """
        'ignore_observed_traders' arg is only used when copying all top x leaderboards
        Returns:
        {
        "trader_id_1": [{pos_1, pos_2, ...}],
        "trader_id_2": [{pos_1, pos_2, ...}]
        }
        """

        with self.connection.cursor(dictionary=True) as cursor:
            # Start a new transaction by committing the previous one so you would see updated data from another connection
            # https://stackoverflow.com/a/52386871
            self.connection.commit()

            query = f"SELECT * FROM trader WHERE is_followed = 1"
            cursor.execute(query)
            results = cursor.fetchall()
            trader_ids = [dict_i["trader_id"] for dict_i in results]

            # get positions for certain traders' IDs
            query = "SELECT * FROM position_temp"
            cursor.execute(query)
            results = cursor.fetchall()
            results_filtered = [dict_i for dict_i in results if dict_i["trader_id"] in trader_ids]
            
            results_restructured = {}
            for dict_i in results_filtered:
                trader_id = dict_i["trader_id"]
                if dict_i["trader_id"] in results_restructured:
                    results_restructured[trader_id].append(dict_i)
                else:
                    results_restructured[trader_id] = []
                    results_restructured[trader_id].append(dict_i)

            return results_restructured
        
    def insert_or_update_success_stats(self, trader_id: str, position_table_name: str, is_win: Optional[bool] = None):
        """
        Inserts a new trader (if the trader doesn't exist)
        If the trader exists and is not active - it sets the trader as active and resets his stats
        If the 'is_win' arg is provided - it updates trader win/lose stats
        """

        # Check if trader exists in the table
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT * FROM success_stats WHERE trader_id = %s AND position_table = %s"
            cursor.execute(query, (trader_id, position_table_name))
            existing_trader = cursor.fetchone()

        if existing_trader:
            is_trader_active = existing_trader["is_active"]
            if not is_trader_active:
                logger.debug(f"Activating existing trader of 'sucess_stats' table. Trader ID: {trader_id}")
                # We need to activate the trader and reset previous stats
                with self.connection.cursor() as cursor:
                    query = "UPDATE success_stats SET is_active = 1, win_count = 0, lose_count = 0 WHERE trader_id = %s AND position_table = %s"
                    cursor.execute(query, (trader_id, position_table_name))
                    self.connection.commit()
                    
                    if is_win is not None:
                        # We refetch the trader if we will be updating his win or lose data
                        with self.connection.cursor(dictionary=True) as cursor:
                            query = "SELECT * FROM success_stats WHERE trader_id = %s AND position_table = %s"
                            cursor.execute(query, (trader_id, position_table_name))
                            existing_trader = cursor.fetchone()
        else:
            # We need to add a trader
            logger.success(f"Inserting new trader to the 'sucess_stats' table. Trader ID: {trader_id}")
            with self.connection.cursor() as cursor:
                query = "INSERT INTO success_stats(trader_id, position_table, win_count, lose_count, is_active) VALUES(%s, %s, 0, 0, 1)"
                cursor.execute(query, (trader_id, position_table_name))
                self.connection.commit()

            # Fetch the new trader's details
            with self.connection.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM success_stats WHERE trader_id = %s AND position_table = %s"
                cursor.execute(query, (trader_id, position_table_name))
                existing_trader = cursor.fetchone()

        if existing_trader:  # Ensure existing_trader is not None before accessing win/lose counts
            if is_win is True:
                prev_win_count = existing_trader["win_count"]
                new_win_count = prev_win_count + 1
                logger.debug(f"Adding a win to the 'sucess_stats' table. Trader ID: {trader_id}")
                with self.connection.cursor() as cursor:
                    query = "UPDATE success_stats SET win_count = %s WHERE trader_id = %s AND position_table = %s"
                    cursor.execute(query, (new_win_count, trader_id, position_table_name))
                    self.connection.commit()
            elif is_win is False:
                prev_lose_count = existing_trader["lose_count"]
                new_lose_count = prev_lose_count + 1
                logger.debug(f"Adding a lose to the 'sucess_stats' table. Trader ID: {trader_id}")
                with self.connection.cursor() as cursor:
                    query = "UPDATE success_stats SET lose_count = %s WHERE trader_id = %s AND position_table = %s"
                    cursor.execute(query, (new_lose_count, trader_id, position_table_name))
                    self.connection.commit()

    def deactivate_trader_in_success_stats(self, trader_id: str, position_table_name: str):
        with self.connection.cursor() as cursor:
            query = "UPDATE success_stats SET is_active = 0 WHERE trader_id = %s AND position_table = %s"
            cursor.execute(query, (trader_id, position_table_name))
            self.connection.commit()

    def get_all_traders_success_stats(self, position_table_name: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT * FROM success_stats WHERE position_table = %s"
            cursor.execute(query, (position_table_name,))
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                trader_id = row["trader_id"]
                results_as_dict[trader_id] = row
            
            return results_as_dict
        
    def deactivate_trader_ids_of_success_stats_table(self, position_table_name: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"SELECT * FROM trader WHERE is_followed = 1 OR is_observed = 1"
            cursor.execute(query)
            results = cursor.fetchall()
            followed_and_observed_trader_ids = [dict_i["trader_id"] for dict_i in results]

        # get all active trader IDs of 'success_stats' table by 'position_table_name' arg
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT * FROM success_stats WHERE is_active = 1 AND position_table = %s"
            cursor.execute(query, (position_table_name,))
            results = cursor.fetchall()
            is_active_trader_ids = [dict_i["trader_id"] for dict_i in results]

        # disable trader IDs (of 'sucess_stats' table) that are not followed anymore
        trader_ids_to_disable = [trader_id for trader_id in is_active_trader_ids if trader_id not in followed_and_observed_trader_ids]
        for trader_id in trader_ids_to_disable:
            self.deactivate_trader_in_success_stats(trader_id=trader_id, position_table_name=position_table_name)

    def insert_trader_ids_to_success_stats_table(self, position_table_name: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"SELECT * FROM trader WHERE is_followed = 1 OR is_observed = 1"
            cursor.execute(query)
            results = cursor.fetchall()
            followed_and_observed_trader_ids = [dict_i["trader_id"] for dict_i in results]

        # get all active trader IDs of 'success_stats' table by 'position_table_name' arg
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT * FROM success_stats WHERE is_active = 1 AND position_table = %s"
            cursor.execute(query, (position_table_name,))
            results = cursor.fetchall()
            is_active_trader_ids = [dict_i["trader_id"] for dict_i in results]

        # insert trader IDs to the 'sucess_stats' table that are not in this table, yet
        # there might be scenarios when the trader ID will exist in the 'success_stats' table but it's not active
        # in this case it will just reactivate the trader
        trader_ids_to_insert = [trader_id for trader_id in followed_and_observed_trader_ids if trader_id not in is_active_trader_ids]
        for trader_id in trader_ids_to_insert:
            self.insert_or_update_success_stats(trader_id=trader_id, position_table_name=position_table_name)

    def insert_or_update_kc(self, kc_stats_table_name: str, top_x_table_name: str):
        with self.connection.cursor() as cursor:
            query = f"""
                INSERT INTO {kc_stats_table_name} (trader_id, trades_count, roe_sum, avg_roe, roe_std_dev, kelly_criteria)
                SELECT
                    trader_id,
                    COUNT(*) AS trades_count,
                    SUM(pnl_ratio) AS roe_sum,
                    AVG(pnl_ratio) AS avg_roe,
                    STDDEV(pnl_ratio) AS roe_std_dev,
                    (AVG(pnl_ratio) / NULLIF(STDDEV(pnl_ratio) * STDDEV(pnl_ratio), 0)) AS kelly_criteria
                FROM {top_x_table_name}
                WHERE is_active = 0 AND u_time >= (UNIX_TIMESTAMP(NOW()) - 365 * 24 * 60 * 60) * 1000
                GROUP BY trader_id
                ON DUPLICATE KEY UPDATE
                    trades_count = VALUES(trades_count),
                    roe_sum = VALUES(roe_sum),
                    avg_roe = VALUES(avg_roe),
                    roe_std_dev = VALUES(roe_std_dev),
                    kelly_criteria = VALUES(kelly_criteria);        
            """
            cursor.execute(query)
            self.connection.commit()

    def calculate_total_kc(self, top_x_table_name: str, trader_ids: list):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT
                    COUNT(*) AS trades_count,
                    SUM(roe) AS roe_sum,
                    AVG(roe) AS avg_roe,
                    STDDEV(roe) AS roe_std_dev,
                    (AVG(roe) / NULLIF(STDDEV(roe) * STDDEV(roe), 0)) AS kelly_criteria
                FROM {top_x_table_name}
                WHERE is_active = 0 AND trader_id IN %s    
            """
            cursor.execute(query, (tuple(trader_ids),))
            result = cursor.fetchone()
            return result["kelly_criteria"]

    def get_all_traders_kc_stats(self, kc_stats_table_name: str):
        """
        Returns all Kelly Criteria values by trader ID
        Ex.: {
            "123qwerty123": 0.11,
            "321qwerty321": 0.09,
        }
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT trader_id, kelly_criteria
                FROM {kc_stats_table_name}
                ORDER BY kelly_criteria DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                trader_id = row["trader_id"]
                results_as_dict[trader_id] = row["kelly_criteria"]
            
            return results_as_dict
        
    def get_all_traders_tc_stats(self, kc_stats_table_name: str):
        """
        Returns all trades count values by trader ID
        Ex.: {
            "123qwerty123": 108,
            "321qwerty321": 29,
        }
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT trader_id, trades_count
                FROM {kc_stats_table_name}
                ORDER BY trades_count DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                trader_id = row["trader_id"]
                results_as_dict[trader_id] = row["trades_count"]
            
            return results_as_dict
        
    def get_all_traders_trades_counts(self, top_x_table_name: str):
        """
        Returns trade counts of all traders
        Ex.: {
            "123qwerty123": 5,
            "321qwerty321": 49,
        }
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT trader_id, COUNT(*) AS trade_count
                FROM {top_x_table_name}
                WHERE is_active = 0
                GROUP BY trader_id
            """
            cursor.execute(query)
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                trader_id = row["trader_id"]
                results_as_dict[trader_id] = row["trade_count"]
            
            return results_as_dict
        
    def detect_trader_type(self, trader_id: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT following, observing
                FROM trader
                WHERE trader_id = %s
            """
            cursor.execute(query, (trader_id,))
            result = cursor.fetchone()

            trader_type = None
            if result:
                is_followed = result["following"]
                if is_followed:
                    trader_type = "followed"
                
                is_observed = result["observing"]
                if is_observed:
                    trader_type = "observed"
            
            return trader_type
        
    def update_last_pos_datetime_for_trader(self, trader_id: str, last_pos_datetime: datetime):
        with self.connection.cursor() as cursor:
            query = "UPDATE trader SET last_pos_datetime = %s WHERE trader_id = %s"
            cursor.execute(query, (last_pos_datetime, trader_id))
            self.connection.commit()

    def update_last_pos_datetime_for_all_traders(self):
        for table_name in self.x_inst_pos_table_names:
            with self.connection.cursor(dictionary=True) as cursor:
                query = f"""
                    UPDATE trader t
                    JOIN (
                        SELECT trader_id, MAX(inserted_on) AS last_trading_date
                        FROM {table_name}
                        GROUP BY trader_id
                    ) subquery
                    ON t.trader_id = subquery.trader_id
                    SET t.last_pos_datetime = 
                        CASE
                            WHEN subquery.last_trading_date IS NOT NULL AND subquery.last_trading_date < t.last_pos_datetime
                            THEN FROM_UNIXTIME(subquery.last_trading_date / 1000)
                            ELSE t.last_pos_datetime
                        END
                """
                cursor.execute(query)
                self.connection.commit()

    def get_all_active_stop_losses(self, position_table: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT *
                FROM stop_losses
                WHERE position_table = %s AND is_active = 1
            """
            cursor.execute(query, (position_table,))
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                orig_position_id = row["orig_position_id"]
                results_as_dict[orig_position_id] = row
            
            return results_as_dict
        
    def get_all_active_pos_stop_losses(self, position_table: str):
        """
        These stop-losses might be not active (already triggered)
        but positions still open (not closed by the trader)
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT sl.*
                FROM stop_losses sl 
                LEFT JOIN {position_table} pos_table
                ON sl.orig_position_id = pos_table.bin_pos_id 
                WHERE sl.position_table = '{position_table}' AND sl.is_filled = 0 AND pos_table.is_active = 1
            """
            cursor.execute(query)
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                orig_position_id = row["orig_position_id"]
                results_as_dict[orig_position_id] = row
            
            return results_as_dict
        
    def get_all_active_take_profits(self, position_table: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT *
                FROM take_profits
                WHERE position_table = %s AND is_active = 1
            """
            cursor.execute(query, (position_table,))
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                orig_position_id = row["orig_position_id"]
                results_as_dict[orig_position_id] = row
            
            return results_as_dict
        
    def get_all_active_pos_take_profits(self, position_table: str):
        """
        These take-profits might be not active (already triggered)
        but positions still open (not closed by the trader)
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT tp.*
                FROM take_profits tp 
                LEFT JOIN {position_table} pos_table
                ON tp.orig_position_id = pos_table.bin_pos_id 
                WHERE tp.position_table = '{position_table}' AND tp.is_filled = 0 AND pos_table.is_active = 1
            """
            cursor.execute(query)
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                orig_position_id = row["orig_position_id"]
                results_as_dict[orig_position_id] = row
            
            return results_as_dict
        
    def get_trader_id_by_position_id(self, position_table: str, position_id: str):
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT trader_id
                FROM {position_table}
                WHERE bin_pos_id = %s
            """
            cursor.execute(query, (position_id,))
            result = cursor.fetchone()

            if result:
                return result["trader_id"]
            else:
                return None
        
    def insert_or_update_penalty(self, top_x_table_name: str, trader_id: str):
        with self.connection.cursor() as cursor:
            query = f"""
                SELECT *
                FROM penalties
                WHERE position_table = %s AND trader_id = %s
            """
            cursor.execute(query, (top_x_table_name, trader_id))
            is_trader_id_exist = cursor.fetchone()

            if is_trader_id_exist:
                query = "UPDATE penalties SET penalty_value = penalty_value * 2 WHERE trader_id = %s AND position_table = %s"
                cursor.execute(query, (trader_id, top_x_table_name))
                self.connection.commit()
            else:
                query = "INSERT INTO penalties(trader_id, position_table, penalty_type, penalty_value) VALUES(%s, %s, %s, %s)"
                cursor.execute(query, (trader_id, top_x_table_name, "sl", 2))
                self.connection.commit()

    def get_all_traders_penalties(self, top_x_table_name: str):
        """
        Returns all penalties values by trader ID
        Ex.: {
            "123qwerty123": 0.11,
            "321qwerty321": 0.09,
        }
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT trader_id, penalty_value
                FROM penalties
                WHERE position_table = %s
            """
            cursor.execute(query, (top_x_table_name,))
            results = cursor.fetchall()

            results_as_dict = {}
            for row in results:
                trader_id = row["trader_id"]
                results_as_dict[trader_id] = row["penalty_value"]
            
            return results_as_dict
        
    def get_trader_kc_table_data(self, kc_stats_table_name: str, trader_id: str):
        """
        Returns trade counts of all traders
        Ex.: {
            "123qwerty123": 5,
            "321qwerty321": 49,
        }
        """
        with self.connection.cursor(dictionary=True) as cursor:
            query = f"""
                SELECT *
                FROM {kc_stats_table_name}
                WHERE trader_id = %s
            """
            cursor.execute(query, (trader_id,))
            result = cursor.fetchone()

            return result
        
    def replicate_existing_table(self, from_table: str, to_table: str):
        logger.debug(f"Trying to replicate tables ({from_table} -> {to_table})")
        with self.connection.cursor(dictionary=True) as cursor:
            # Check if the destination table is empty
            cursor.execute(f"SELECT COUNT(*) AS row_count FROM {to_table}")
            result = cursor.fetchone()
            if result['row_count'] > 0:
                logger.warning(f"Table {to_table} is not empty. Aborting replication.")
                return False

            # If the destination table is empty, proceed with replication
            query = f"""
                INSERT INTO {to_table} SELECT * FROM {from_table};
            """
            cursor.execute(query)
            self.connection.commit()
            logger.debug(f"Replication completed successfully.")
            return True



if __name__ == "__main__":
    db = DatabaseManager(db_host=db_host, db_user=db_user, db_password=db_password, database=database)
    db.create_tables()
    db.insert_or_update_success_stats(trader_id="abcd123abcd", position_table_name="position_top_daily_x1", is_win=False)
    db.deactivate_trader_in_success_stats(trader_id="abcd123abcd", position_table_name="position_top_daily_x1")
    all_traders = db.get_all_traders_success_stats(position_table_name="position_top_daily_x1")
    pprint(all_traders, indent=4)
