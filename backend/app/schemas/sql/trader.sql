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
);
            