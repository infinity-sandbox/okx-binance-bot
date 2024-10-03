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
);