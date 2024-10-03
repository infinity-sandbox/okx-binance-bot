CREATE TABLE IF NOT EXISTS {table_name} (
    trader_id VARCHAR(255) NOT NULL PRIMARY KEY,
    trades_count INT,
    roe_sum DECIMAL(10, 4),
    avg_roe DECIMAL(10, 4),
    roe_std_dev DECIMAL(10, 4),
    kelly_criteria DECIMAL(20, 10)
);