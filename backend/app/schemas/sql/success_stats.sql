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
);