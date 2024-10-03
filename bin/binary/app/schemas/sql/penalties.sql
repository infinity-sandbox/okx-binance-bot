CREATE TABLE IF NOT EXISTS penalties (
    trader_id VARCHAR(255) NOT NULL,
    position_table VARCHAR(255) NOT NULL,
    penalty_type VARCHAR(255) NOT NULL,
    penalty_value INT NOT NULL,
    UNIQUE (trader_id, position_table)
);