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
);