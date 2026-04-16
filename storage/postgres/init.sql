CREATE TABLE IF NOT EXISTS players (
    player_id VARCHAR PRIMARY KEY,
    username VARCHAR,
    country VARCHAR,
    elo_current INT,
    account_created TIMESTAMP,
    title VARCHAR
);

CREATE TABLE IF NOT EXISTS openings (
    eco_code VARCHAR PRIMARY KEY,
    name VARCHAR,
    moves_uci TEXT,
    total_games INT,
    white_wins INT,
    black_wins INT,
    draws INT,
    avg_elo_white FLOAT,
    avg_elo_black FLOAT
);

CREATE TABLE IF NOT EXISTS als_vectors (
    player_id VARCHAR PRIMARY KEY,
    factors JSONB,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS opening_clusters (
    eco_code VARCHAR PRIMARY KEY,
    cluster_id INT,
    cluster_label VARCHAR,
    aggression_score FLOAT,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS player_styles (
    player_id VARCHAR PRIMARY KEY,
    aggression_index FLOAT,
    avg_captures_per_game FLOAT,
    avg_pawn_advances FLOAT,
    sacrifices_per_game FLOAT,
    computed_at TIMESTAMP
);
