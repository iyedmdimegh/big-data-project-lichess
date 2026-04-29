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

-- Append-only event log: same player can finish multiple games in the same
-- UTC second (PGN timestamp resolution). Surrogate id avoids PK collisions
-- under JDBC batch insert; query by (player_id, recorded_at) is still indexed.
CREATE TABLE IF NOT EXISTS elo_history (
    id          BIGSERIAL PRIMARY KEY,
    player_id   VARCHAR     NOT NULL,
    recorded_at TIMESTAMP   NOT NULL,
    elo         INT,
    delta       INT
);

CREATE INDEX IF NOT EXISTS idx_elo_history_recorded_at
    ON elo_history (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_elo_history_player_recorded
    ON elo_history (player_id, recorded_at DESC);

-- Stream output: opening popularity in 5-minute tumbling windows.
-- Keyed by (window_start, eco) so re-running the job upserts cleanly.
CREATE TABLE IF NOT EXISTS opening_trends (
    window_start TIMESTAMP   NOT NULL,
    window_end   TIMESTAMP   NOT NULL,
    eco          VARCHAR(8)  NOT NULL,
    opening_name VARCHAR,
    game_count   BIGINT      NOT NULL,
    PRIMARY KEY (window_start, eco)
);

CREATE INDEX IF NOT EXISTS idx_opening_trends_window
    ON opening_trends (window_start DESC);

-- Stream output: running per-player standings inside each tournament.
-- Stateful aggregation upserts (tournament_id, player_id) on every change.
CREATE TABLE IF NOT EXISTS tournament_leaderboard (
    tournament_id  VARCHAR     NOT NULL,
    player_id      VARCHAR     NOT NULL,
    points         FLOAT       NOT NULL,
    games_played   INT         NOT NULL,
    wins           INT         NOT NULL,
    draws          INT         NOT NULL,
    losses         INT         NOT NULL,
    last_updated   TIMESTAMP   NOT NULL,
    PRIMARY KEY (tournament_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_tournament_leaderboard_points
    ON tournament_leaderboard (tournament_id, points DESC);
